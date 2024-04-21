use std::{collections::HashMap, future::Future, sync::Arc};

use anyhow::anyhow;
use hwaddr::HwAddr;
use tokio::sync::Mutex;

use crate::{
    proto::{
        actor_service_client::ActorServiceClient, actor_service_server::ActorService, ActorInfo,
        CreateActorRequest, CreateActorResponse, Status, SubscribeRequest, SubscribeResponse,
        UpdateActorRequest, UpdateActorResponse,
    },
    HostAddr, LocalXdpManager, NodeInfo, PortReceiveHandleImpl, PortTable, XdpConfig,
    XdpManagerRef,
};

pub trait DataView: Clone + Send + 'static + Sync {
    fn new() -> Self;
}

#[allow(async_fn_in_trait)]
pub trait Actor: Send + 'static {
    type C: DataView;
    fn new(context: ActorContext<Self::C>) -> Self;
    fn run(&mut self) -> impl Future<Output = Result<(), anyhow::Error>> + Send;
}

pub struct ActorContext<D: DataView> {
    pub port_table: PortTable,
    pub receive_handle: PortReceiveHandleImpl,
    pub actor_id: u32,
    pub data_view: D,
}

#[derive(Debug)]
pub(crate) struct ActorConfig {
    pub local_port_config: XdpConfig,
}

pub(crate) struct ActorManager<D: DataView> {
    join_handles: Vec<tokio::task::JoinHandle<u32>>,

    local_port_manager: LocalXdpManager,

    port_table: PortTable,

    data_view: D,

    actor_id: u32,

    actor_infos: HashMap<u32, ActorInfo>,
}

impl<C: DataView> ActorManager<C> {
    pub fn new(xdp_manager: XdpManagerRef, port_table: PortTable) -> Self {
        let local_port_manager = LocalXdpManager::new(xdp_manager, port_table.clone());
        Self {
            join_handles: Vec::new(),
            local_port_manager,
            data_view: C::new(),
            port_table,
            actor_id: 0,
            actor_infos: HashMap::new(),
        }
    }

    async fn create_actor_context(
        &mut self,
        config: ActorConfig,
    ) -> anyhow::Result<ActorContext<C>> {
        let actor_id = self.actor_id;
        self.actor_id += 1;

        let receive_handle = self
            .local_port_manager
            .create_xdp(config.local_port_config)
            .await?;

        Ok(ActorContext {
            data_view: self.data_view.clone(),
            port_table: self.port_table.clone(),
            receive_handle,
            actor_id,
        })
    }

    pub async fn actor_infos(&self) -> Vec<ActorInfo> {
        self.actor_infos.values().cloned().collect()
    }

    pub async fn add_remote_actor(
        &mut self,
        remote_addr: HostAddr,
        port_mac: HwAddr,
    ) -> anyhow::Result<()> {
        self.port_table.add_remote_port(remote_addr, port_mac).await
    }

    pub async fn add_actor<A: Actor>(&mut self, config: ActorConfig) -> anyhow::Result<()>
    where
        A: Actor<C = C>,
    {
        let mac = config.local_port_config.mac_addr;
        let actor_context = self.create_actor_context(config).await?;
        let actor_id = actor_context.actor_id;
        let mut actor = A::new(actor_context);

        let join = tokio::spawn(async move {
            match actor.run().await {
                Ok(_) => (),
                Err(err) => {
                    log::error!("Actor {} failed: {:?}", actor_id, err);
                }
            }
            actor_id
        });

        // Update actor_infos
        self.actor_infos.insert(
            actor_id,
            ActorInfo {
                mac_addr: mac.octets().to_vec(),
            },
        );

        self.join_handles.push(join);

        Ok(())
    }

    pub async fn join_all(&mut self) {
        for join in self.join_handles.drain(..) {
            let _actor_id = join.await.unwrap();
            // # TODO: Remove actor from actor_infos
        }
    }
}

/// This function:
/// 1. Resgiter to other remote hosts to get the remote actor infos and update the local transport table.
/// 2. Return the actor service.
pub(crate) async fn start_actor<A: Actor>(
    mut actor_manager: ActorManager<A::C>,
    node_info: NodeInfo,
    remote_addrs: Vec<HostAddr>,
) -> anyhow::Result<ActorServiceImpl<A>> {
    let mut remote_node_client = Vec::with_capacity(remote_addrs.len());

    // Register to remote hosts to get the remote actor infos.
    for remote_addr in remote_addrs {
        let mut client = ActorServiceClient::connect(remote_addr.clone()).await?;
        let response = client
            .subscribe(SubscribeRequest {
                info: Some(node_info.clone().into()),
            })
            .await?
            .into_inner();
        if response.status.unwrap().code != 0 {
            return Err(anyhow!("Failed to subscribe to remote host"));
        }
        for info in response.infos {
            let mac = HwAddr::from(&info.mac_addr[..6]);
            actor_manager
                .add_remote_actor(remote_addr.clone(), mac)
                .await?;
        }
        remote_node_client.push(remote_addr);
    }

    Ok(ActorServiceImpl {
        inner: Arc::new(Mutex::new(ActorServiceImplInner::<A> {
            node_info,
            actor_manager,
            remote_node_client,
        })),
    })
}

/// ActorService is control path and not in the data path. To avoid the corret and consistency issue, we use
/// a lock to protect the whole inner state and make it execute serially.
pub struct ActorServiceImpl<A: Actor> {
    inner: Arc<Mutex<ActorServiceImplInner<A>>>,
}

#[async_trait::async_trait]
impl<A: Actor> ActorService for ActorServiceImpl<A> {
    async fn subscribe(
        &self,
        request: tonic::Request<SubscribeRequest>,
    ) -> std::result::Result<tonic::Response<SubscribeResponse>, tonic::Status> {
        self.inner.lock().await.subscribe(request).await
    }

    async fn update_actor(
        &self,
        request: tonic::Request<UpdateActorRequest>,
    ) -> std::result::Result<tonic::Response<UpdateActorResponse>, tonic::Status> {
        self.inner.lock().await.update_actor(request).await
    }

    async fn create_actor(
        &self,
        request: tonic::Request<CreateActorRequest>,
    ) -> std::result::Result<tonic::Response<CreateActorResponse>, tonic::Status> {
        self.inner.lock().await.create_actor(request).await
    }
}

struct ActorServiceImplInner<A: Actor> {
    actor_manager: ActorManager<A::C>,
    remote_node_client: Vec<HostAddr>,
    node_info: NodeInfo,
}

impl<A: Actor> ActorServiceImplInner<A> {
    async fn subscribe(
        &mut self,
        request: tonic::Request<SubscribeRequest>,
    ) -> std::result::Result<tonic::Response<SubscribeResponse>, tonic::Status> {
        let remote_addr = request
            .into_inner()
            .info
            .unwrap()
            .host_addr
            .as_str()
            .parse::<HostAddr>()
            .map_err(|_| tonic::Status::invalid_argument("Invalid addr"))?;
        self.remote_node_client.push(remote_addr);

        Ok(tonic::Response::new(SubscribeResponse {
            infos: self.actor_manager.actor_infos().await,
            status: Some(Status {
                code: 0,
                message: String::new(),
            }),
        }))
    }

    async fn update_actor(
        &mut self,
        request: tonic::Request<UpdateActorRequest>,
    ) -> std::result::Result<tonic::Response<UpdateActorResponse>, tonic::Status> {
        let UpdateActorRequest { info, mac_addr } = request.into_inner();
        let info: NodeInfo = info
            .unwrap()
            .try_into()
            .map_err(|_| tonic::Status::invalid_argument("Fail to convert node info"))?;
        let mac = HwAddr::from(&mac_addr[..6]);
        self.actor_manager
            .add_remote_actor(info.addr, mac)
            .await
            .map_err(|_| tonic::Status::not_found("Build the transport before update actor"))?;
        Ok(tonic::Response::new(UpdateActorResponse {
            status: Some(Status {
                code: 0,
                message: String::new(),
            }),
        }))
    }

    async fn create_actor(
        &mut self,
        request: tonic::Request<CreateActorRequest>,
    ) -> std::result::Result<tonic::Response<CreateActorResponse>, tonic::Status> {
        let CreateActorRequest {
            if_name,
            queue_id,
            port_type: _,
            mac_addr,
        } = request.into_inner();

        if mac_addr.len() != 6 {
            return Err(tonic::Status::invalid_argument(
                "mac address should be 6 bytes length",
            ));
        }
        let local_port_config = XdpConfig::new_with_default_socket_config(
            if_name,
            queue_id,
            HwAddr::from(&mac_addr[..6]),
        );
        let actor_config = ActorConfig { local_port_config };

        self.actor_manager
            .add_actor::<A>(actor_config)
            .await
            .unwrap();

        // Broadcast the new port to remote node.
        for remote_node_client in self.remote_node_client.iter_mut() {
            let mut remote_node_client = ActorServiceClient::connect(remote_node_client.clone())
                .await
                .unwrap();
            remote_node_client
                .update_actor(UpdateActorRequest {
                    info: Some(self.node_info.clone().into()),
                    mac_addr: mac_addr.clone(),
                })
                .await?;
        }

        Ok(tonic::Response::new(CreateActorResponse {
            status: Some(Status {
                code: 0,
                message: String::new(),
            }),
        }))
    }
}
