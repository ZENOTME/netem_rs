use std::{collections::HashMap, future::Future, sync::Arc};

use async_xdp::{
    config::{SocketConfig, UmemConfig},
    umem::frame::{Data, DataMut},
    FrameDesc, SlabManagerConfig, Umem, XdpReceiveHandle, XdpSendHandle,
};
use hwaddr::HwAddr;
use mac_address::mac_address_by_name;
use tokio::sync::RwLock;

use crate::{xdp::XdpManager, RouteTable};

pub trait DataView: Clone + Send + 'static {
    fn new() -> Self;
}

#[allow(async_fn_in_trait)]
pub trait Actor: Send + 'static {
    type C: DataView;
    fn new(context: ActorContext<Self::C>) -> Self;
    fn run(&mut self) -> impl Future<Output = Result<(), anyhow::Error>> + Send;
}

pub struct ActorContext<D: DataView> {
    pub send_handles: Arc<RwLock<HashMap<u32, XdpSendHandle>>>,
    pub receive_handle: XdpReceiveHandle,
    pub actor_id: u32,
    pub data_view: D,
    pub route_table: RouteTable,
    umem: Umem,
    // ActorId
}

impl<D: DataView> ActorContext<D> {
    pub fn packet_data(&self, frame: &FrameDesc) -> Data<'_> {
        unsafe { self.umem.data(frame) }
    }

    pub fn packet_data_mut<'a>(&'a self, frame: &'a mut FrameDesc) -> DataMut<'a> {
        unsafe { self.umem.data_mut(frame) }
    }
}

#[derive(Debug)]
pub struct ActorConfig {
    pub actor_id: u32,
    // Port config
    pub port_id: u32,
    pub veth_name: String,
    pub queue_id: u32,
}

pub struct ActorManager<D: DataView> {
    join_handles: Vec<tokio::task::JoinHandle<()>>,

    socket_config: SocketConfig,

    // Specific for XDP
    xdp_manager: XdpManager,
    send_handles: Arc<RwLock<HashMap<u32, XdpSendHandle>>>,

    data_view: D,

    route_table: RouteTable,
}

impl<C: DataView> Default for ActorManager<C> {
    fn default() -> Self {
        let umem_config = UmemConfig::builder()
            .fill_queue_size((4096).try_into().unwrap())
            .comp_queue_size((4096).try_into().unwrap())
            .build()
            .unwrap();
        let socket_config = SocketConfig::builder()
            .rx_queue_size((4096).try_into().unwrap())
            .tx_queue_size((4096).try_into().unwrap())
            .build();
        let slab_manager_config = SlabManagerConfig::new(4096);
        Self::new(umem_config, slab_manager_config, socket_config)
    }
}

impl<C: DataView> ActorManager<C> {
    fn new(
        umem_config: UmemConfig,
        slab_manager_config: SlabManagerConfig,
        socket_config: SocketConfig,
    ) -> Self {
        let xdp_manager = XdpManager::new(umem_config, slab_manager_config);
        let send_handles = Arc::new(RwLock::new(HashMap::new()));
        Self {
            join_handles: Vec::new(),
            socket_config,
            xdp_manager,
            send_handles,
            data_view: C::new(),
            route_table: RouteTable::new(),
        }
    }

    async fn create_actor_context(
        &mut self,
        config: ActorConfig,
    ) -> anyhow::Result<ActorContext<C>> {
        let ActorConfig {
            actor_id,
            port_id,
            veth_name,
            queue_id,
        } = config;

        let mac_addr = if let Some(mac_addr) = mac_address_by_name(&veth_name)? {
            HwAddr::from(mac_addr.bytes())
        } else {
            return Err(anyhow::anyhow!(
                "Failed to get mac address for {}",
                veth_name
            ));
        };

        let context =
            self.xdp_manager
                .add_xdp(port_id, &veth_name, queue_id, self.socket_config)?;

        // Guranatee the port id is unique globally
        assert!(self
            .send_handles
            .write()
            .await
            .insert(port_id, context.send_handle())
            .is_none());

        self.route_table.add_route(mac_addr, port_id).await;

        Ok(ActorContext {
            actor_id,
            send_handles: self.send_handles.clone(),
            receive_handle: context.receive_handle().unwrap(),
            data_view: self.data_view.clone(),
            route_table: self.route_table.clone(),
            umem: context.umem_ref().clone(),
        })
    }

    pub async fn add_actor<A: Actor>(&mut self, config: ActorConfig) -> anyhow::Result<()>
    where
        A: Actor<C = C>,
    {
        let actor_context = self.create_actor_context(config).await?;
        // Init the actor
        let mut actor = A::new(actor_context);

        let join = tokio::spawn(async move {
            match actor.run().await {
                Ok(_) => todo!(),
                Err(_) => todo!(),
            }
        });

        self.join_handles.push(join);

        Ok(())
    }
}
