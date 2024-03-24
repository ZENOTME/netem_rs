use std::future::Future;

use crate::{PortConfig, PortManager, PortReceiveHandleImpl, RouteTable, SendHandleMapRef};

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
    pub send_handles: SendHandleMapRef,
    pub receive_handle: PortReceiveHandleImpl,
    pub actor_id: u32,
    pub data_view: D,
    pub route_table: RouteTable,
}

#[derive(Debug)]
pub(crate) struct ActorConfig {
    pub actor_id: u32,
    pub port_config: PortConfig,
}

pub(crate) struct ActorManager<D: DataView> {
    join_handles: Vec<tokio::task::JoinHandle<()>>,

    port_manager: PortManager,
    data_view: D,
    route_table: RouteTable,
}

impl<C: DataView> ActorManager<C> {
    pub fn new(port_manager: PortManager, route_table: RouteTable) -> Self {
        Self {
            join_handles: Vec::new(),
            port_manager,
            data_view: C::new(),
            route_table,
        }
    }

    async fn create_actor_context(
        &mut self,
        config: ActorConfig,
    ) -> anyhow::Result<ActorContext<C>> {
        let receive_handle = self.port_manager.create_port(config.port_config).await?;

        Ok(ActorContext {
            data_view: self.data_view.clone(),
            route_table: self.route_table.clone(),
            send_handles: self.port_manager.send_handle_map(),
            receive_handle,
            actor_id: config.actor_id,
        })
    }

    pub async fn add_actor<A: Actor>(&mut self, config: ActorConfig) -> anyhow::Result<()>
    where
        A: Actor<C = C>,
    {
        let actor_context = self.create_actor_context(config).await?;
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

    pub async fn join_all(&mut self) {
        for join in self.join_handles.drain(..) {
            join.await.unwrap();
        }
    }
}
