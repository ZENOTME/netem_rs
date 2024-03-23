use std::collections::HashMap;

use async_xdp::{
    config::{SocketConfig, UmemConfig},
    FrameManager, SingleThreadRunner, SlabManager, SlabManagerConfig, Umem, XdpContext,
    XdpContextBuilder,
};

pub(crate) struct XdpManager {
    xdp_runner: SingleThreadRunner,
    xdp_contexts: HashMap<u32, XdpContext>,

    umem: Umem,
    frame_manager: SlabManager,
}

impl XdpManager {
    pub fn new(umem_config: UmemConfig, slab_manager_config: SlabManagerConfig) -> Self {
        // Create the umem and slab manager
        let (umem, frames) =
            Umem::new(umem_config, (4096 * 16).try_into().unwrap(), false).unwrap();
        let frame_manager = SlabManager::new(slab_manager_config, frames).unwrap();

        Self {
            xdp_runner: SingleThreadRunner::new(),
            xdp_contexts: HashMap::new(),
            umem,
            frame_manager,
        }
    }

    fn create_new_context(
        &self,
        if_name: &str,
        queue_id: u32,
        socket: SocketConfig,
    ) -> anyhow::Result<XdpContext> {
        let mut xdp_context_builder = XdpContextBuilder::<'_, SlabManager, SingleThreadRunner>::new(
            if_name,
            queue_id,
            &self.xdp_runner,
        );
        xdp_context_builder
            .with_exist_umem(self.umem.clone(), self.frame_manager.clone())
            .with_socket_config(socket);
        xdp_context_builder.build()
    }

    pub fn add_xdp(
        &mut self,
        port_id: u32,
        if_name: &str,
        queue_id: u32,
        socket: SocketConfig,
    ) -> anyhow::Result<&XdpContext> {
        let context = self.create_new_context(if_name, queue_id, socket)?;
        self.xdp_contexts.insert(port_id, context);
        Ok(self.xdp_contexts.get(&port_id).unwrap())
    }
}
