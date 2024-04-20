use std::sync::Arc;

use async_xdp::{
    config::{LibxdpFlags, SocketConfig, UmemConfig},
    regsiter_xdp_program, FrameManager, SingleThreadRunner, SlabManager, SlabManagerConfig, Umem,
    XdpContext, XdpContextBuilder,
};
use hwaddr::HwAddr;

mod local;
pub(crate) use local::*;

#[derive(Clone, Debug)]
pub struct XdpConfig {
    pub if_name: String,
    pub queue_id: u32,
    pub mac_addr: HwAddr,
    pub socket_config: SocketConfig,
}

impl XdpConfig {
    pub fn new_with_default_socket_config(
        if_name: String,
        queue_id: u32,
        mac_addr: HwAddr,
    ) -> Self {
        let socket_config = SocketConfig::builder()
            .rx_queue_size((4096).try_into().unwrap())
            .tx_queue_size((4096).try_into().unwrap())
            .build();
        Self {
            if_name,
            queue_id,
            mac_addr,
            socket_config,
        }
    }

    pub fn new_remote_with_default_socket_config(
        if_name: String,
        queue_id: u32,
        mac_addr: HwAddr,
    ) -> Self {
        let socket_config = SocketConfig::builder()
            .rx_queue_size((4096).try_into().unwrap())
            .tx_queue_size((4096).try_into().unwrap())
            .libbpf_flags(LibxdpFlags::XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD)
            .build();
        Self {
            if_name,
            queue_id,
            mac_addr,
            socket_config,
        }
    }
}

pub(crate) struct XdpManagerConfig {
    pub umem_config: UmemConfig,
    pub slab_manager_config: SlabManagerConfig,
}

impl Default for XdpManagerConfig {
    fn default() -> Self {
        let umem_config = UmemConfig::builder()
            .fill_queue_size((4096).try_into().unwrap())
            .comp_queue_size((4096).try_into().unwrap())
            .build()
            .unwrap();
        let slab_manager_config = SlabManagerConfig::new(4096);
        Self {
            umem_config,
            slab_manager_config,
        }
    }
}

pub(crate) type XdpManagerRef = Arc<XdpManager>;

pub(crate) struct XdpManager {
    xdp_runner: SingleThreadRunner,
    umem: Umem,
    frame_manager: SlabManager,
}

impl XdpManager {
    pub fn new(config: XdpManagerConfig) -> Self {
        let XdpManagerConfig {
            umem_config,
            slab_manager_config,
        } = config;

        // Create the umem and slab manager
        let (umem, frames) =
            Umem::new(umem_config, (4096 * 16).try_into().unwrap(), false).unwrap();
        let frame_manager = SlabManager::new(slab_manager_config, frames).unwrap();

        Self {
            xdp_runner: SingleThreadRunner::new(),
            umem,
            frame_manager,
        }
    }

    /// Register the xdp program if the remote xdp mode is enabled. Otherwise the remote xdp port will create
    /// fail.
    #[allow(dead_code)]
    pub fn register_xdp_program(
        &self,
        program: &str,
        session: &str,
        if_name: &str,
    ) -> anyhow::Result<()> {
        regsiter_xdp_program(program, session, if_name).map_err(|e| anyhow::anyhow!(e))
    }

    fn create_new_context(
        &self,
        if_name: &str,
        queue_id: u32,
        socket: SocketConfig,
    ) -> anyhow::Result<XdpContext> {
        let mut xdp_context_builder = XdpContextBuilder::<SlabManager>::new(if_name, queue_id);
        xdp_context_builder
            .with_exist_umem(self.umem.clone(), self.frame_manager.clone())
            .with_socket_config(socket)
            .with_trace_mode(true);
        xdp_context_builder.build(&self.xdp_runner)
    }

    pub fn create_xdp(&self, config: XdpConfig) -> anyhow::Result<XdpContext> {
        let XdpConfig {
            if_name,
            queue_id,
            mac_addr: _,
            socket_config,
        } = config;
        self.create_new_context(&if_name, queue_id, socket_config)
    }
}
