use std::sync::Arc;

use async_xdp::{
    config::{BindFlags, LibxdpFlags, SocketConfig, UmemConfig},
    regsiter_xdp_program, FrameManager, PollerRunner, SlabManager, SlabManagerConfig, Umem,
    XdpContext, XdpContextBuilder,
};
use hwaddr::HwAddr;

mod local;
pub(crate) use local::*;
mod remote;
pub(crate) use remote::*;

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
            .frame_headroom(16)
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
    xdp_runner: Box<dyn PollerRunner>,
    umem_config: UmemConfig,
    umem: Umem,
    frame_manager: SlabManager,
}

impl XdpManager {
    pub fn new(config: XdpManagerConfig, xdp_runner: Box<dyn PollerRunner>) -> Self {
        let XdpManagerConfig {
            umem_config,
            slab_manager_config,
        } = config;

        // Create the umem and slab manager
        let (umem, frames) =
            Umem::new(umem_config, (4096 * 256).try_into().unwrap(), false).unwrap();
        let frame_manager = SlabManager::new(slab_manager_config, frames).unwrap();

        Self {
            xdp_runner,
            umem,
            umem_config,
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

    pub fn create_xdp(&self, config: XdpConfig) -> anyhow::Result<XdpContext> {
        let XdpConfig {
            if_name,
            queue_id,
            mac_addr: _,
            socket_config,
        } = config;
        let mut xdp_context_builder = XdpContextBuilder::<SlabManager>::new(&if_name, queue_id);

        let new_umem = unsafe { Umem::new_from_umem(self.umem_config, self.umem.clone())? };

        xdp_context_builder
            .with_exist_umem(new_umem, self.frame_manager.clone())
            .with_socket_config(socket_config);
        xdp_context_builder.build(&self.xdp_runner)
    }
}
