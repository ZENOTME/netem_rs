use std::collections::HashMap;

use async_xdp::{
    config::{SocketConfig, UmemConfig},
    FrameManager, SingleThreadRunner, SlabManager, SlabManagerConfig, Umem, XdpContext,
    XdpContextBuilder,
};
use hwaddr::HwAddr;
use smallvec::SmallVec;

use crate::FrameImpl;

use super::{Frame, PortReceiveHandle, PortSendHandle, BATCH_SIZSE};

impl Frame for async_xdp::Frame {
    fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl PortReceiveHandle for async_xdp::XdpReceiveHandle {
    async fn receive_frames(&mut self) -> anyhow::Result<SmallVec<[FrameImpl; BATCH_SIZSE]>> {
        let frames = self.receive().await?;
        Ok(frames
            .into_iter()
            .map(FrameImpl::Xdp)
            .collect())
    }
}

impl PortSendHandle for async_xdp::XdpSendHandle {
    fn send_frame(&self, frame: FrameImpl) -> anyhow::Result<()> {
        match frame {
            FrameImpl::Xdp(frame) => self.send_frame(vec![frame].into()),
        }
    }

    fn send_raw_data(&self, data: Vec<u8>) -> anyhow::Result<()> {
        self.send(data)
    }
}

#[derive(Debug)]
pub(crate) struct XdpConfig {
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

pub(crate) struct XdpManager {
    xdp_runner: SingleThreadRunner,
    xdp_contexts: HashMap<u32, XdpContext>,

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
        let mut xdp_context_builder = XdpContextBuilder::<SlabManager>::new(if_name, queue_id);
        xdp_context_builder
            .with_exist_umem(self.umem.clone(), self.frame_manager.clone())
            .with_socket_config(socket);
        xdp_context_builder.build(&self.xdp_runner)
    }

    pub fn create_xdp(&mut self, port_id: u32, config: XdpConfig) -> anyhow::Result<&XdpContext> {
        let XdpConfig {
            if_name,
            queue_id,
            mac_addr: _,
            socket_config,
        } = config;
        let context = self.create_new_context(&if_name, queue_id, socket_config)?;
        self.xdp_contexts.insert(port_id, context);
        Ok(self.xdp_contexts.get(&port_id).unwrap())
    }
}
