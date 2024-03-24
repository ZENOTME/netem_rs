mod xdp;
use tokio::sync::RwLock;
pub(crate) use xdp::*;

use smallvec::SmallVec;
use std::{collections::HashMap, future::Future, sync::Arc};

use crate::RouteTable;
use async_xdp::{Frame as XdpFrame, XdpReceiveHandle, XdpSendHandle};

#[derive(Debug)]
pub(crate) struct PortConfig {
    port_id: u32,
    kind_config: PortKindConfig,
}

impl PortConfig {
    pub fn xdp_config(port_id: u32, xdp_config: XdpConfig) -> Self {
        Self {
            port_id,
            kind_config: PortKindConfig::Xdp(xdp_config),
        }
    }
}

#[derive(Debug)]
pub(crate) enum PortKindConfig {
    Xdp(XdpConfig),
}

pub type SendHandleMapRef = Arc<RwLock<HashMap<u32, PortSendHandleImpl>>>;

pub(crate) struct PortManager {
    route_table: RouteTable,
    xdp_manager: XdpManager,
    send_handle_map: SendHandleMapRef,
}

impl PortManager {
    pub fn new(route_table: RouteTable, xdp_manager_config: XdpManagerConfig) -> Self {
        Self {
            route_table,
            xdp_manager: XdpManager::new(xdp_manager_config),
            send_handle_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn create_xdp_port(
        &mut self,
        port_id: u32,
        xdp_config: XdpConfig,
    ) -> anyhow::Result<(PortReceiveHandleImpl, PortSendHandleImpl)> {
        let mac_addr = xdp_config.mac_addr;
        let context = self.xdp_manager.create_xdp(port_id, xdp_config)?;
        let receive_handle = context.receive_handle()?;
        let send_handle = context.send_handle();

        self.route_table.add_route(mac_addr, port_id).await;

        Ok((
            PortReceiveHandleImpl {
                inner: PortReceiveHandleImplInner::Xdp(receive_handle),
                port_id,
            },
            PortSendHandleImpl {
                inner: PortSendHandleImplInner::Xdp(send_handle),
                port_id,
            },
        ))
    }

    pub async fn create_port(
        &mut self,
        config: PortConfig,
    ) -> anyhow::Result<PortReceiveHandleImpl> {
        let (receive_handle, send_handle) = match config.kind_config {
            PortKindConfig::Xdp(xdp_config) => {
                self.create_xdp_port(config.port_id, xdp_config).await?
            }
        };
        self.send_handle_map
            .write()
            .await
            .insert(config.port_id, send_handle);
        Ok(receive_handle)
    }

    pub fn send_handle_map(&self) -> SendHandleMapRef {
        self.send_handle_map.clone()
    }
}

pub enum FrameImpl {
    Xdp(XdpFrame),
}

trait Frame {
    fn as_slice(&self) -> &[u8];
    fn as_slice_mut(&mut self) -> &mut [u8];
}

impl FrameImpl {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Xdp(frame) => frame.as_slice(),
        }
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Xdp(frame) => frame.as_slice_mut(),
        }
    }
}

const BATCH_SIZSE: usize = 32;

#[allow(async_fn_in_trait)]
trait PortReceiveHandle {
    fn receive_frames(
        &mut self,
    ) -> impl Future<Output = anyhow::Result<SmallVec<[FrameImpl; BATCH_SIZSE]>>> + Send;
}

pub struct PortReceiveHandleImpl {
    port_id: u32,
    inner: PortReceiveHandleImplInner,
}

enum PortReceiveHandleImplInner {
    Xdp(XdpReceiveHandle),
}

impl PortReceiveHandleImpl {
    pub async fn receive_frames(&mut self) -> anyhow::Result<SmallVec<[FrameImpl; BATCH_SIZSE]>> {
        match &mut self.inner {
            PortReceiveHandleImplInner::Xdp(handle) => handle.receive_frames().await,
        }
    }

    pub fn port_id(&self) -> u32 {
        self.port_id
    }
}

#[allow(async_fn_in_trait)]
trait PortSendHandle {
    fn send_frame(&self, frame: FrameImpl) -> anyhow::Result<()>;
    fn send_raw_data(&self, data: Vec<u8>) -> anyhow::Result<()>;
}

pub struct PortSendHandleImpl {
    port_id: u32,
    inner: PortSendHandleImplInner,
}

#[derive(Clone)]
pub enum PortSendHandleImplInner {
    Xdp(XdpSendHandle),
}

impl PortSendHandleImpl {
    pub fn port_id(&self) -> u32 {
        self.port_id
    }

    pub fn send_frame(&self, frame: FrameImpl) -> anyhow::Result<()> {
        match &self.inner {
            PortSendHandleImplInner::Xdp(handle) => PortSendHandle::send_frame(handle, frame),
        }
    }

    pub fn send_raw_data(&self, data: Vec<u8>) -> anyhow::Result<()> {
        match &self.inner {
            PortSendHandleImplInner::Xdp(handle) => PortSendHandle::send_raw_data(handle, data),
        }
    }
}
