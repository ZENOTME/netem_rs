mod grpc;
pub(crate) use grpc::*;
mod xdp;
use tokio::sync::mpsc::UnboundedSender;
pub use xdp::*;
mod port_table;
pub use port_table::*;

use smallvec::SmallVec;

use async_xdp::{Frame as XdpFrame, XdpReceiveHandle, XdpSendHandle};

use crate::proto::Packet;

const BATCH_SIZSE: usize = 32;

pub struct PortReceiveHandleImpl {
    port_id: u32,
    inner: PortReceiveHandleImplInner,
}

enum PortReceiveHandleImplInner {
    Xdp(XdpReceiveHandle),
}

impl PortReceiveHandleImpl {
    pub fn new_local(port_id: u32, handle: XdpReceiveHandle) -> Self {
        Self {
            port_id,
            inner: PortReceiveHandleImplInner::Xdp(handle),
        }
    }

    pub async fn receive_frames(&mut self) -> anyhow::Result<SmallVec<[XdpFrame; BATCH_SIZSE]>> {
        match &mut self.inner {
            PortReceiveHandleImplInner::Xdp(handle) => handle.receive().await,
        }
    }

    pub fn port_id(&self) -> u32 {
        self.port_id
    }
}

#[derive(Clone)]
pub struct PortSendHandleImpl {
    port_id: u32,
    inner: PortSendHandleImplInner,
}

#[derive(Clone)]
pub enum PortSendHandleImplInner {
    Xdp(XdpSendHandle),
    Remote(UnboundedSender<Packet>),
    RemoteXdp(RemoteXdpSendHandle),
}

impl PortSendHandleImpl {
    pub fn new_local(port_id: u32, handle: XdpSendHandle) -> Self {
        Self {
            port_id,
            inner: PortSendHandleImplInner::Xdp(handle),
        }
    }

    pub fn new_remote(port_id: u32, sender: UnboundedSender<Packet>) -> Self {
        Self {
            port_id,
            inner: PortSendHandleImplInner::Remote(sender),
        }
    }

    pub fn new_remote_xdp(port_id: u32, handle: RemoteXdpSendHandle) -> Self {
        Self {
            port_id,
            inner: PortSendHandleImplInner::RemoteXdp(handle),
        }
    }

    pub fn is_remote(&self) -> bool {
        matches!(self.inner, PortSendHandleImplInner::Remote(_))
    }

    pub fn port_id(&self) -> u32 {
        self.port_id
    }

    pub fn send_frame(&self, frame: SmallVec<[XdpFrame; BATCH_SIZSE]>) -> anyhow::Result<()> {
        match &self.inner {
            PortSendHandleImplInner::Xdp(handle) => handle.send_frame(frame),
            PortSendHandleImplInner::Remote(handle) => {
                for frame in frame {
                    handle.send(Packet {
                        payload: frame.data_ref().to_vec(),
                    })?;
                }
                Ok(())
            }
            PortSendHandleImplInner::RemoteXdp(handle) => handle.send_frame(frame),
        }
    }

    pub fn send_raw_data(&self, data: Vec<u8>) -> anyhow::Result<()> {
        match &self.inner {
            PortSendHandleImplInner::Xdp(handle) => handle.send(data),
            PortSendHandleImplInner::Remote(hanle) => {
                hanle.send(Packet { payload: data })?;
                Ok(())
            }
            PortSendHandleImplInner::RemoteXdp(handle) => handle.send_raw_data(data),
        }
    }
}
