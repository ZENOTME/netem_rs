use futures::{stream::SelectAll, StreamExt};
use packet::ether::Packet;
use tonic::Status;

use crate::{
    proto::{
        remote_port_service_client::RemotePortServiceClient,
        remote_port_service_server::RemotePortService, Packet as RemotePacket,
    },
    HostAddr, PortSendHandleImpl, PortTable,
};
use futures_async_stream::try_stream;
use std::sync::Arc;

use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_stream::Stream;
use tonic::Streaming;

pub type GrpcTransportManagerRef = Arc<GrpcTransportManager>;

pub struct GrpcTransportManager {
    notify_tx: UnboundedSender<(u32, Streaming<RemotePacket>)>,
    _background_task: JoinHandle<()>,
}

impl GrpcTransportManager {
    pub fn new(port_table: PortTable) -> Self {
        let (notify_tx, notify_rx) = tokio::sync::mpsc::unbounded_channel();
        let background_task = tokio::spawn(async move {
            let _ = RemoteActor::run(notify_rx, port_table.clone()).await;
        });
        Self {
            notify_tx,
            _background_task: background_task,
        }
    }

    pub fn register_new_port(&self, port_id: u32, stream: Streaming<RemotePacket>) {
        self.notify_tx.send((port_id, stream)).unwrap();
    }
}

struct RemoteActor {}

impl RemoteActor {
    async fn process_packet(
        packet: &Packet<&[u8]>,
        port_table: &mut PortTable,
    ) -> anyhow::Result<()> {
        if packet.destination().is_broadcast() {
            log::trace!("process broadcast packet");
            port_table
                .for_each_port(|_, send_handle| {
                    if !send_handle.is_remote() {
                        send_handle.send_raw_data(packet.as_ref().to_vec())
                    } else {
                        Ok(())
                    }
                })
                .await?;
        } else {
            log::trace!(
                "process unicast packet, dst: {:?}, len: {}",
                packet.destination(),
                packet.as_ref().len()
            );
            if let Some(send_handle) = port_table.get_send_handle(packet.destination()).await {
                assert!(!send_handle.is_remote());
                send_handle.send_raw_data(packet.as_ref().to_vec())?;
            } else {
                log::warn!(
                    "Failed to find port for destination: {:?}",
                    packet.destination()
                );
            }
        }
        Ok(())
    }

    pub async fn run(
        mut notify_rx: tokio::sync::mpsc::UnboundedReceiver<(u32, Streaming<RemotePacket>)>,
        mut port_table: PortTable,
    ) -> anyhow::Result<()> {
        let mut all_rx_stream = SelectAll::new();
        loop {
            tokio::select! {
                Some((_port_id, rx)) = notify_rx.recv() => {
                    all_rx_stream.push(rx);
                }
                Some(packet) = all_rx_stream.next() => {
                    let packet_with_id = packet.unwrap();
                    let packet = Packet::new(packet_with_id.payload.as_slice()).unwrap();
                    if let Err(e) = Self::process_packet(&packet,&mut port_table).await {
                        log::error!("Failed to process packet: {:?}", e);
                    }
                }
                else => {
                    break;
                }
            }
        }
        Ok(())
    }
}

/// This function:
/// 1. connects to remote hosts to build the remote port
/// 2. return the remote port service to be used by connected by remote hosts in the future.
pub async fn start_remote_grpc(
    hosts: Vec<HostAddr>,
    grpc_manager: GrpcTransportManagerRef,
    port_table: PortTable,
) -> anyhow::Result<RemotePortServiceImpl> {
    // create stream with remote host
    for host in hosts {
        let (send_tx, send_rx) = tokio::sync::mpsc::unbounded_channel();

        // Send the send stream to remote actor and get the receive stream
        let mut client = RemotePortServiceClient::connect(host.clone()).await?;
        let receive_stream = client
            .create_stream(tonic::Request::new(
                tokio_stream::wrappers::UnboundedReceiverStream::new(send_rx),
            ))
            .await?
            .into_inner();

        let port_id = port_table.fetch_new_port_id();
        port_table
            .add_remote_handle(host.host, PortSendHandleImpl::new_remote(port_id, send_tx))
            .await;
        grpc_manager.register_new_port(port_id, receive_stream);
    }
    Ok(RemotePortServiceImpl {
        grpc_manager,
        port_table,
    })
}

#[derive(Clone)]
pub struct RemotePortServiceImpl {
    grpc_manager: GrpcTransportManagerRef,
    port_table: PortTable,
}

impl RemotePortServiceImpl {
    #[try_stream(ok = RemotePacket, error = Status)]
    async fn get_stream_impl(send_rx: UnboundedReceiver<RemotePacket>) {
        let mut send_rx = send_rx;
        while let Some(event) = send_rx.recv().await {
            yield event;
        }
    }
}

#[async_trait::async_trait]
impl RemotePortService for RemotePortServiceImpl {
    type CreateStreamStream = impl Stream<Item = Result<RemotePacket, Status>>;

    async fn create_stream(
        &self,
        request: tonic::Request<tonic::Streaming<RemotePacket>>,
    ) -> std::result::Result<tonic::Response<Self::CreateStreamStream>, tonic::Status> {
        let (send_tx, send_rx) = tokio::sync::mpsc::unbounded_channel();
        let port_id = self.port_table.fetch_new_port_id();
        let remote_addr = match request.remote_addr().unwrap() {
            std::net::SocketAddr::V4(addr) => addr.ip().to_string(),
            std::net::SocketAddr::V6(_) => unimplemented!(),
        };
        self.port_table
            .add_remote_handle(
                remote_addr,
                PortSendHandleImpl::new_remote(port_id, send_tx),
            )
            .await;
        self.grpc_manager
            .register_new_port(port_id, request.into_inner());

        Ok(tonic::Response::new(Self::get_stream_impl(send_rx)))
    }
}
