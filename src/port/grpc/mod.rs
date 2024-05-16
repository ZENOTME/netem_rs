use crate::proto::event::Event as EventEnum;
use crate::proto::Packet as RemotePacket;
use crate::NodeInfo;
use crate::{
    proto::{
        remote_port_service_client::RemotePortServiceClient,
        remote_port_service_server::RemotePortService, Event,
    },
    HostAddr, PortSendHandleImpl, PortTable,
};
use futures::{stream::SelectAll, StreamExt};
use futures_async_stream::{stream, try_stream};
use packet::ether::Packet;
use std::sync::Arc;
use tonic::Status;

use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_stream::Stream;
use tonic::Streaming;

pub type GrpcTransportManagerRef = Arc<GrpcTransportManager>;

pub struct GrpcTransportManager {
    notify_tx: UnboundedSender<(u32, Streaming<Event>)>,
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

    pub fn register_new_port(&self, port_id: u32, stream: Streaming<Event>) {
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
            port_table.for_each_port(|_, send_handle| {
                if !send_handle.is_remote() {
                    send_handle.send_raw_data(packet.as_ref().to_vec())
                } else {
                    Ok(())
                }
            })?;
        } else {
            log::trace!(
                "process unicast packet, dst: {:?}, len: {}",
                packet.destination(),
                packet.as_ref().len()
            );
            if let Some(send_handle) = port_table.get_send_handle(packet.destination()) {
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
        mut notify_rx: tokio::sync::mpsc::UnboundedReceiver<(u32, Streaming<Event>)>,
        mut port_table: PortTable,
    ) -> anyhow::Result<()> {
        let mut all_rx_stream = SelectAll::new();
        loop {
            tokio::select! {
                Some((_port_id, rx)) = notify_rx.recv() => {
                    all_rx_stream.push(rx);
                }
                Some(packet) = all_rx_stream.next() => {
                    let packet = match packet.unwrap().event.unwrap() {
                        EventEnum::Packet(packet) => packet,
                        EventEnum::Info(_) => unreachable!("Info event is the first init message, never received here."),
                    };
                    let packet = Packet::new(packet.payload.as_slice()).unwrap();
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

#[stream(item = Event)]
async fn get_event_receive_stream(node_info: NodeInfo, send_rx: UnboundedReceiver<RemotePacket>) {
    // Send the node info first.
    yield Event {
        event: Some(EventEnum::Info(node_info.into())),
    };
    // Send the packet stream.
    let mut send_rx = send_rx;
    while let Some(packet) = send_rx.recv().await {
        yield Event {
            event: Some(EventEnum::Packet(packet)),
        }
    }
}

/// This function:
/// 1. connects to remote hosts to build the remote port
/// 2. return the remote port service to be used by connected by remote hosts in the future.
pub async fn start_remote_grpc(
    node_info: NodeInfo,
    hosts: Vec<HostAddr>,
    grpc_manager: GrpcTransportManagerRef,
    port_table: PortTable,
) -> anyhow::Result<RemotePortServiceImpl> {
    // create stream with remote host
    for host in hosts {
        let (send_tx, send_rx) = tokio::sync::mpsc::unbounded_channel();

        // Send the send stream to remote actor and get the receive stream
        let mut client = RemotePortServiceClient::connect(host.clone()).await?;
        let mut receive_stream = client
            .create_stream(tonic::Request::new(get_event_receive_stream(
                node_info.clone(),
                send_rx,
            )))
            .await?
            .into_inner();

        let info: NodeInfo = match receive_stream.message().await?.unwrap().event.unwrap() {
            EventEnum::Info(info) => info.try_into()?,
            EventEnum::Packet(_) => {
                return Err(anyhow::anyhow!(
                    "The first message should be the node info."
                ));
            }
        };
        if info.addr != host {
            return Err(anyhow::anyhow!(
                "The host addr is not matched. {:?} {:?}",
                info.addr,
                host
            ));
        }

        let port_id = port_table.fetch_new_port_id();
        port_table.add_remote_handle(info.addr, PortSendHandleImpl::new_remote(port_id, send_tx));
        grpc_manager.register_new_port(port_id, receive_stream);
    }
    Ok(RemotePortServiceImpl {
        grpc_manager,
        port_table,
        node_info,
    })
}

#[derive(Clone)]
pub struct RemotePortServiceImpl {
    node_info: NodeInfo,
    grpc_manager: GrpcTransportManagerRef,
    port_table: PortTable,
}

impl RemotePortServiceImpl {
    #[try_stream(ok = Event, error = Status)]
    async fn get_event_receive_stream(
        node_info: NodeInfo,
        send_rx: UnboundedReceiver<RemotePacket>,
    ) {
        // Send the node info first.
        yield Event {
            event: Some(EventEnum::Info(node_info.into())),
        };
        // Send the packet stream.
        let mut send_rx = send_rx;
        while let Some(packet) = send_rx.recv().await {
            yield Event {
                event: Some(EventEnum::Packet(packet)),
            }
        }
    }
}

#[async_trait::async_trait]
impl RemotePortService for RemotePortServiceImpl {
    type CreateStreamStream = impl Stream<Item = Result<Event, Status>>;

    async fn create_stream(
        &self,
        request: tonic::Request<tonic::Streaming<Event>>,
    ) -> std::result::Result<tonic::Response<Self::CreateStreamStream>, tonic::Status> {
        let (send_tx, send_rx) = tokio::sync::mpsc::unbounded_channel();

        let mut receive_stream = request.into_inner();
        let info: NodeInfo = match receive_stream.message().await?.unwrap().event.unwrap() {
            EventEnum::Info(info) => info.try_into().map_err(|err| {
                tonic::Status::invalid_argument(format!("Fail convert node info: {err}"))
            })?,
            EventEnum::Packet(_) => {
                return Err(tonic::Status::invalid_argument(
                    "The first message should be the node info.",
                ))
            }
        };

        let port_id = self.port_table.fetch_new_port_id();
        self.port_table
            .add_remote_handle(info.addr, PortSendHandleImpl::new_remote(port_id, send_tx));
        self.grpc_manager.register_new_port(port_id, receive_stream);

        Ok(tonic::Response::new(Self::get_event_receive_stream(
            self.node_info.clone(),
            send_rx,
        )))
    }
}
