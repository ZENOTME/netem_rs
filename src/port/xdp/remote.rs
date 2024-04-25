use crate::proto::remote_xdp_port_service_client::RemoteXdpPortServiceClient;
use crate::HostAddr;
use crate::{
    port::BATCH_SIZSE, proto::remote_xdp_port_service_server::RemoteXdpPortService, NodeInfo,
    PortSendHandleImpl, PortTable, XdpConfig, XdpManagerRef,
};
use async_xdp::{Frame, XdpReceiveHandle};
use hwaddr::HwAddr;
use packet::ether::{Packet, Protocol};
use packet::PacketMut;
use smallvec::{smallvec, SmallVec};

const XDP_PROTOCOL: u16 = 5401;

pub struct RemoteXdpManager {
    port_table: PortTable,
    src_mac: HwAddr,
    xdp_context: async_xdp::XdpContext,
    _background_task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl RemoteXdpManager {
    pub async fn new(
        xdp_manager: XdpManagerRef,
        port_table: PortTable,
        xdp_program: &str,
        xdp_prog_session: &str,
        if_name: &str,
        src_mac: HwAddr,
    ) -> anyhow::Result<Self> {
        xdp_manager.register_xdp_program(xdp_program, xdp_prog_session, if_name)?;

        // Create the remote XDP socket.
        let xdp_context = xdp_manager.create_xdp(
            XdpConfig::new_remote_with_default_socket_config(if_name.to_string(), 0, src_mac),
        )?;

        // Create background task to receive the remote XDP packet and send to the local port.
        let receive_handle = xdp_context.receive_handle()?;
        let _background_task =
            tokio::spawn(RemoteXdpActor::run(receive_handle, port_table.clone()));

        Ok(Self {
            src_mac,
            xdp_context,
            port_table,
            _background_task,
        })
    }

    pub async fn create_remote_handle(
        &self,
        remote_addr: HostAddr,
        remote_mac_addr: HwAddr,
    ) -> anyhow::Result<()> {
        let remote_send_handle = RemoteXdpSendHandle {
            src_mac: self.src_mac,
            dst_mac: remote_mac_addr,
            xdp_send_handle: self.xdp_context.send_handle(),
        };

        let port_id = self.port_table.fetch_new_port_id();
        self.port_table
            .add_remote_handle(
                remote_addr,
                PortSendHandleImpl::new_remote_xdp(port_id, remote_send_handle),
            )
            .await;

        Ok(())
    }
}

/// start remote xdp
/// 1. connect to other remote node to create remote xdp handle
/// 2. return the remote xdp port service
pub async fn start_remote_xdp(
    remote_hosts: Vec<NodeInfo>,
    xdp_manager: RemoteXdpManager,
    node_info: NodeInfo,
) -> anyhow::Result<RemoteXdpPortServiceImpl> {
    let node_info: crate::proto::NodeInfo = node_info.into();
    for remote_node in remote_hosts {
        let mut client = RemoteXdpPortServiceClient::connect(remote_node.addr.clone()).await?;
        client
            .create_xdp_port(tonic::Request::new(node_info.clone()))
            .await?;

        xdp_manager
            .create_remote_handle(remote_node.addr, remote_node.eth_mac_addr.unwrap())
            .await?;
    }
    Ok(RemoteXdpPortServiceImpl {
        remote_xdp_manager: xdp_manager,
        xdp_subnet_id: node_info.xdp_subnet_id,
    })
}

struct RemoteXdpActor {}

impl RemoteXdpActor {
    async fn run(
        mut receive_handle: XdpReceiveHandle,
        mut port_table: PortTable,
    ) -> anyhow::Result<()> {
        loop {
            let frames = receive_handle.receive().await?;
            for mut frame in frames {
                // Check the packet is from remote XDP channel.
                let packet = Packet::new(frame.data_ref()).unwrap();
                if !matches!(packet.protocol(), Protocol::Unknown(XDP_PROTOCOL)) {
                    return Err(anyhow::anyhow!("Invalid protocol"));
                }
                // decapsulate the eth header for remote XDP channel.
                log::trace!("before adjust: {:?}",Packet::new(frame.data_ref()));
                frame.adjust_head(14);
                // Process
                let packet = Packet::new(frame.data_ref()).unwrap();
                log::trace!("after adjust: {:?}",packet);
                if packet.destination().is_broadcast() {
                    port_table
                        .for_each_port(|&_, send_handle| {
                            if !send_handle.is_remote() {
                                send_handle.send_raw_data(frame.data_ref().to_vec())
                            } else {
                                Ok(())
                            }
                        })
                        .await?;
                } else {
                    let Some(send_hanle) = port_table.get_send_handle(packet.destination()).await
                    else {
                        log::error!("Can't find send handle for {:?}", packet);
                        continue;
                    };
                    if send_hanle.is_remote() {
                        return Err(anyhow::anyhow!(
                            "Can't send to another remote port for remote port"
                        ));
                    }
                    send_hanle.send_frame(smallvec![frame])?;
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct RemoteXdpSendHandle {
    src_mac: HwAddr,
    dst_mac: HwAddr,
    xdp_send_handle: async_xdp::XdpSendHandle,
}

impl RemoteXdpSendHandle {
    pub fn send_frame(&self, mut frames: SmallVec<[Frame; BATCH_SIZSE]>) -> anyhow::Result<()> {
        for frame in &mut frames {
            log::trace!("before adjust: {:?}", Packet::new(frame.data_ref()));
            // Encapsulate the eth header for remote XDP channel.
            frame.adjust_head(-14);
            let data = frame.data_mut();
            let mut packet = Packet::new(data).unwrap();
            packet
                .set_protocol(Protocol::Unknown(XDP_PROTOCOL))?
                .set_source(self.src_mac)?
                .set_destination(self.dst_mac)?;
            log::trace!("after adjust: {:?}",packet);
        }
        self.xdp_send_handle.send_frame(frames)?;
        Ok(())
    }

    pub fn send_raw_data(&self, data: Vec<u8>) -> anyhow::Result<()> {
        let mut buffer = vec![0; data.len() + 14];

        let mut packet = Packet::new(&mut buffer).unwrap();
        packet
            .set_protocol(Protocol::Unknown(XDP_PROTOCOL))?
            .set_source(self.src_mac)?
            .set_destination(self.dst_mac)?;
        packet.payload_mut().copy_from_slice(&data);
            
        self.xdp_send_handle.send(buffer)?;
        Ok(())
    }
}

pub struct RemoteXdpPortServiceImpl {
    remote_xdp_manager: RemoteXdpManager,
    xdp_subnet_id: i32,
}

#[tonic::async_trait]
impl RemoteXdpPortService for RemoteXdpPortServiceImpl {
    async fn create_xdp_port(
        &self,
        request: tonic::Request<crate::proto::NodeInfo>,
    ) -> std::result::Result<tonic::Response<crate::proto::Status>, tonic::Status> {
        let NodeInfo {
            addr,
            eth_mac_addr,
            xdp_subnet_id,
        } = request
            .into_inner()
            .try_into()
            .map_err(|_| tonic::Status::invalid_argument("Invalid node info"))?;

        if xdp_subnet_id != self.xdp_subnet_id && self.xdp_subnet_id != -1 {
            return Err(tonic::Status::unavailable("Two node in different xdp subnet. Should not create remote xdp port. Some inconsistency happen in meta"));
        }

        let Some(eth_mac_addr) = eth_mac_addr else {
            return Err(tonic::Status::invalid_argument("Invalid mac address"));
        };

        self.remote_xdp_manager
            .create_remote_handle(addr, eth_mac_addr)
            .await
            .map_err(|_| tonic::Status::unknown("Fail to create xdp remote hadnle"))?;

        Ok(tonic::Response::new(crate::proto::Status {
            code: 0,
            message: String::new(),
        }))
    }
}
