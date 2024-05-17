use crate::{
    meta::NodeInfo,
    proto::{
        actor_service_server::ActorServiceServer,
        remote_port_service_server::RemotePortServiceServer,
        remote_xdp_port_service_server::RemoteXdpPortServiceServer,
    },
    start_actor, start_remote_grpc, start_remote_xdp, Actor, ActorManager, GrpcTransportManager,
    MetaClient, PortTable, RemoteXdpManager, XdpManager, XdpManagerConfig,
};
use anyhow::Result;
use async_xdp::SingleThreadRunner;
use clap::Parser;
use log::trace;
use tokio::runtime::Runtime;
use std::sync::Arc;
use tonic::transport::Server;

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
pub struct RemoteRuntimeArgs {
    #[clap(long, default_value = "127.0.0.1:5688")]
    pub listen_addr: String,

    #[clap(long)]
    pub meta_address: String,

    #[clap(long)]
    pub remote_xdp_mode: bool,

    #[clap(long)]
    pub eth_iface: Option<String>,

    #[clap(long)]
    pub eth_mac_addr: Option<String>,

    #[clap(long)]
    pub xdp_subnet_id: Option<i32>,

    #[clap(long)]
    pub xdp_program: Option<String>,

    #[clap(long)]
    pub xdp_program_section: Option<String>,
}

pub struct RemtoeRuntime {}

impl RemtoeRuntime {
    pub async fn start<A: Actor, M: MetaClient>() -> Result<()> {
        let args = RemoteRuntimeArgs::parse();

        if args.remote_xdp_mode {
            assert!(args.eth_iface.is_some());
            assert!(args.eth_mac_addr.is_some());
            assert!(args.xdp_subnet_id.is_some());
            assert!(args.xdp_program.is_some());
        }

        let self_node_info = NodeInfo {
            addr: args.listen_addr.as_str().try_into().unwrap(),
            eth_mac_addr: args.eth_mac_addr.map(|mac| mac.parse().unwrap()),
            xdp_subnet_id: args.xdp_subnet_id.unwrap_or(-1),
        };

        trace!("Start to regsiter to meta..");
        let meta = M::connet(args.meta_address.as_str().try_into().unwrap());
        let remote_nodes = meta
            .register(args.listen_addr.as_str().try_into().unwrap())
            .await;

        let port_table = PortTable::new();
        let xdp_manager = Arc::new(XdpManager::new(
            XdpManagerConfig::default(),
            Box::new(SingleThreadRunner::new()),
        ));

        let actor_manager: ActorManager<A::C> =
            ActorManager::new(xdp_manager.clone(), port_table.clone(), Runtime::new().unwrap());

        let connect_with_grpc = |node: &NodeInfo| -> bool {
            !args.remote_xdp_mode
                || node.xdp_subnet_id == -1
                || args.xdp_subnet_id.unwrap() != node.xdp_subnet_id
        };

        // start remote xdp service
        let remote_xdp_service = if args.remote_xdp_mode {
            let remote_xdp_manager = RemoteXdpManager::new(
                xdp_manager,
                port_table.clone(),
                &args.xdp_program.unwrap(),
                &args.xdp_program_section.unwrap_or("".to_string()),
                &args.eth_iface.unwrap(),
                self_node_info.eth_mac_addr.as_ref().cloned().unwrap(),
            )
            .await?;
            Some(
                start_remote_xdp(
                    remote_nodes.clone(),
                    remote_xdp_manager,
                    self_node_info.clone(),
                )
                .await?,
            )
        } else {
            None
        };

        // start grpc transport service
        let grpc_transport_manager = Arc::new(GrpcTransportManager::new(port_table.clone()));
        let grpc_nodes = remote_nodes
            .iter()
            .filter_map(|node| {
                if connect_with_grpc(node) {
                    Some(node.addr.clone())
                } else {
                    None
                }
            })
            .collect();
        trace!(
            "Start to build grpc transport with remote hosts: {:?}",
            grpc_nodes
        );
        let grpc_service = start_remote_grpc(
            self_node_info.clone(),
            grpc_nodes,
            grpc_transport_manager.clone(),
            port_table.clone(),
        )
        .await?;

        // start actor service
        let remote_node_info = remote_nodes.iter().map(|node| node.addr.clone()).collect();
        let actor_serive =
            start_actor::<A>(actor_manager, self_node_info, remote_node_info).await?;

        let actor_server = ActorServiceServer::new(actor_serive);
        let remote_port_server = RemotePortServiceServer::new(grpc_service);
        let remote_xdp_server = remote_xdp_service.map(RemoteXdpPortServiceServer::new);

        trace!("Start to serve..");

        let builder = Server::builder()
            .initial_stream_window_size(33554432)
            .add_service(actor_server)
            .add_service(remote_port_server);
        let builder = if let Some(server) = remote_xdp_server {
            builder.add_service(server)
        } else {
            builder
        };
        builder
            .serve(args.listen_addr.as_str().parse().unwrap())
            .await
            .unwrap();

        Ok(())
    }
}
