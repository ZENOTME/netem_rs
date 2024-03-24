use std::{fs, str::FromStr};

use clap::{command, Parser};
use hwaddr::HwAddr;

use crate::{
    Actor, ActorConfig, ActorManager, PortConfig, PortManager, RouteTable, XdpConfig,
    XdpManagerConfig,
};

#[derive(Parser)]
#[command(about, long_about = None)]
struct LocalRunTimeArgs {
    /// toml config file include actor config
    #[arg(short, long)]
    toml: String,
}

/// Parse the toml config file to get the actor config.
/// The toml file should be like:
/// ```toml
/// [actor1]
/// actor_id = 1
/// port_type = "xdp"
/// port_id = 0
/// if_name = "veth0"
/// queue_id = 0
/// mac_addr = "00:00:00:00:00:01"
/// ```
fn parse_toml_config_file(yml: &str) -> anyhow::Result<Vec<ActorConfig>> {
    let file_content = match fs::read_to_string(yml) {
        Ok(content) => content,
        Err(error) => {
            panic!("Failed to read file: {}", error);
        }
    };
    let table: toml::Table = toml::from_str(&file_content)?;
    table
        .into_iter()
        .map(|(_k, v)| {
            let v = v.as_table().unwrap();
            let actor_id = v["actor_id"].as_integer().unwrap() as u32;
            let port_ty = v["port_type"].as_str().unwrap();
            let port_id = v["port_id"].as_integer().unwrap() as u32;
            let veth_name = v["if_name"].as_str().unwrap().to_string();
            let queue_id = v["queue_id"].as_integer().unwrap() as u32;
            let mac_addr = v["mac_addr"].as_str().unwrap().to_string();

            if port_ty != "xdp" {
                return Err(anyhow::anyhow!("Unsupported port type: {}", port_ty));
            }

            Ok(ActorConfig {
                actor_id,
                port_config: PortConfig::xdp_config(
                    port_id,
                    XdpConfig::new_with_default_socket_config(
                        veth_name,
                        queue_id,
                        HwAddr::from_str(&mac_addr).unwrap(),
                    ),
                ),
            })
        })
        .collect()
}

pub struct LocalRunTime;

impl LocalRunTime {
    pub async fn start<A: Actor>() {
        let args = LocalRunTimeArgs::parse();
        let config = parse_toml_config_file(&args.toml).unwrap();

        let route_table = RouteTable::new();
        let port_manager = PortManager::new(route_table.clone(), XdpManagerConfig::default());

        let mut actor_manager: ActorManager<A::C> = ActorManager::new(port_manager, route_table);

        for actor_config in config {
            actor_manager.add_actor::<A>(actor_config).await.unwrap();
        }

        actor_manager.join_all().await;
    }
}
