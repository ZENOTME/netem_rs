use std::fs;

use clap::{command, Parser};

use crate::{Actor, ActorConfig, ActorManager};

#[derive(Parser)]
#[command(about, long_about = None)]
struct LocalRunTimeArgs {
    /// toml config file include actor config
    #[arg(short, long)]
    toml: String,
}

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
            let port_id = v["port_id"].as_integer().unwrap() as u32;
            let veth_name = v["veth_name"].as_str().unwrap().to_string();
            let queue_id = v["queue_id"].as_integer().unwrap() as u32;
            Ok(ActorConfig {
                actor_id,
                port_id,
                veth_name,
                queue_id,
            })
        })
        .collect()
}

pub struct LocalRunTime;

impl LocalRunTime {
    pub async fn start<A: Actor>() {
        let args = LocalRunTimeArgs::parse();
        let config = parse_toml_config_file(&args.toml).unwrap();

        let mut actor_manager: ActorManager<A::C> = ActorManager::default();
        for actor_config in config {
            actor_manager.add_actor::<A>(actor_config).await.unwrap();
        }

        loop {}
    }
}
