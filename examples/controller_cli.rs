use netem_rs::proto::{actor_service_client, CreateActorRequest};

#[tokio::main]
async fn main() {
    env_logger::init();

    let mut client = actor_service_client::ActorServiceClient::connect("http://127.0.0.1:10000")
        .await
        .unwrap();
    client
        .create_actor(CreateActorRequest {
            if_name: "veth1".to_string(),
            queue_id: 0,
            port_type: "xdp".to_string(),
            mac_addr: vec![0xaa, 0, 0, 0, 0, 0],
        })
        .await
        .unwrap();

    let mut client = actor_service_client::ActorServiceClient::connect("http://127.0.0.1:10001")
        .await
        .unwrap();
    client
        .create_actor(CreateActorRequest {
            if_name: "veth2".to_string(),
            queue_id: 0,
            port_type: "xdp".to_string(),
            mac_addr: vec![0xaa, 0, 0, 0, 0, 1],
        })
        .await
        .unwrap();
}
