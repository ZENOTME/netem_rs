use netem_rs::{Actor, ActorContext, DataView, LocalRunTime};
use packet::ether::Packet;

#[derive(Clone)]
struct EmptyDataView;

impl DataView for EmptyDataView {
    fn new() -> Self {
        Self
    }
}

struct ForwardActor {
    context: ActorContext<EmptyDataView>,
}

impl ForwardActor {
    fn new(context: ActorContext<EmptyDataView>) -> Self {
        Self { context }
    }
}

impl Actor for ForwardActor {
    type C = EmptyDataView;

    fn new(context: ActorContext<Self::C>) -> Self {
        Self::new(context)
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            let frames = self.context.receive_handle.receive().await?;
            for frame in frames {
                let payload = self.context.packet_data(&frame);
                let data = payload.contents();
                let packet = Packet::new(data).unwrap();
                if packet.destination().is_broadcast() {
                    self.context
                        .send_handles
                        .read()
                        .await
                        .values()
                        .for_each(|send_handle| send_handle.send(data.to_vec()).unwrap());
                } else {
                    let port_id = self
                        .context
                        .route_table
                        .get_port_id(packet.destination())
                        .await;
                    if let Some(port_id) = port_id {
                        self.context
                            .send_handles
                            .read()
                            .await
                            .get(&port_id)
                            .unwrap()
                            .send_frame(vec![frame].into())
                            .unwrap();
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    LocalRunTime::start::<ForwardActor>().await;
}
