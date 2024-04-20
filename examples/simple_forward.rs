use netem_rs::{Actor, ActorContext, DataView, LocalRunTime};
use packet::ether::Packet;
use smallvec::smallvec;

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
        let self_port_id = self.context.receive_handle.port_id();
        loop {
            let frames = self.context.receive_handle.receive_frames().await?;
            for frame in frames {
                let packet = Packet::new(frame.data_ref()).unwrap();
                if packet.destination().is_broadcast() {
                    self.context
                        .port_table
                        .for_each_port(|&port_id, send_handle| {
                            if port_id != self_port_id {
                                send_handle.send_raw_data(frame.data_ref().to_vec())
                            } else {
                                Ok(())
                            }
                        })
                        .await?;
                } else {
                    self.context
                        .port_table
                        .get_send_handle(packet.destination())
                        .await
                        .unwrap()
                        .send_frame(smallvec![frame])?;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    LocalRunTime::start::<ForwardActor>().await;
}
