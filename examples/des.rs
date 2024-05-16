use async_xdp::Frame;
use log::error;
use netem_rs::{
    des::{Event, EventProcess, EventReceiver, EventSimulator, EventWrap},
    Actor, ActorContext, DataView, LocalRunTime, PortTable,
};
use smallvec::smallvec;
use tokio::{select, sync::mpsc::UnboundedSender};

struct EmptyEvent {
    frame: Option<Frame>,
}

struct EmptyProcess {
    port_table: PortTable,
}

impl EventProcess for EmptyProcess {
    type T = EmptyEvent;

    fn process_event(
        &mut self,
        event: &mut EventWrap<Self::T>,
        _event_queue: &mut netem_rs::des::EventQueue<Self::T>,
        _is_optmiistic: bool,
    ) -> anyhow::Result<()> {
        let frame = event.inner_mut().frame.take().unwrap();
        let packet = packet::ether::Packet::new(frame.data_ref()).unwrap();
        let src_port_id = self.port_table.get_port_id(packet.source()).unwrap();
        if packet.destination().is_broadcast() {
            self.port_table.for_each_port(|&port_id, send_handle| {
                if port_id != src_port_id {
                    send_handle.send_raw_data(frame.data_ref().to_vec())
                } else {
                    Ok(())
                }
            })?;
        } else {
            if let Some(handle) = self.port_table.get_send_handle(packet.destination()) {
                handle.send_frame(smallvec![frame])?;
            } else {
                error!("No such port {}", packet.destination());
            }
        }
        Ok(())
    }

    fn redoable(&self, _event: &EventWrap<Self::T>) -> bool {
        false
    }

    fn undo_state(&mut self, _event: &EventWrap<Self::T>) -> anyhow::Result<()> {
        Ok(())
    }

    fn commit_state(&mut self, _event: &EventWrap<Self::T>) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct EmptyDataView {
    control_tx: UnboundedSender<EventReceiver<EmptyEvent>>,
}

impl DataView for EmptyDataView {
    fn new_wtih_port_table(port_table: PortTable) -> Self {
        let event_prosser = EmptyProcess { port_table };
        let (control_tx, control_rx) = tokio::sync::mpsc::unbounded_channel();
        std::thread::spawn(move || {
            let mut simulator = EventSimulator::new(event_prosser, control_rx, None);
            simulator.run().unwrap();
        });
        Self { control_tx }
    }
}

struct ForwardActor {
    context: ActorContext<EmptyDataView>,
    tx: tokio::sync::mpsc::UnboundedSender<Event<EmptyEvent>>,
}

impl Actor for ForwardActor {
    type C = EmptyDataView;

    fn new(context: ActorContext<Self::C>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let rx = EventReceiver::new(rx);
        context.data_view.control_tx.send(rx).unwrap();
        Self { context, tx }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut has_send = false;
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(5));
        loop {
            select! {
                biased;
                frames = self.context.receive_handle.receive_frames() => {
                    for frame in frames? {
                        let now = std::time::SystemTime::now();
                        self.tx.send(Event::new(now, EmptyEvent { frame: Some(frame) })).unwrap();
                    }
                    has_send = true;
                },
                _ = interval.tick() => {
                    if !has_send {
                        let now = std::time::SystemTime::now();
                        self.tx.send(Event::new_null(now, EmptyEvent {frame: None })).unwrap();
                    }
                    has_send = false;
                }
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    env_logger::init();
    LocalRunTime::start::<ForwardActor>().await;
}
