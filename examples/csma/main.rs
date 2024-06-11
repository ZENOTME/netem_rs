mod csma;
use std::time::SystemTime;
use csma::{CsmaEvent, CsmaProcess};
use netem_rs::{
    des::serialize::{Event, EventReceiver, EventSimulator},
    Actor, ActorContext, DataView, LocalRunTime, PortTable,
};
use tokio::{select, sync::mpsc::UnboundedSender};

#[derive(Clone)]
struct EmptyDataView {
    control_tx: UnboundedSender<EventReceiver<CsmaEvent>>,
}

impl DataView for EmptyDataView {
    fn new_wtih_port_table(port_table: PortTable) -> Self {
        let (control_tx, control_rx) = tokio::sync::mpsc::unbounded_channel();

        std::thread::spawn(move || {
            let process = CsmaProcess::new(port_table);
            let mut simulator = EventSimulator::new(process, control_rx);
            simulator.run().unwrap();
        });

        Self { control_tx }
    }
}

struct ForwardActor {
    context: ActorContext<EmptyDataView>,
    tx: tokio::sync::mpsc::UnboundedSender<Event<CsmaEvent>>,
}

const SYNC_DURATION: std::time::Duration = std::time::Duration::from_micros(10);

impl Actor for ForwardActor {
    type C = EmptyDataView;

    fn new(context: ActorContext<Self::C>) -> Self {
        // test
        let cnt = 0;
        for _i in 0..cnt {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let rx = EventReceiver::new(rx);
            context.data_view.control_tx.send(rx).unwrap();
            context.runtime_handle.spawn(async move {
                let tx = tx;
                let mut interval = tokio::time::interval(SYNC_DURATION);
                loop {
                    interval.tick().await;
                    let now = std::time::SystemTime::now();
                    tx.send(Event::new_null(now)).unwrap();
                }
            });
        }

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let rx = EventReceiver::new(rx);
        context.data_view.control_tx.send(rx).unwrap();
        tx.send(Event::new(
            SystemTime::now(),
            CsmaEvent::NewDevice(context.receive_handle.port_id()),
        ))
        .unwrap();
        Self { context, tx }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut has_send = false;
        let mut interval = tokio::time::interval(SYNC_DURATION);
        let src_id = self.context.receive_handle.port_id();
        loop {
            select! {
                biased;
                frames = self.context.receive_handle.receive_frames() => {
                    for frame in frames? {
                        let now = std::time::SystemTime::now();
                        self.tx.send(Event::new(now, CsmaEvent::Send((src_id,frame)))).unwrap();
                    }
                    has_send = true;
                },
                _ = interval.tick() => {
                    if !has_send {
                        let now = std::time::SystemTime::now();
                        self.tx.send(Event::new_null(now)).unwrap();
                    }
                    has_send = false;
                }
            }
        }
    }
}

fn main() {
    env_logger::init();
    LocalRunTime::start::<ForwardActor>();
}
