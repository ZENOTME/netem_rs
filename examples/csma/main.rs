use std::{
    sync::{atomic::AtomicUsize, Arc},
    thread,
    time::SystemTime,
};
mod csma;
use async_xdp::PollerRunner;
use core_affinity::CoreId;
use csma::{CsmaEvent, CsmaProcess};
use netem_rs::{
    des::serialize::{Event, EventReceiver, EventSimulator},
    Actor, ActorContext, DataView, LocalRunTime, PortTable,
};
use tokio::{runtime::Builder, select, sync::mpsc::UnboundedSender};

// cpu allocation for test
const POLLER_RUNNER_CORES: [usize; 5] = [0, 1, 2, 3, 4];
const EVENT_SIMULATOR_CORE: usize = 9;
// const ACTOR_SIMULATOR_CORES: [usize; 25] = [
//     3, 4, 5, 6, 7, 8, 2, 0, 1, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
// ];
const ACTOR_SIMULATOR_CORES: [usize; 8] = [20, 21, 22, 23, 24, 25, 26, 27];

#[derive(Clone)]
struct EmptyDataView {
    control_tx: UnboundedSender<EventReceiver<CsmaEvent>>,
}

impl DataView for EmptyDataView {
    fn new_wtih_port_table(port_table: PortTable) -> Self {
        let (control_tx, control_rx) = tokio::sync::mpsc::unbounded_channel();

        std::thread::spawn(move || {
            let process = CsmaProcess::new(port_table);
            core_affinity::set_for_current(CoreId {
                id: EVENT_SIMULATOR_CORE,
            });
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
        )).unwrap();
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

struct BindCorePollerRunner {
    cores: Vec<usize>,
    idx: AtomicUsize,
}

impl BindCorePollerRunner {
    pub fn new(cores: Vec<usize>) -> Self {
        Self {
            cores,
            idx: AtomicUsize::new(0),
        }
    }
}

impl PollerRunner for BindCorePollerRunner {
    fn add_poller(
        &self,
        mut poller: Box<dyn async_xdp::Poller>,
    ) -> anyhow::Result<async_xdp::JoinHandle<()>> {
        let idx = self.idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if idx >= self.cores.len() {
            panic!("no enough cores")
        }
        let core = self.cores[idx];
        Ok(thread::spawn(move || {
            core_affinity::set_for_current(CoreId { id: core });
            poller.init().unwrap();
            poller.run().unwrap();
        }))
    }
}

fn main() {
    env_logger::init();
    let core_binder = {
        let idx = Arc::new(AtomicUsize::new(0));
        let core_ids = Arc::new(ACTOR_SIMULATOR_CORES.to_vec());
        move || {
            let idx = idx.clone();
            let core_ids = core_ids.clone();
            let idx = idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let core_id = core_ids[idx];
            core_affinity::set_for_current(CoreId { id: core_id });
        }
    };
    let actor_runtime = Builder::new_multi_thread()
        .worker_threads(8)
        .thread_name("actor-thread")
        .on_thread_start(core_binder)
        .enable_all()
        .build()
        .unwrap();
    LocalRunTime::start_with_custom::<ForwardActor>(
        actor_runtime,
        Box::new(BindCorePollerRunner::new(POLLER_RUNNER_CORES.to_vec())),
    );
}
