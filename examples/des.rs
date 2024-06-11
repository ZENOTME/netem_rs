use std::{
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
    task::{Context, Poll},
    thread,
    time::Instant,
};

use async_xdp::{Frame, PollerRunner};
use core_affinity::CoreId;
use futures::Future;
use log::error;
use netem_rs::{
    des::{Event, EventProcess, EventReceiver, EventSimulator, EventWrap},
    Actor, ActorContext, DataView, LocalRunTime, PortTable,
};
use smallvec::smallvec;
use tokio::{runtime::Builder, select, sync::mpsc::UnboundedSender};

// cpu allocation for test
const POLLER_RUNNER_CORES: [usize; 4] = [
    0, 1, 2, 3,
];
const EVENT_SIMULATOR_CORE: usize = 4;
// const ACTOR_SIMULATOR_CORES: [usize; 25] = [
//     3, 4, 5, 6, 7, 8, 2, 0, 1, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
// ];
const ACTOR_SIMULATOR_CORES: [usize; 4] = [5,6,7,12];

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
            core_affinity::set_for_current(CoreId {
                id: EVENT_SIMULATOR_CORE,
            });
            let mut simulator = EventSimulator::new(event_prosser, control_rx, None);
            simulator.run().unwrap();
        });

        Self { control_tx }
    }
}

struct FutureWrap<F> {
    future: F,
    last_end_time: Option<Instant>,
    start_time: Option<Instant>,
}

impl<F> FutureWrap<F> {
    fn new(future: F) -> Self {
        FutureWrap {
            future,
            last_end_time: None,
            start_time: None,
        }
    }
}

impl<F: Future + Unpin> Future for FutureWrap<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let now = Instant::now();

        if let Some(last_end) = self.last_end_time {
            let interval = now.duration_since(last_end);
            println!("Task was rescheduled after {:?}", interval);
        }

        self.start_time = Some(now);
        let result = Pin::new(&mut self.future).poll(cx);
        let end_time = Instant::now();

        if result.is_ready() {
            println!(
                "Task completed, total time: {:?}",
                end_time.duration_since(self.start_time.unwrap())
            );
        } else {
            self.last_end_time = Some(end_time);
        }

        result
    }
}

struct ForwardActor {
    context: ActorContext<EmptyDataView>,
    tx: tokio::sync::mpsc::UnboundedSender<Event<EmptyEvent>>,
}

const SYNC_DURATION: std::time::Duration = std::time::Duration::from_micros(1);

impl ForwardActor {
    async fn runn_inner(&mut self) -> anyhow::Result<()> {
        let mut has_send = false;
        let mut interval = tokio::time::interval(SYNC_DURATION);
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

impl Actor for ForwardActor {
    type C = EmptyDataView;

    fn new(context: ActorContext<Self::C>) -> Self {
        // test
        let cnt = 0;
        for _i in 0..cnt {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            // context.data_view.control_tx.send(EventReceiver::new(rx)).unwrap();
            context.runtime_handle.spawn(async move {
                let tx = tx;
                let mut rx = rx;
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(1));
                loop {
                    interval.tick().await;
                    let now = std::time::SystemTime::now();
                    tx.send(Event::new_null(now, EmptyEvent { frame: None }))
                        .unwrap();
                    // let _ = rx.try_recv().unwrap();
                }
            });
        }

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let rx = EventReceiver::new(rx);
        context.data_view.control_tx.send(rx).unwrap();
        Self { context, tx }
    }

    fn run(&mut self) -> impl Future<Output = Result<(), anyhow::Error>> + Send {
        let fut = Box::pin(self.runn_inner());
        // FutureWrap::new(fut)
        fut
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
        .worker_threads(4)
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
