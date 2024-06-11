use std::{
    sync::{atomic::AtomicUsize, Arc},
    thread,
};

use async_xdp::{Frame, PollerRunner};
use core_affinity::CoreId;
use log::error;
use netem_rs::{
    des::{
        batch::EventSimulator,
        parallel::{
            EventProperty, EventReadOnlyProcess, EventReadWriteProcess,
            EventSimulator as ParallelEventSimulator,
        },
        Event, EventReceiver, EventWrap,
    },
    proto::event,
    Actor, ActorContext, DataView, LocalRunTime, PortTable,
};
use smallvec::smallvec;
use std::fmt::Debug;
use tokio::{runtime::Builder, select, sync::mpsc::UnboundedSender};

// cpu allocation for test
const POLLER_RUNNER_CORES: [usize; 5] = [0, 1, 2, 3, 4];
const EVENT_SIMULATOR_CORE: usize = 9;
// const ACTOR_SIMULATOR_CORES: [usize; 25] = [
//     3, 4, 5, 6, 7, 8, 2, 0, 1, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
// ];
const ACTOR_SIMULATOR_CORES: [usize; 8] = [20, 21, 22, 23, 24, 25, 26, 27];

enum CsmaEvent {
    // Read-Only
    TransmitStart(Option<Frame>),
    // Read-Write
    TryTransmit(Option<Frame>),
    // Read-Write
    Transmit(u32, Option<Frame>),
    // Read-Write
    TransmitEnd(u32, Option<Frame>),
}

impl Debug for CsmaEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CsmaEvent::TransmitStart(_) => write!(f, "TransmitStart"),
            CsmaEvent::TryTransmit(_) => write!(f, "TryTransmit"),
            CsmaEvent::Transmit(id, _) => write!(f, "Transmit {id}"),
            CsmaEvent::TransmitEnd(id, _) => write!(f, "TransmitEnd {id}"),
        }
    }
}

impl EventProperty for CsmaEvent {
    fn is_read_only(&self) -> bool {
        matches!(self, CsmaEvent::TransmitStart(_))
    }
}

#[derive(Debug, Clone)]
enum CsmaState {
    Idle,
    Transmitting(u32),
    Propagating(u32),
}

#[derive(Clone)]
struct CsmaProcess {
    data_rate_bps: u64,
    delay_nano_sec: u64,
    state: CsmaState,
    interleave_event_cnt: u64,
    port_table: PortTable,
}

impl CsmaProcess {
    fn calculate_tx_nano_delay_for_data_rate(&self, pkt_sz: u64) -> u64 {
        (pkt_sz * 8) * 1_000_000_000 / self.data_rate_bps
    }
}

impl EventReadOnlyProcess for CsmaProcess {
    type T = CsmaEvent;
    type S = CsmaState;
    fn process_event(
        &mut self,
        event: &mut Event<Self::T>,
        result: &mut Vec<Event<Self::T>>,
        state: &Self::S,
    ) -> anyhow::Result<()> {
        let event_ts = *event.ts();
        match event.inner_mut() {
            CsmaEvent::TransmitStart(frame) => {
                if !matches!(state, CsmaState::Idle) {
                    return Ok(());
                }
                result.push(Event::new(event_ts, CsmaEvent::TryTransmit(frame.take())));
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

impl EventReadWriteProcess for CsmaProcess {
    type T = CsmaEvent;
    type S = CsmaState;
    type ROP = CsmaProcess;
    fn process_event(
        &mut self,
        event: &mut EventWrap<Self::T>,
        event_queue: &mut netem_rs::des::EventQueue<Self::T>,
    ) -> anyhow::Result<()> {
        let event_ts = *event.ts();
        match event.inner_mut() {
            CsmaEvent::TransmitStart(frame) => {
                if !matches!(self.state, CsmaState::Idle) {
                    return Ok(());
                }
                event_queue.enqueu_event(Box::new(EventWrap::from(Event::new(
                    event_ts,
                    CsmaEvent::TryTransmit(frame.take()),
                ))));
            }
            CsmaEvent::TryTransmit(frame) => {
                if !matches!(self.state, CsmaState::Idle) {
                    self.interleave_event_cnt += 1;
                    return Ok(());
                }
                let frame = frame.take().unwrap();
                let packet = packet::ether::Packet::new(frame.data_ref()).unwrap();
                let src_port_id = self.port_table.get_port_id(packet.source()).unwrap();
                self.state = CsmaState::Transmitting(src_port_id);
                let delay =
                    self.calculate_tx_nano_delay_for_data_rate(frame.data_ref().len() as u64);
                let new_event = Box::new(EventWrap::from(Event::new(
                    event
                        .ts()
                        .checked_add(std::time::Duration::from_nanos(delay))
                        .unwrap(),
                    CsmaEvent::Transmit(src_port_id, Some(frame)),
                )));
                event_queue.enqueu_event(new_event);
            }
            CsmaEvent::Transmit(src_port_id, frame) => {
                assert!(
                    matches!(self.state, CsmaState::Transmitting(src_id) if *src_port_id == src_id)
                );
                let frame = frame.take().unwrap();
                let new_event = Box::new(EventWrap::from(Event::new(
                    event_ts
                        .checked_add(std::time::Duration::from_nanos(self.delay_nano_sec))
                        .unwrap(),
                    CsmaEvent::TransmitEnd(*src_port_id, Some(frame)),
                )));
                self.state = CsmaState::Propagating(*src_port_id);
                event_queue.enqueu_event(new_event);
            }
            CsmaEvent::TransmitEnd(src_port_id, frame) => {
                let src_port_id = *src_port_id;
                assert!(
                    matches!(self.state, CsmaState::Propagating(port_id) if port_id == src_port_id)
                );
                let frame = frame.take().unwrap();
                let packet = packet::ether::Packet::new(frame.data_ref()).unwrap();
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
                // println!("interleave_event_cnt: {}", self.interleave_event_cnt);
                self.interleave_event_cnt = 0;
                self.state = CsmaState::Idle;
            }
        }
        Ok(())
    }

    fn clone_current_state(&self) -> Self::S {
        self.state.clone()
    }

    fn clone_read_only_process(&self) -> Self::ROP {
        self.clone()
    }

    fn process_read_only_event_batch(
        &mut self,
        event: &mut [EventWrap<Self::T>],
        event_queue: &mut netem_rs::des::EventQueue<Self::T>,
        state: &Self::S,
    ) -> anyhow::Result<()> {
        if !matches!(state, CsmaState::Idle) {
            return Ok(());
        }
        let event = event.first_mut().unwrap();
        let event_ts = *event.ts();
        match event.inner_mut() {
            CsmaEvent::TransmitStart(frame) => {
                if !matches!(state, CsmaState::Idle) {
                    return Ok(());
                }
                event_queue.enqueu_event(Box::new(EventWrap::from(Event::new(
                    event_ts,
                    CsmaEvent::TryTransmit(frame.take()),
                ))));
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

#[derive(Clone)]
struct EmptyDataView {
    control_tx: UnboundedSender<EventReceiver<CsmaEvent>>,
}

impl DataView for EmptyDataView {
    fn new_wtih_port_table(port_table: PortTable) -> Self {
        let event_prosser = CsmaProcess {
            port_table,
            data_rate_bps: u64::MAX,
            delay_nano_sec: 0,
            state: CsmaState::Idle,
            interleave_event_cnt: 0,
        };
        let (control_tx, control_rx) = tokio::sync::mpsc::unbounded_channel();

        std::thread::spawn(move || {
            core_affinity::set_for_current(CoreId {
                id: EVENT_SIMULATOR_CORE,
            });
            //let mut simulator = ParallelEventSimulator::new(event_prosser, control_rx, 4);
            let mut simulator = EventSimulator::new(event_prosser, control_rx);
            simulator.run().unwrap();
        });

        Self { control_tx }
    }
}

struct ForwardActor {
    context: ActorContext<EmptyDataView>,
    tx: tokio::sync::mpsc::UnboundedSender<Event<CsmaEvent>>,
}

const SYNC_DURATION: std::time::Duration = std::time::Duration::from_micros(100);

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
                    tx.send(Event::new_null(now, CsmaEvent::TransmitStart(None)))
                        .unwrap();
                }
            });
        }

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let rx = EventReceiver::new(rx);
        context.data_view.control_tx.send(rx).unwrap();
        Self { context, tx }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut has_send = false;
        let mut interval = tokio::time::interval(SYNC_DURATION);
        loop {
            select! {
                biased;
                frames = self.context.receive_handle.receive_frames() => {
                    for frame in frames? {
                        let now = std::time::SystemTime::now();
                        self.tx.send(Event::new(now, CsmaEvent::TransmitStart(Some(frame)))).unwrap();
                    }
                    has_send = true;
                },
                _ = interval.tick() => {
                    if !has_send {
                        let now = std::time::SystemTime::now();
                        self.tx.send(Event::new_null(now, CsmaEvent::TransmitStart(None))).unwrap();
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
