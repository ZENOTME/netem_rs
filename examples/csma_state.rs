use std::{
    sync::{atomic::AtomicUsize, Arc},
    thread,
    time::SystemTime,
};

use async_xdp::{Frame, PollerRunner};
use core_affinity::CoreId;
use log::error;
use netem_rs::{
    des::{
        realtime_executor::{Event, EventProcess, EventRumtime, EventRuntimeHandle},
        state::ShareState,
    },
    Actor, ActorContext, DataView, LocalRunTime, PortTable,
};
use smallvec::smallvec;
use std::fmt::Debug;
use tokio::{
    runtime::Builder,
    select,
    sync::{mpsc::UnboundedSender, Mutex},
};

// cpu allocation for test
const POLLER_RUNNER_CORES: [usize; 5] = [0, 1, 2, 3, 4];
const EVENT_SIMULATOR_CORE: usize = 9;
// const ACTOR_SIMULATOR_CORES: [usize; 25] = [
//     3, 4, 5, 6, 7, 8, 2, 0, 1, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
// ];
const ACTOR_SIMULATOR_CORES: [usize; 8] = [20, 21, 22, 23, 24, 25, 26, 27];

enum CsmaEvent {
    TransmitStart(Option<Frame>),
    // after delay of data rate
    Transmit(u32, Option<Frame>),
    // after delay
    TransmitEnd(u32, Option<Frame>),
}

impl Debug for CsmaEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CsmaEvent::TransmitStart(_) => write!(f, "TransmitStart"),
            CsmaEvent::Transmit(id, _) => write!(f, "Transmit {id}"),
            CsmaEvent::TransmitEnd(id, _) => write!(f, "TransmitEnd {id}"),
        }
    }
}

#[derive(Clone, Debug)]
enum CsmaState {
    Idle,
    Transmitting(u32),
    Propagating(u32),
}

#[derive(Clone)]
struct CsmaProcess {
    state: Arc<Mutex<CsmaState>>,
    port_table: PortTable,
}

impl EventProcess for CsmaProcess {
    type T = CsmaEvent;

    fn process_event(
        &mut self,
        event: netem_rs::des::realtime_executor::Event<Self::T>,
        queue: &mut netem_rs::des::realtime_executor::RealTimeQueue<Self::T>,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send {
        async move {
            // println!("event: {:?}", event.data);
            match event.data {
                CsmaEvent::TransmitStart(frame) => {
                    let mut state = self.state.lock().await;
                    if !matches!(*state, CsmaState::Idle) {
                        return Ok(());
                    }
                    let packet =
                        packet::ether::Packet::new(frame.as_ref().unwrap().data_ref()).unwrap();
                    let src_port_id = self.port_table.get_port_id(packet.source()).unwrap();
                    *state = CsmaState::Transmitting(src_port_id);
                    queue.enqueue_event(Box::new(Event::new(
                        CsmaEvent::TransmitEnd(src_port_id, frame),
                        event.time,
                    )));
                    return Ok(());
                }
                // loop {
                //     let mut tx = self.state.init_read_write_transaction(event.time).await;
                //     if !matches!(tx.get_state(), CsmaState::Idle) {
                //         self.state.abort_read_write(tx).await;
                //         return Ok(());
                //     }
                //     let packet =
                //         packet::ether::Packet::new(frame.as_ref().unwrap().data_ref()).unwrap();
                //     let src_port_id = self.port_table.get_port_id(packet.source()).unwrap();
                //     *tx.get_state_mut() = CsmaState::Transmitting(src_port_id);
                //     if self.state.commit_read_write(tx).await {
                //         queue.enqueue_event(Box::new(Event::new(
                //             CsmaEvent::TransmitEnd(src_port_id, frame),
                //             event.time,
                //         )));
                //         return Ok(());
                //     }
                // },
                CsmaEvent::Transmit(src_port_id, frame) => todo!(),
                // loop {
                //     let mut tx = self.state.init_read_write_transaction(event.time).await;
                //     *tx.get_state_mut() = CsmaState::Propagating(src_port_id);
                //     if self.state.commit_read_write(tx).await {
                //         queue.enqueue_event(Box::new(Event::new(
                //             CsmaEvent::TransmitEnd(src_port_id, frame),
                //             event.time,
                //         )));
                //         return Ok(());
                //     }
                // },
                CsmaEvent::TransmitEnd(src_port_id, frame) => {
                // loop {
                    // let mut tx = self.state.init_read_write_transaction(event.time).await;
                    // *tx.get_state_mut() = CsmaState::Idle;
                    // if self.state.commit_read_write(tx).await {
                        *self.state.lock().await = CsmaState::Idle;
                        let packet =
                            packet::ether::Packet::new(frame.as_ref().unwrap().data_ref()).unwrap();
                        if packet.destination().is_broadcast() {
                            self.port_table.for_each_port(|&port_id, send_handle| {
                                if port_id != src_port_id {
                                    send_handle
                                        .send_raw_data(frame.as_ref().unwrap().data_ref().to_vec())
                                } else {
                                    Ok(())
                                }
                            })?;
                        } else {
                            if let Some(handle) =
                                self.port_table.get_send_handle(packet.destination())
                            {
                                handle.send_frame(smallvec![frame.unwrap()])?;
                            } else {
                                error!("No such port {}", packet.destination());
                            }
                        }
                        return Ok(());
                    //     return Ok(());
                    // }
                },
            }
        }
    }
}

#[derive(Clone)]
struct EmptyDataView {
    runtime_handle: EventRuntimeHandle<CsmaEvent>,
}

impl DataView for EmptyDataView {
    fn new_wtih_port_table_runtime_handle(
        port_table: PortTable,
        handle: tokio::runtime::Handle,
    ) -> Self {
        let csma_process = CsmaProcess {
            // state: ShareState::new(CsmaState::Idle),
            state: Arc::new(Mutex::new(CsmaState::Idle)),
            port_table,
        };
        let runtime = EventRumtime::start(1, csma_process, handle);

        Self {
            runtime_handle: runtime.handle(),
        }
    }

    fn new_wtih_port_table(port_table: PortTable) -> Self {
        todo!()
    }
}

struct ForwardActor {
    context: ActorContext<EmptyDataView>,
    tx: EventRuntimeHandle<CsmaEvent>,
}

const SYNC_DURATION: std::time::Duration = std::time::Duration::from_micros(10);

impl Actor for ForwardActor {
    type C = EmptyDataView;

    fn new(context: ActorContext<Self::C>) -> Self {
        Self {
            tx: context.data_view.runtime_handle.clone(),
            context,
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            for frame in self.context.receive_handle.receive_frames().await? {
                let now = std::time::SystemTime::now();
                self.tx
                    .spawn_event(Event::new(CsmaEvent::TransmitStart(Some(frame)), now), 0);
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
