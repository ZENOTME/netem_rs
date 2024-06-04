use async_xdp::Frame;
use log::{error, trace};
use netem_rs::des::serialize::{Event, EventProcess, EventQueueRef, EventWrap};
use netem_rs::{PortSendHandleImpl, PortTable};
use rand::Rng;
use smallvec::smallvec;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::rc::Rc;
use std::time::Duration;

fn calculate_tx_nano_delay_for_data_rate(pkt_sz: u64, data_rate_bps: u64) -> u64 {
    (pkt_sz * 8) * 1_000_000_000 / data_rate_bps
}

pub enum CsmaEvent {
    NewDevice(u32),
    Send((u32, Frame)),
}

impl Debug for CsmaEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CsmaEvent::NewDevice(_) => write!(f, "NewDevice"),
            CsmaEvent::Send(_) => write!(f, "Send"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum CsmaState {
    Idle,
    Transmitting,
}

#[derive(Debug, PartialEq, Eq)]
enum CsmaDeciveTxState {
    Ready,
    Busy,
    Gap,
    Backoff,
}

struct Backoff {
    slot_time: Duration,
    min_slots: u32,
    max_slots: u32,
    ceiling: u32,
    max_retries: u32,
    num_backoff_retries: u32,
    rng: rand::rngs::ThreadRng,
}

impl Backoff {
    fn new() -> Self {
        Self {
            slot_time: Duration::from_micros(1),
            min_slots: 1,
            max_slots: 1000,
            ceiling: 10,
            max_retries: 1000,
            num_backoff_retries: 0,
            rng: rand::thread_rng(),
        }
    }

    fn _with_params(
        slot_time: Duration,
        min_slots: u32,
        max_slots: u32,
        ceiling: u32,
        max_retries: u32,
    ) -> Self {
        Self {
            slot_time,
            min_slots,
            max_slots,
            ceiling,
            max_retries,
            num_backoff_retries: 0,
            rng: rand::thread_rng(),
        }
    }

    fn get_backoff_time(&mut self) -> Duration {
        let ceiling = if self.ceiling > 0 && self.num_backoff_retries > self.ceiling {
            self.ceiling
        } else {
            self.num_backoff_retries
        };

        let min_slot = self.min_slots;
        let max_slot = 2u32.pow(ceiling) - 1;
        let max_slot = if max_slot > self.max_slots {
            self.max_slots
        } else {
            max_slot
        };

        let backoff_slots = self.rng.gen_range(min_slot..=max_slot);
        self.slot_time * backoff_slots
    }

    fn reset_backoff_time(&mut self) {
        self.num_backoff_retries = 0;
    }

    fn max_retries_reached(&self) -> bool {
        self.num_backoff_retries >= self.max_retries
    }

    fn incr_num_retries(&mut self) {
        self.num_backoff_retries += 1;
    }
}

struct Queue {
    queue: VecDeque<Frame>,
    max_queue_size: usize,
}

impl Queue {
    fn new(max_queue_size: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(max_queue_size),
            max_queue_size,
        }
    }

    fn enqueue(&mut self, frame: Frame) -> bool {
        if self.queue.len() < self.max_queue_size {
            self.queue.push_back(frame);
            true
        } else {
            false
        }
    }

    fn dequeue(&mut self) -> Option<Frame> {
        self.queue.pop_front()
    }
}

struct CsmaDecive {
    tx_state: RefCell<CsmaDeciveTxState>,
    backoff: RefCell<Backoff>,
    channel: Rc<RefCell<CsmaChannel>>,
    data_rate: u64,
    queue: RefCell<Queue>,
    current_packet: RefCell<Option<Frame>>,
    interframe_gap: Duration,
    send_handle: PortSendHandleImpl,
}

impl CsmaDecive {
    pub fn new(csma_channel: Rc<RefCell<CsmaChannel>>, send_handle: PortSendHandleImpl) -> Self {
        Self {
            tx_state: RefCell::new(CsmaDeciveTxState::Ready),
            backoff: RefCell::new(Backoff::new()),
            data_rate: u64::MAX,
            channel: csma_channel,
            queue: RefCell::new(Queue::new(100)),
            current_packet: RefCell::new(None),
            interframe_gap: Duration::from_micros(0),
            send_handle,
        }
    }

    pub fn send(self: &Rc<Self>, frame: Frame, event_queue: EventQueueRef<CsmaEvent>) {
        trace!("dev send");
        if !self.queue.borrow_mut().enqueue(frame) {
            return;
        }

        if matches!(*self.tx_state.borrow(), CsmaDeciveTxState::Ready) {
            let frame = self.queue.borrow_mut().dequeue();
            if let Some(frame) = frame {
                *self.current_packet.borrow_mut() = Some(frame);
                self.clone().transmit_start(event_queue);
            }
        }
    }

    pub fn transmit_start(self: Rc<Self>, event_queue: EventQueueRef<CsmaEvent>) {
        trace!("dev transmit_start");
        assert!(
            matches!(
                *self.tx_state.borrow(),
                CsmaDeciveTxState::Ready | CsmaDeciveTxState::Backoff
            ),
            "transmit_start: tx_state is {:?}",
            self.tx_state
        );
        if self.channel.borrow().get_state() != CsmaState::Idle {
            *self.tx_state.borrow_mut() = CsmaDeciveTxState::Backoff;

            if self.backoff.borrow_mut().max_retries_reached() {
                self.transmit_abort(event_queue.clone());
            } else {
                self.backoff.borrow_mut().incr_num_retries();
                let backoff_time = self.backoff.borrow_mut().get_backoff_time();
                let new_ts = event_queue
                    .borrow()
                    .current_time()
                    .checked_add(backoff_time)
                    .unwrap();
                let event_queue_clone = event_queue.clone();
                let new_event = Box::new(EventWrap::new_func(
                    new_ts,
                    Box::new(move || {
                        self.transmit_start(event_queue_clone);
                    }),
                ));
                event_queue.borrow_mut().enqueue_event(new_event);
            }
        } else {
            let pkt_sz = self
                .current_packet
                .borrow()
                .as_ref()
                .unwrap()
                .data_ref()
                .len() as u64;
            if self
                .channel
                .borrow_mut()
                .transmit_start(self.current_packet.take().unwrap())
            {
                self.backoff.borrow_mut().reset_backoff_time();
                *self.tx_state.borrow_mut() = CsmaDeciveTxState::Busy;
                let nano_delay = calculate_tx_nano_delay_for_data_rate(pkt_sz, self.data_rate);
                let new_ts = event_queue
                    .borrow()
                    .current_time()
                    .checked_add(Duration::from_nanos(nano_delay))
                    .unwrap();
                let event_queue_clone = event_queue.clone();
                let new_event = Box::new(EventWrap::new_func(
                    new_ts,
                    Box::new(|| {
                        self.transmit_complete(event_queue_clone);
                    }),
                ));
                event_queue.borrow_mut().enqueue_event(new_event);
            } else {
                *self.tx_state.borrow_mut() = CsmaDeciveTxState::Ready;
            }
        }
    }

    pub fn transmit_abort(self: Rc<Self>, event_queue: EventQueueRef<CsmaEvent>) {
        trace!("dev transmit_abort");
        self.current_packet.take();
        assert!(
            matches!(*self.tx_state.borrow(), CsmaDeciveTxState::Backoff),
            "transmit_drop: tx_state is {:?}",
            self.tx_state
        );
        self.backoff.borrow_mut().reset_backoff_time();
        *self.tx_state.borrow_mut() = CsmaDeciveTxState::Ready;
        if let Some(frame) = self.queue.borrow_mut().dequeue() {
            *self.current_packet.borrow_mut() = Some(frame);
            self.clone().transmit_start(event_queue);
        }
    }

    pub fn transmit_complete(self: Rc<Self>, event_queue: EventQueueRef<CsmaEvent>) {
        trace!("dev transmit_complete");
        assert!(
            matches!(*self.tx_state.borrow(), CsmaDeciveTxState::Busy),
            "transmit_complete: tx_state is {:?}",
            self.tx_state
        );
        assert!(matches!(
            self.channel.borrow().get_state(),
            CsmaState::Transmitting
        ));
        *self.tx_state.borrow_mut() = CsmaDeciveTxState::Gap;

        self.channel.borrow_mut().transmit_end();
        self.current_packet.take();
        let new_ts = event_queue
            .borrow()
            .current_time()
            .checked_add(self.interframe_gap)
            .unwrap();
        let event_queue_clone = event_queue.clone();
        let new_event = Box::new(EventWrap::new_func(
            new_ts,
            Box::new(move || {
                self.clone().transmit_ready(event_queue_clone);
            }),
        ));
        event_queue.borrow_mut().enqueue_event(new_event);
    }

    pub fn transmit_ready(self: Rc<Self>, event_queue: EventQueueRef<CsmaEvent>) {
        trace!("dev transmit_ready");
        assert!(
            matches!(*self.tx_state.borrow(), CsmaDeciveTxState::Gap),
            "transmit_ready: tx_state is {:?}",
            self.tx_state
        );
        *self.tx_state.borrow_mut() = CsmaDeciveTxState::Ready;
        assert!(self.current_packet.borrow().is_none());

        if let Some(frame) = self.queue.borrow_mut().dequeue() {
            *self.current_packet.borrow_mut() = Some(frame);
            self.clone().transmit_start(event_queue);
        }
    }

    pub fn receive(&self, frame: Frame) {
        trace!("dev receive");
        self.send_handle.send_frame(smallvec![frame]).unwrap();
    }

    pub fn receive_data(&self, data: Vec<u8>) {
        trace!("dev receive_data");
        self.send_handle.send_raw_data(data).unwrap();
    }
}

struct CsmaChannel {
    state: CsmaState,
    current_pkt: Option<Frame>,
    port_table: PortTable,
    devices: HashMap<u32, Rc<CsmaDecive>>,
}

impl CsmaChannel {
    pub fn add_device(&mut self, port_id: u32, device: Rc<CsmaDecive>) {
        self.devices.insert(port_id, device);
    }

    pub fn get_state(&self) -> CsmaState {
        self.state.clone()
    }

    pub fn transmit_start(&mut self, packet: Frame) -> bool {
        trace!("csma_channel transmit_start");
        if self.state != CsmaState::Idle {
            return false;
        }

        self.current_pkt = Some(packet);
        self.state = CsmaState::Transmitting;
        true
    }

    pub fn transmit_end(&mut self) {
        trace!("csma_channel transmit_end");
        let packet =
            packet::ether::Packet::new(self.current_pkt.as_ref().unwrap().data_ref()).unwrap();
        let src_port_id = self.port_table.get_port_id(packet.source()).unwrap();
        if packet.destination().is_broadcast() {
            self.devices.iter().for_each(|(port_id, device)| {
                if *port_id != src_port_id {
                    device.receive_data(packet.as_ref().to_vec());
                }
            });
        } else {
            if let Some(port_id) = self.port_table.get_port_id(packet.destination()) {
                if let Some(device) = self.devices.get(&port_id) {
                    device.receive(self.current_pkt.take().unwrap());
                } else {
                    error!("No such port {}", packet.destination());
                }
            }
        }
        self.state = CsmaState::Idle;
    }
}

pub struct CsmaProcess {
    channel: Rc<RefCell<CsmaChannel>>,
    device: HashMap<u32, Rc<CsmaDecive>>,
    port_table: PortTable,
}

impl CsmaProcess {
    pub fn new(port_table: PortTable) -> Self {
        let channel = Rc::new(RefCell::new(CsmaChannel {
            state: CsmaState::Idle,
            current_pkt: None,
            port_table: port_table.clone(),
            devices: HashMap::new(),
        }));
        Self {
            channel,
            device: HashMap::new(),
            port_table,
        }
    }
}

impl EventProcess for CsmaProcess {
    type T = CsmaEvent;

    fn process_event(
        &mut self,
        event: Event<Self::T>,
        event_queue: EventQueueRef<Self::T>,
    ) -> anyhow::Result<()> {
        match event.inner().unwrap() {
            CsmaEvent::NewDevice(src_port) => {
                let device = Rc::new(CsmaDecive::new(
                    self.channel.clone(),
                    self.port_table
                        .get_send_handle_by_id(src_port)
                        .unwrap()
                        .clone(),
                ));
                self.device.insert(src_port, device.clone());
                self.channel.borrow_mut().add_device(src_port, device);
            }
            CsmaEvent::Send((src_port, frame)) => {
                if let Some(device) = self.device.get_mut(&src_port) {
                    device.send(frame, event_queue);
                }
            }
        };
        Ok(())
    }
}
