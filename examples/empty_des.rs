use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    fmt::Debug,
    sync::{atomic::AtomicBool, Arc},
    task::Poll,
    time::SystemTime,
};

use anyhow::anyhow;
use async_xdp::Frame;
use futures::{Stream, StreamExt};
use log::{error, warn};
use netem_rs::{Actor, ActorContext, DataView, LocalRunTime, PortTable};
use packet::ether::Packet;
use smallvec::smallvec;
use tokio::{select, sync::Mutex, task};
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Debug)]
enum SourceEvent {
    Packet(PacketEvent),
    Tick(SystemTime),
}

struct PacketEvent {
    frame: Frame,
    ts: SystemTime,
}

impl Debug for PacketEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketEvent").field("ts", &self.ts).finish()
    }
}

impl PacketEvent {
    pub fn new(frame: Frame, ts: SystemTime) -> Self {
        Self { frame, ts }
    }
}

impl PartialEq for PacketEvent {
    fn eq(&self, other: &Self) -> bool {
        self.ts == other.ts
    }
}

impl Eq for PacketEvent {}

impl PartialOrd for PacketEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.ts.partial_cmp(&other.ts)
    }
}

impl Ord for PacketEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ts.cmp(&other.ts)
    }
}

struct LeastTimeTracer {
    last_input_ts: HashMap<usize, SystemTime>,
}

impl LeastTimeTracer {
    pub fn update(&mut self, input_id: usize, ts: SystemTime) {
        self.last_input_ts.get_mut(&input_id).map(|v| *v = ts);
    }

    pub fn insert(&mut self, input_id: usize, ts: SystemTime) {
        self.last_input_ts.insert(input_id, ts);
    }

    pub fn get_least_time(&self) -> SystemTime {
        *self
            .last_input_ts
            .values()
            .min()
            .unwrap_or(&SystemTime::UNIX_EPOCH)
    }
}

pub struct UnsafeEvent {
    event: Option<Event>,
    event_meta: EventMetaRef,
    unsafe_event_handle: UnsafeEventHandleRef,
}

impl UnsafeEvent {
    pub fn new(event: Event) -> Self {
        Self {
            event_meta: event.meta.clone(),
            event: Some(event),
            unsafe_event_handle: Arc::new(UnsafeEventHandle {
                child_event: Mutex::new(Vec::new()),
            }),
        }
    }

    /// There are two kinds of unsafe event process:
    /// 1. Undoable operation. The key point of these operations is that don't need to take the ownership of the event.
    /// 2. Non-undoable operation. The key point of these operations is that need to take the ownership of the event. E.g send out the packet.
    /// For the non-undoable operation, we will only warn when the conflict happens, we can't do anything to fix them.
    pub fn take_out_event(&mut self) -> Event {
        self.event.take().unwrap()
    }

    pub async fn is_cancel(&self) -> bool {
        self.event_meta
            .cancel
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn mark_cancel(&self) {
        // This event has been canceled by parent event.
        if self
            .event_meta
            .cancel
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        self.event_meta.mark_cancel();
        for child in self.unsafe_event_handle.child_event.lock().await.iter() {
            child.mark_cancel();
        }
    }
}

pub type UnsafeEventHandleRef = Arc<UnsafeEventHandle>;

struct UnsafeEventHandle {
    child_event: Mutex<Vec<EventMetaRef>>,
}

impl UnsafeEventHandle {
    pub async fn add_child_event(&self, event: EventMetaRef) {
        self.child_event.lock().await.push(event);
    }
}

struct UnsafeWindow {
    // guarantee the order of event
    window: Vec<UnsafeEvent>,
}

impl UnsafeWindow {
    pub fn new() -> Self {
        Self { window: Vec::new() }
    }

    pub fn insert(&mut self, event: UnsafeEvent) {
        // safety check, ts of new event must be greater than the last event
        if let Some(last) = self.window.last() {
            assert!(event.event_meta.ts > last.event_meta.ts);
        }

        self.window.push(event);
    }

    /// For new event comming, we should detect whether there are events with ts greater than the new event have been process.
    /// If so, we should:
    /// 1. mark the event and all its child events as canceled to stop process them.
    /// 2. undo the event in unsafe window. The event in unsafe windows means that they have been processed by the queue.
    /// 3. For the source event, we should remove them and requeue them to the queue.
    /// E.g.
    /// |   unsafe window  |  queue |
    /// |    (e1)   (e2)   |  (e3)  |
    /// |      ------⬆---------⬆   |
    /// When we detect event1 with the greater ts, we should mark e1, e2(child of e1), e3(child of e2) as canceled.
    /// Then we should undo e1, e2.
    /// e1 is the source event, so we requeue it to the queue.
    pub async fn notify_new_event(&mut self, ts: SystemTime, event_queue: &mut EventQueue) {
        let Some(start_pos) = self
            .window
            .iter()
            .position(|unsafe_event| unsafe_event.event_meta.ts >= ts)
        else {
            return;
        };
        for unsafe_event in self.window.drain(start_pos..) {
            if unsafe_event.event_meta.ts > ts {
                let is_source_event = !unsafe_event.is_cancel().await;
                unsafe_event.mark_cancel().await;
                if is_source_event {
                    if let Some(event) = unsafe_event.event {
                        event_queue.enqueu_event(event);
                    } else {
                        warn!(
                            "Failed to fix non-redoable event: {:?}",
                            unsafe_event.event_meta
                        );
                    }
                }
                // # TODO:
                // undo
            }
        }
    }

    /// Notify the new least time to the window.
    /// The event in the window wtih ts less than the least time can be removed and commit.
    pub fn notify_new_least(&mut self, ts: SystemTime) {
        let Some(end_pos) = self
            .window
            .iter()
            .position(|unsafe_event| unsafe_event.event_meta.ts > ts)
        else {
            return;
        };
        for event_handle in self.window.drain(..end_pos) {
            // # TODO
            // commit
        }
    }
}

type EventMetaRef = Arc<EventMeta>;

struct EventMeta {
    ts: SystemTime,
    cancel: AtomicBool,
    unsafe_event_handle: Option<UnsafeEventHandleRef>,
}

impl Debug for EventMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventMeta")
            .field("ts", &self.ts)
            .field("cancel", &self.cancel)
            .finish()
    }
}

impl EventMeta {
    fn mark_cancel(&self) {
        self.cancel
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

struct Event {
    meta: EventMetaRef,
    inner: EventInner,
}

impl Event {
    pub fn packet_event(packet_event: PacketEvent) -> Self {
        let meta = Arc::new(EventMeta {
            ts: packet_event.ts,
            cancel: AtomicBool::new(false),
            unsafe_event_handle: None,
        });
        Self {
            meta,
            inner: EventInner::Packet(packet_event),
        }
    }

    pub fn meta(&self) -> &EventMetaRef {
        &self.meta
    }
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.meta.ts == other.meta.ts
    }
}

impl Eq for Event {}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.meta.ts.partial_cmp(&other.meta.ts)
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.meta.ts.cmp(&other.meta.ts)
    }
}

enum EventInner {
    Packet(PacketEvent),
}

struct EventQueue {
    queue: BinaryHeap<Reverse<Event>>,
}

impl EventQueue {
    pub fn new() -> Self {
        Self {
            queue: BinaryHeap::new(),
        }
    }

    pub fn enqueu_event(&mut self, event: Event) {
        self.queue.push(Reverse(event));
    }

    fn discard_cancel_event(&mut self) {
        while let Some(event) = self.queue.peek() {
            if event
                .0
                .meta
                .cancel
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                self.queue.pop();
            } else {
                break;
            }
        }
    }

    pub fn pop_event(&mut self) -> Option<Event> {
        self.discard_cancel_event();
        self.queue.pop().map(|event| event.0)
    }

    pub fn peek_event(&mut self) -> Option<&Event> {
        self.discard_cancel_event();
        self.queue.peek().map(|event| &event.0)
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }
}

type EventStream = Box<dyn Stream<Item = SourceEvent> + Unpin + Send + Sync>;

struct CombineStream {
    streams: Vec<EventStream>,
    last_poll: usize,
}

impl CombineStream {
    pub fn new() -> Self {
        Self {
            streams: Vec::new(),
            last_poll: 0,
        }
    }

    pub fn add_stream(&mut self, stream: EventStream) -> usize {
        self.streams.push(stream);
        self.streams.len() - 1
    }

    pub fn len(&self) -> usize {
        self.streams.len()
    }
}

impl Stream for CombineStream {
    type Item = (u32, SourceEvent);

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.streams.is_empty() {
            return Poll::Pending;
        }
        for (id, stream) in self.streams.iter_mut().enumerate() {
            if let Poll::Ready(event) = stream.poll_next_unpin(cx) {
                if let Some(event) = event {
                    return Poll::Ready(Some((id as u32, event)));
                } else {
                    panic!("Stream {} closed", id);
                }
            }
        }
        Poll::Pending
    }
}

struct EmptyChannel {
    control_rx: tokio::sync::mpsc::UnboundedReceiver<EventStream>,
    inputs: CombineStream,
    least_time_tracer: LeastTimeTracer,
    event_list: EventQueue,
    unsafe_window: UnsafeWindow,
    port_table: PortTable,
}

impl EmptyChannel {
    async fn process_unsafe_packet(event: &mut UnsafeEvent, port_table: &mut PortTable) -> anyhow::Result<()> {
        let EventInner::Packet(event) = event.take_out_event().inner;
        Self::process_packet(event, port_table).await.unwrap();
        Ok(())
    }

    async fn process_packet(event: PacketEvent, port_table: &mut PortTable) -> anyhow::Result<()> {
        // Redirect this packet
        let packet = Packet::new(event.frame.data_ref()).unwrap();
        let src_port_id = port_table.get_port_id(packet.source()).await.unwrap();
        if packet.destination().is_broadcast() {
            port_table
                .for_each_port(|&port_id, send_handle| {
                    if port_id != src_port_id {
                        send_handle.send_raw_data(event.frame.data_ref().to_vec())
                    } else {
                        Ok(())
                    }
                })
                .await?;
        } else {
            if let Some(handle) = port_table.get_send_handle(packet.destination()).await {
                handle.send_frame(smallvec![event.frame])?;
            } else {
                error!("No such port {}", packet.destination());
            }
        }
        Ok(())
    }

    async fn process_event(
        event_list: &mut EventQueue,
        least_time: SystemTime,
        port_table: &mut PortTable,
        unsafe_window: &mut UnsafeWindow,
    ) -> anyhow::Result<()> {
        if least_time == SystemTime::UNIX_EPOCH {
            return Ok(());
        }
        if let Some(event) = event_list.peek_event() {
            if event.meta.ts < least_time {
                let EventInner::Packet(event) = event_list.pop_event().unwrap().inner;
                Self::process_packet(event, port_table).await?;
            } else {
                // optimize process
                let mut event = UnsafeEvent::new(event_list.pop_event().unwrap());
                Self::process_unsafe_packet(&mut event, port_table).await?;
                unsafe_window.insert(event);
            }
        }
        return Ok(());
    }

    async fn run(self) -> anyhow::Result<()> {
        let EmptyChannel {
            mut control_rx,
            mut inputs,
            mut least_time_tracer,
            mut event_list,
            mut port_table,
            mut unsafe_window,
        } = self;
        loop {
            select! {
                // biased;
                control_msg = control_rx.recv() => {
                    if let Some(input_stream) = control_msg {
                        let id = inputs.add_stream(input_stream);
                        least_time_tracer.insert(id,SystemTime::now());
                    } else {
                        return Err(anyhow!("Control channel closed"));
                    }
                }
                event = inputs.next() => {
                    if let Some((id,event)) = event {
                        match event {
                            SourceEvent::Tick(ts) => {
                                least_time_tracer.update(id as usize,ts);
                                unsafe_window.notify_new_least(ts);
                            },
                            SourceEvent::Packet(event) => {
                                least_time_tracer.update(id as usize,event.ts);
                                unsafe_window.notify_new_event(event.ts,&mut event_list).await;
                                event_list.enqueu_event(Event::packet_event(event));
                            }
                        }
                    }
                }
                _ = Self::process_event(&mut event_list,least_time_tracer.get_least_time(),&mut port_table, &mut unsafe_window) => {}
            }
        }
    }
}

#[derive(Clone)]
struct EmptyDataView {
    control_tx: tokio::sync::mpsc::UnboundedSender<EventStream>,
}

impl DataView for EmptyDataView {
    fn new_wtih_port_table(port_table: PortTable) -> Self {
        let (control_tx, control_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(task::unconstrained(
            EmptyChannel {
                control_rx,
                inputs: CombineStream::new(),
                least_time_tracer: LeastTimeTracer {
                    last_input_ts: HashMap::new(),
                },
                event_list: EventQueue::new(),
                port_table,
                unsafe_window: UnsafeWindow::new(),
            }
            .run(),
        ));
        Self { control_tx }
    }
}

struct ForwardActor {
    context: ActorContext<EmptyDataView>,
    tx: tokio::sync::mpsc::UnboundedSender<SourceEvent>,
}

impl Actor for ForwardActor {
    type C = EmptyDataView;

    fn new(context: ActorContext<Self::C>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        context
            .data_view
            .control_tx
            .send(Box::new(UnboundedReceiverStream::new(rx)))
            .unwrap();
        Self { context, tx }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut has_send = false;
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(1));
        loop {
            select! {
                biased;
                frames = self.context.receive_handle.receive_frames() => {
                    for frame in frames? {
                        let now = std::time::SystemTime::now();
                        self.tx.send(SourceEvent::Packet(PacketEvent::new(frame, now))).unwrap();
                    }
                    has_send = true;
                },
                _ = interval.tick() => {
                    let now = std::time::SystemTime::now();
                    if !has_send {
                        self.tx.send(SourceEvent::Tick(now)).unwrap();
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

#[cfg(test)]
mod test {
    use std::thread::spawn;

    use async_xdp::Frame;

    #[test]
    fn test_least_time_tracer() {
        use super::LeastTimeTracer;
        use std::time::SystemTime;

        let mut tracer = LeastTimeTracer {
            last_input_ts: Default::default(),
        };

        assert_eq!(tracer.get_least_time(), SystemTime::UNIX_EPOCH);

        tracer.insert(0, SystemTime::UNIX_EPOCH);
        assert_eq!(tracer.get_least_time(), SystemTime::UNIX_EPOCH);

        tracer.insert(1, SystemTime::UNIX_EPOCH);
        assert_eq!(tracer.get_least_time(), SystemTime::UNIX_EPOCH);

        let time1 = SystemTime::now();
        tracer.update(0, time1);
        assert_eq!(tracer.get_least_time(), SystemTime::UNIX_EPOCH);

        let time2 = SystemTime::now();
        tracer.update(1, time2);
        assert_eq!(tracer.get_least_time(), time1);

        tracer.update(0, SystemTime::now());
        assert_eq!(tracer.get_least_time(), time2);

        tracer.insert(3, SystemTime::UNIX_EPOCH);
        assert_eq!(tracer.get_least_time(), SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn test_event_list() {
        use std::time::SystemTime;

        let mut event_list = std::collections::BinaryHeap::new();
        let time1 = SystemTime::now();
        let time2 = SystemTime::now();

        event_list.push(std::cmp::Reverse(time1));
        event_list.push(std::cmp::Reverse(time2));

        assert_eq!(event_list.peek().unwrap().0, time1);
        assert_eq!(event_list.pop().unwrap().0, time1);
    }
}
