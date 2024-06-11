use intrusive_collections::intrusive_adapter;
use intrusive_collections::LinkedList;
use intrusive_collections::LinkedListLink;
use log::trace;
use log::warn;
use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::rc::Rc;
use std::time::SystemTime;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct EventWrap<T> {
    link: LinkedListLink,
    inner: InnerEvent<T>,
}

impl<T> EventWrap<T> {
    pub fn new_func(ts: SystemTime, func: Box<dyn FnOnce()>) -> Self {
        Self {
            link: LinkedListLink::new(),
            inner: InnerEvent::new_func(ts, func),
        }
    }

    pub fn ts(&self) -> &SystemTime {
        self.inner.ts()
    }
}

pub enum InnerEvent<T> {
    External(Event<T>),
    Func((SystemTime, Box<dyn FnOnce()>)),
}

impl<T> InnerEvent<T> {
    pub fn new_func(ts: SystemTime, func: Box<dyn FnOnce()>) -> Self {
        Self::Func((ts, func))
    }

    pub fn ts(&self) -> &SystemTime {
        match self {
            Self::External(event) => event.ts(),
            Self::Func((ts, _)) => ts,
        }
    }
}

pub struct Event<T> {
    ts: SystemTime,
    inner: Option<T>,
}

impl<T: Debug + Send> Debug for Event<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event")
            .field("ts", &self.ts)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T> Event<T> {
    pub fn new(ts: SystemTime, inner: T) -> Self {
        Self {
            ts,
            inner: Some(inner),
        }
    }

    pub fn new_null(ts: SystemTime) -> Self {
        Self { ts, inner: None }
    }

    pub fn inner(self) -> Option<T> {
        self.inner
    }

    pub fn ts(&self) -> &SystemTime {
        &self.ts
    }

    pub fn is_null(&self) -> bool {
        self.inner.is_none()
    }
}

impl<T> From<Event<T>> for EventWrap<T> {
    fn from(event: Event<T>) -> Self {
        Self {
            link: LinkedListLink::new(),
            inner: InnerEvent::External(event),
        }
    }
}

intrusive_adapter!(EventAdpator<T> = Box<EventWrap<T>>: EventWrap<T> { link: LinkedListLink });

pub type EventQueueRef<T> = Rc<RefCell<EventQueue<T>>>;

pub struct EventQueue<T> {
    queue: LinkedList<EventAdpator<T>>,
    time: SystemTime,
    enqueue_log: Vec<SystemTime>,
    len: usize,
}

impl<T> EventQueue<T> {
    pub fn new() -> Self {
        Self {
            queue: LinkedList::new(EventAdpator::new()),
            time: SystemTime::UNIX_EPOCH,
            enqueue_log: Vec::new(),
            len: 0,
        }
    }

    pub fn enqueue_event(&mut self, event: Box<EventWrap<T>>) -> usize {
        self.len += 1;
        let enqueue_from_front = event.ts()
            <= &self
                .queue
                .front()
                .get()
                .map(|event| *event.ts())
                .unwrap_or(SystemTime::UNIX_EPOCH);

        if enqueue_from_front {
            self.enqueue_event_from_front(event)
        } else {
            self.enqueue_event_from_back(event)
        }
    }

    pub fn enqueue_event_from_front(&mut self, event: Box<EventWrap<T>>) -> usize {
        let mut reverse_cnt = 0;
        let mut last_cursor = self.queue.front_mut();
        while let Some(element) = last_cursor.get() {
            if element.ts() <= event.ts() {
                reverse_cnt += 1;
                last_cursor.move_next();
            } else {
                break;
            }
        }
        last_cursor.insert_before(event);
        self.len - reverse_cnt
    }

    pub fn enqueue_event_from_back(&mut self, event: Box<EventWrap<T>>) -> usize {
        let mut reverse_cnt = 0;
        let mut last_cursor = self.queue.back_mut();
        while let Some(element) = last_cursor.get() {
            if element.ts() > event.ts() {
                reverse_cnt += 1;
                last_cursor.move_prev();
            } else {
                break;
            }
        }
        last_cursor.insert_after(event);
        reverse_cnt
    }

    pub fn dequeue_event(
        &mut self,
        check: impl Fn(&EventWrap<T>) -> bool,
    ) -> Option<Box<EventWrap<T>>> {
        let mut cursor = self.queue.front_mut();
        while let Some(event) = cursor.get() {
            if check(event) {
                self.time = event.ts().clone();
                self.len -= 1;
                return cursor.remove();
            } else {
                return None;
            }
        }
        None
    }

    pub fn current_time(&self) -> SystemTime {
        self.time.clone()
    }

    pub fn is_empy(&self) -> bool {
        self.queue.is_empty()
    }
}

pub trait EventProcess {
    type T;
    fn process_event(
        &mut self,
        event: Event<Self::T>,
        event_queue: EventQueueRef<Self::T>,
    ) -> anyhow::Result<()>;
}

pub struct EventReceiver<T> {
    receiver: UnboundedReceiver<Event<T>>,
    next_least: SystemTime,
}

impl<T> EventReceiver<T> {
    pub fn new(receiver: UnboundedReceiver<Event<T>>) -> Self {
        Self {
            receiver,
            next_least: SystemTime::UNIX_EPOCH,
        }
    }
}

impl<T> EventReceiver<T> {
    fn try_recv(&mut self) -> anyhow::Result<Option<Event<T>>> {
        match self.receiver.try_recv() {
            Ok(event) => {
                if event.ts < self.next_least {
                    warn!("Sync mode, event ts < next least ts. Drop it.");
                    return Ok(None);
                }
                self.next_least = event.ts;
                if event.is_null() {
                    Ok(None)
                } else {
                    Ok(Some(event))
                }
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(err) => Err(anyhow::anyhow!("Receive error: {:?}", err)),
        }
    }

    fn next_least_ts(&self) -> SystemTime {
        self.next_least
    }
}

struct SourceReceiver<T> {
    receivers: Vec<EventReceiver<T>>,
    // Used to safety check
    last_min_next_least: SystemTime,
}

impl<T> SourceReceiver<T> {
    pub fn new() -> Self {
        Self {
            receivers: Vec::new(),
            last_min_next_least: SystemTime::UNIX_EPOCH,
        }
    }

    pub fn add_receiver(&mut self, mut receiver: EventReceiver<T>) {
        if receiver.next_least < self.last_min_next_least {
            warn!("This receiver has the event less than the last min next least. To preserve the order, make this receiver enter sync mode.");
            receiver.next_least = self.last_min_next_least;
        }
        self.receivers.push(receiver);
    }

    // Return
    // next least time for the next receive
    // min time for the received event
    pub fn try_receive(
        &mut self,
        mut receive_handler: impl FnMut(usize, Box<EventWrap<T>>),
    ) -> anyhow::Result<(SystemTime, Option<SystemTime>)> {
        let mut min_event_ts = None;
        let mut min_next_least: Option<SystemTime> = None;
        let mut violate_cnt = vec![0;self.receivers.len()];
        for (id, receiver) in self.receivers.iter_mut().enumerate() {
            while let Some(event) = receiver.try_recv()? {
                assert!(event.ts >= receiver.next_least);
                assert!(!event.is_null());
                if let Some(min_ts) = min_event_ts {
                    if event.ts < min_ts {
                        min_event_ts = Some(event.ts);
                    }
                } else {
                    min_event_ts = Some(event.ts);
                }
                if event.ts > self.last_min_next_least {
                    violate_cnt[id] += 1;
                }
                receive_handler(id, Box::new(EventWrap::from(event)));
            }
            min_next_least = Some(
                min_next_least
                    .map(|min_next_least| min_next_least.min(receiver.next_least_ts()))
                    .unwrap_or(receiver.next_least_ts()),
            );
        }
        if violate_cnt.iter().any(|cnt|*cnt>0) {
            warn!(target:"violate","{:?}", violate_cnt);
        }
        self.last_min_next_least = min_next_least.unwrap_or(self.last_min_next_least);
        // Guranatee the event for next receive always greater than this time.
        // It's for source task(they generate the event in realtime so we can make this assumption)
        Ok((self.last_min_next_least, min_event_ts))
    }
}

pub struct EventSimulator<T: EventProcess> {
    control_receiver: UnboundedReceiver<EventReceiver<T::T>>,
    source_receiver: SourceReceiver<T::T>,
    event_queue: Rc<RefCell<EventQueue<T::T>>>,
    event_process: T,
}

impl<T: EventProcess> EventSimulator<T> {
    pub fn new(event_process: T, control_receiver: UnboundedReceiver<EventReceiver<T::T>>) -> Self {
        Self {
            control_receiver,
            source_receiver: SourceReceiver::new(),
            event_queue: Rc::new(RefCell::new(EventQueue::new())),
            event_process,
        }
    }

    fn receive(&mut self) -> anyhow::Result<SystemTime> {
        let (next_least, _min_event_ts) = self.source_receiver.try_receive(|id, event| {
            let ts = event.ts().clone();
            let reverse_cnt = self.event_queue.borrow_mut().enqueue_event(event);
            if reverse_cnt > 0 {
                warn!("{} {:?} Reverse count: {} ", id, ts, reverse_cnt);
            }
        })?;
        Ok(next_least)
    }

    #[inline]
    fn run_once(&mut self) -> anyhow::Result<()> {
        // Control
        loop {
            let control_receiver = self.control_receiver.try_recv();
            if let Ok(control_receiver) = control_receiver {
                self.source_receiver.add_receiver(control_receiver);
            } else if let Err(TryRecvError::Empty) = control_receiver {
                break;
            } else {
                return Err(anyhow::anyhow!("Control receiver closed"));
            }
        }

        // New Source Event
        let next_least = self.receive()?;

        // Process
        loop {
            let event = self
                .event_queue
                .borrow_mut()
                .dequeue_event(|event| event.ts() <= &next_least);
            if let Some(event) = event {
                match event.inner {
                    InnerEvent::External(event) => self
                        .event_process
                        .process_event(event, self.event_queue.clone())?,
                    InnerEvent::Func(func) => func.1(),
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        let mut now = SystemTime::now();
        loop {
            if now.elapsed().unwrap().as_secs() > 11 {
                trace!(
                    target: "eventlog",
                    "{}",
                    std::mem::take(&mut self.event_queue.borrow_mut().enqueue_log)
                        .into_iter()
                        .map(|time| format!("{:?}", time))
                        .collect::<Vec<_>>()
                        .join("\n")
                );
                now = SystemTime::now();
            }
            self.run_once()?;
        }
    }
}
