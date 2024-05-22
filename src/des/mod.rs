// A discret event simulator support
// 1. automactic event scheduling
// 2. optimized event scheduling

use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, SystemTime};

use intrusive_collections::intrusive_adapter;
use intrusive_collections::LinkedList;
use intrusive_collections::LinkedListLink;
use log::warn;
use std::fmt::{self, Debug};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct EventWrap<T> {
    link: LinkedListLink,

    // Cancel used in optimistic event scheduling.
    cancel: Rc<Cell<bool>>,
    child_cancel: Vec<Rc<Cell<bool>>>,

    event: Event<T>,
}

pub struct Event<T> {
    ts: SystemTime,
    inner: T,
    is_null: bool,
}

impl<T: Debug> Debug for Event<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event")
            .field("ts", &self.ts)
            .field("inner", &self.inner)
            .field("is_null", &self.is_null)
            .finish()
    }
}

impl<T> Event<T> {
    pub fn new(ts: SystemTime, inner: T) -> Self {
        Self {
            ts,
            inner,
            is_null: false,
        }
    }

    // null event used to update the least time, it will not be processed.
    pub fn new_null(ts: SystemTime, inner: T) -> Self {
        Self {
            ts,
            inner,
            is_null: true,
        }
    }
}

impl<T> From<Event<T>> for EventWrap<T> {
    fn from(event: Event<T>) -> Self {
        Self {
            link: LinkedListLink::new(),
            cancel: Rc::new(Cell::new(false)),
            child_cancel: Vec::new(),
            event,
        }
    }
}

impl<T> EventWrap<T> {
    #[inline]
    pub fn inner_ref(&self) -> &T {
        &self.event.inner
    }

    #[inline]
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.event.inner
    }

    #[inline]
    pub fn ts(&self) -> &SystemTime {
        &self.event.ts
    }
}

intrusive_adapter!(EventAdpator<T> = Box<EventWrap<T>>: EventWrap<T> { link: LinkedListLink });

pub struct EventQueue<T> {
    queue: LinkedList<EventAdpator<T>>,
}

impl<T> EventQueue<T> {
    pub fn new() -> Self {
        Self {
            queue: LinkedList::new(EventAdpator::new()),
        }
    }

    pub fn enqueu_event(&mut self, event: Box<EventWrap<T>>) {
        let mut last_cursor = self.queue.back_mut();
        while let Some(element) = last_cursor.get() {
            if element.ts() >= event.ts() {
                last_cursor.move_prev();
            } else {
                break;
            }
        }
        last_cursor.insert_after(event);
    }

    // dequeue the first event statisfied the check
    pub fn dequeue_event(
        &mut self,
        check: impl Fn(&EventWrap<T>) -> bool,
    ) -> Option<Box<EventWrap<T>>> {
        let mut cursor = self.queue.front_mut();
        while let Some(event) = cursor.get() {
            if event.cancel.get() {
                cursor.remove();
                continue;
            }
            if check(event) {
                return cursor.remove();
            } else {
                return None;
            }
        }
        None
    }

    pub fn is_empy(&self) -> bool {
        self.queue.is_empty()
    }
}

pub trait EventProcess {
    type T;
    fn process_event(
        &mut self,
        event: &mut EventWrap<Self::T>,
        event_queue: &mut EventQueue<Self::T>,
        is_optmiistic: bool,
    ) -> anyhow::Result<()>;
    fn redoable(&self, event: &EventWrap<Self::T>) -> bool;
    fn undo_state(&mut self, event: &EventWrap<Self::T>) -> anyhow::Result<()>;
    fn commit_state(&mut self, event: &EventWrap<Self::T>) -> anyhow::Result<()>;
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
                // Sync mode
                if event.ts < self.next_least {
                    warn!("Sync mode, event ts < next least ts. Drop it.");
                    return Ok(None);
                }
                self.next_least = event.ts;
                if event.is_null {
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
        mut receive_handler: impl FnMut(Box<EventWrap<T>>),
    ) -> anyhow::Result<(SystemTime, Option<SystemTime>)> {
        let mut min_event_ts = None;
        let mut min_next_least: Option<SystemTime> = None;
        for receiver in self.receivers.iter_mut() {
            while let Some(event) = receiver.try_recv()? {
                assert!(event.ts >= receiver.next_least);
                assert!(!event.is_null);
                if let Some(min_ts) = min_event_ts {
                    if event.ts < min_ts {
                        min_event_ts = Some(event.ts);
                    }
                } else {
                    min_event_ts = Some(event.ts);
                }
                receive_handler(Box::new(EventWrap::from(event)));
            }
            min_next_least = Some(
                min_next_least
                    .map(|min_next_least| min_next_least.min(receiver.next_least_ts()))
                    .unwrap_or(receiver.next_least_ts()),
            );
        }
        self.last_min_next_least = min_next_least.unwrap_or(self.last_min_next_least);
        // Guranatee the event for next receive always greater than this time.
        // It's for source task(they generate the event in realtime so we can make this assumption)
        Ok((self.last_min_next_least, min_event_ts))
    }
}

struct UnsafeWindow<T> {
    events: LinkedList<EventAdpator<T>>,
}

impl<T> UnsafeWindow<T> {
    pub fn new() -> Self {
        Self {
            events: LinkedList::new(EventAdpator::new()),
        }
    }

    pub fn insert(&mut self, event: Box<EventWrap<T>>) {
        // safety check, ts of new event must be greater than the last event
        let last_event = self.events.back();
        assert!({
            if let Some(last_event) = last_event.get() {
                last_event.ts() <= event.ts()
            } else {
                true
            }
        });

        self.events.push_back(event);
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
    pub fn notify_new_event(
        &mut self,
        ts: SystemTime,
        event_queue: &mut EventQueue<T>,
        process: &mut impl EventProcess<T = T>,
    ) -> anyhow::Result<()> {
        let mut cursor = self.events.back_mut();
        while let Some(unsafe_event) = cursor.get()
            && unsafe_event.ts() >= &ts
        {
            cursor.move_prev();
        }

        cursor.move_next();
        while let Some(mut event) = cursor.remove() {
            let is_source_event = !event.cancel.get();

            // cancel child event
            event.cancel.set(true);
            for child_cancel in event.child_cancel.iter() {
                child_cancel.set(true);
            }

            process.undo_state(&event)?;

            // Requeue the source event
            if is_source_event {
                if process.redoable(&event) {
                    event.child_cancel.clear();
                    event.cancel.set(false);
                    event_queue.enqueu_event(event);
                } else {
                    warn!("Event is not redoable, discard it");
                }
            }
        }
        Ok(())
    }

    /// Notify the new least time to the window.
    /// The event in the window wtih ts less than the least time can be removed and commit.
    pub fn notify_new_least(
        &mut self,
        ts: SystemTime,
        process: &mut impl EventProcess<T = T>,
    ) -> anyhow::Result<()> {
        let mut cursor = self.events.front_mut();
        while let Some(unsafe_event) = cursor.get()
            && unsafe_event.ts() <= &ts
        {
            let event = cursor.remove().unwrap();
            process.commit_state(&event)?;
        }
        Ok(())
    }
}

pub struct EventSimulator<T: EventProcess> {
    unsafe_window: UnsafeWindow<T::T>,
    control_receiver: UnboundedReceiver<EventReceiver<T::T>>,
    source_receiver: SourceReceiver<T::T>,
    event_queue: EventQueue<T::T>,
    event_process: T,
    enable_optimistic: Option<u64>,
}

impl<T: EventProcess> EventSimulator<T> {
    pub fn new(
        event_process: T,
        control_receiver: UnboundedReceiver<EventReceiver<T::T>>,
        enable_optimistic: Option<u64>,
    ) -> Self {
        Self {
            unsafe_window: UnsafeWindow::new(),
            control_receiver,
            source_receiver: SourceReceiver::new(),
            event_queue: EventQueue::new(),
            event_process,
            enable_optimistic,
        }
    }

    fn receive(&mut self) -> anyhow::Result<SystemTime> {
        let (next_least, min_event_ts) = self.source_receiver.try_receive(|event| {
            self.event_queue.enqueu_event(event);
        })?;
        // # NOTE
        // This order is matter.
        if let Some(min) = min_event_ts {
            self.unsafe_window.notify_new_event(
                min,
                &mut self.event_queue,
                &mut self.event_process,
            )?;
        }
        self.unsafe_window
            .notify_new_least(next_least, &mut self.event_process)?;
        Ok(next_least)
    }

    #[inline]
    fn run_once_without_occ(&mut self) -> anyhow::Result<()> {
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
        while let Some(mut event) = self
            .event_queue
            .dequeue_event(|event| event.ts() <= &next_least)
        {
            self.event_process
                .process_event(&mut event, &mut self.event_queue, false)?;
        }

        Ok(())
    }

    pub fn run_once(&mut self) -> anyhow::Result<()> {
        self.run_once_without_occ()?;

        // Occ Process
        if let Some(interval_nanosecond) = self.enable_optimistic {
            // Try to receive again first
            let next_least = self.receive()?;

            let unsafe_bound = next_least
                .checked_add(Duration::from_nanos(interval_nanosecond))
                .unwrap();
            while let Some(mut event) = self
                .event_queue
                .dequeue_event(|event| event.ts() <= &unsafe_bound)
            {
                if event.ts() <= &next_least {
                    self.event_process
                        .process_event(&mut event, &mut self.event_queue, false)?;
                } else {
                    self.event_process
                        .process_event(&mut event, &mut self.event_queue, true)?;
                    self.unsafe_window.insert(event);
                }
            }
        }

        Ok(())
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        loop {
            self.run_once()?;
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::SystemTime;

    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    struct TestEventProcess {
        state: Rc<Cell<i32>>,
        uncommit_state: Rc<Cell<i32>>,
        commit_state: Rc<Cell<i32>>,
    }

    impl EventProcess for TestEventProcess {
        type T = i32;

        fn process_event(
            &mut self,
            _event: &mut EventWrap<Self::T>,
            _event_queue: &mut EventQueue<Self::T>,
            is_optmiistic: bool,
        ) -> anyhow::Result<()> {
            self.state.replace(self.state.get() + 1);
            if is_optmiistic {
                self.uncommit_state.replace(self.uncommit_state.get() + 1);
            }
            Ok(())
        }

        fn redoable(&self, _event: &EventWrap<Self::T>) -> bool {
            true
        }

        fn undo_state(&mut self, _event: &EventWrap<Self::T>) -> anyhow::Result<()> {
            self.state.replace(self.state.get() - 1);
            self.uncommit_state.replace(self.uncommit_state.get() - 1);
            Ok(())
        }

        fn commit_state(&mut self, _event: &EventWrap<Self::T>) -> anyhow::Result<()> {
            self.commit_state.replace(self.commit_state.get() + 1);
            self.uncommit_state.replace(self.uncommit_state.get() - 1);
            Ok(())
        }
    }

    #[test]
    fn test_event_simulator_simple_process() {
        let (source_sender, source_receiver) = unbounded_channel();
        let state = Rc::new(Cell::new(0));
        let _commit_state = Rc::new(Cell::new(0));
        let (control_sender, control_receiver) = unbounded_channel();
        let mut simulator = EventSimulator::new(
            TestEventProcess {
                state: state.clone(),
                uncommit_state: Rc::new(Cell::new(0)),
                commit_state: _commit_state.clone(),
            },
            control_receiver,
            None,
        );
        control_sender
            .send(EventReceiver::new(source_receiver))
            .unwrap();

        simulator.run_once().unwrap();
        assert_eq!(state.get(), 0);

        let now = SystemTime::now();
        let event1 = Event::new(now, 1);
        let event2 = Event::new(now, 2);
        let event3 = Event::new(now, 3);
        source_sender.send(event1).unwrap();
        source_sender.send(event2).unwrap();
        source_sender.send(event3).unwrap();

        simulator.run_once().unwrap();
        assert_eq!(state.get(), 3);
    }

    #[test]
    fn test_unsafe_window() {
        let mut window = UnsafeWindow::new();

        let now = SystemTime::now();
        for i in 0..8_i32 {
            let event = Event::new(now.checked_add(Duration::from_nanos(i as u64)).unwrap(), i);
            window.insert(Box::new(EventWrap::from(event)));
        }
        // event 8 -> event 9  -> event 11
        //    |-----> event 10
        let mut event_8 = Box::new(EventWrap::from(Event::new(
            now.checked_add(Duration::from_nanos(8)).unwrap(),
            8,
        )));
        let mut event_9 = Box::new(EventWrap::from(Event::new(
            now.checked_add(Duration::from_nanos(9)).unwrap(),
            9,
        )));
        let event_10 = Box::new(EventWrap::from(Event::new(
            now.checked_add(Duration::from_nanos(10)).unwrap(),
            10,
        )));
        let event_11 = Box::new(EventWrap::from(Event::new(
            now.checked_add(Duration::from_nanos(11)).unwrap(),
            11,
        )));
        event_8.child_cancel.push(event_9.cancel.clone());
        event_8.child_cancel.push(event_10.cancel.clone());
        event_9.child_cancel.push(event_11.cancel.clone());
        window.insert(event_8);
        window.insert(event_9);

        let check = |extra_check: Option<&dyn Fn(&EventWrap<i32>) -> bool>,
                     expect_cnt: usize,
                     init_time: SystemTime,
                     window: &mut UnsafeWindow<i32>| {
            let mut time = init_time;
            let mut cursor = window.events.front_mut();
            let mut cnt = 0;
            while let Some(event) = cursor.get() {
                assert_eq!(event.ts(), &time);
                if let Some(extra_check) = extra_check {
                    assert!(extra_check(event));
                }
                time = time.checked_add(Duration::from_nanos(1)).unwrap();
                cursor.move_next();
                cnt += 1;
            }
            assert_eq!(cnt, expect_cnt);
        };

        // check init
        check(None, 10, now, &mut window);

        // check after notify new least
        let mut process = TestEventProcess {
            state: Rc::new(Cell::new(0)),
            uncommit_state: Rc::new(Cell::new(0)),
            commit_state: Rc::new(Cell::new(0)),
        };
        window
            .notify_new_least(
                now.checked_add(Duration::from_nanos(5)).unwrap(),
                &mut process,
            )
            .unwrap();
        check(
            None,
            4,
            now.checked_add(Duration::from_nanos(6)).unwrap(),
            &mut window,
        );

        // check after notify new event
        let mut event_queue = EventQueue::new();
        window
            .notify_new_event(
                now.checked_add(Duration::from_nanos(8)).unwrap(),
                &mut event_queue,
                &mut process,
            )
            .unwrap();
        check(
            None,
            2,
            now.checked_add(Duration::from_nanos(6)).unwrap(),
            &mut window,
        );
        let event_8 = event_queue.dequeue_event(|_| true).unwrap();
        // check event_8 has been reset
        assert!(!event_8.cancel.get());
        assert!(event_8.child_cancel.is_empty());
        assert_eq!(
            event_8.ts(),
            &now.checked_add(Duration::from_nanos(8)).unwrap()
        );
        assert_eq!(event_8.inner_ref(), &8);
        // check only event_8 has been requeue
        assert!(event_queue.is_empy());
        // check event 10 and event 11 has been canceled
        assert!(event_10.cancel.get());
        assert!(event_11.cancel.get());
    }

    #[test]
    fn test_event_simulator_optimistic_process() {
        let (source_sender, source_receiver) = unbounded_channel();
        let state = Rc::new(Cell::new(0));
        let commit_state = Rc::new(Cell::new(0));
        let uncommit_state = Rc::new(Cell::new(0));
        let (control_sender, control_receiver) = unbounded_channel();
        let mut simulator = EventSimulator::new(
            TestEventProcess {
                state: state.clone(),
                uncommit_state: uncommit_state.clone(),
                commit_state: commit_state.clone(),
            },
            control_receiver,
            Some(9000_000_000),
        );
        control_sender
            .send(EventReceiver::new(source_receiver))
            .unwrap();
        let (source_sender2, source_receiver2) = unbounded_channel();
        control_sender
            .send(EventReceiver::new(source_receiver2))
            .unwrap();

        // test optimistic process in advance 1s
        // ................. e1 e2 e3
        // now ------------ now + 1s
        let now = SystemTime::now();
        let event1 = Event::new(now.checked_add(Duration::from_secs(1)).unwrap(), 1);
        let event2 = Event::new(now.checked_add(Duration::from_secs(1)).unwrap(), 2);
        let event3 = Event::new(now.checked_add(Duration::from_secs(1)).unwrap(), 3);
        source_sender2.send(Event::new_null(now, 1)).unwrap();
        source_sender.send(event1).unwrap();
        source_sender.send(event2).unwrap();
        source_sender.send(event3).unwrap();

        simulator.run_once().unwrap();

        assert_eq!(state.get(), 3);
        assert_eq!(uncommit_state.get(), 3);

        // wait to commit the result
        source_sender2
            .send(Event::new_null(
                now.checked_add(Duration::from_secs(1)).unwrap(),
                1,
            ))
            .unwrap();
        simulator.run_once().unwrap();
        assert_eq!(commit_state.get(), 3);
        assert_eq!(uncommit_state.get(), 0);

        // test more complex case
        // .................    e1    ................  e3,e4
        // now ------------  now + 1s -------------- now + 3s
        // .................................e2 ............
        // now -------------------------- now + 2s ----------
        let now = SystemTime::now();
        let event1 = Event::new(now.checked_add(Duration::from_secs(1)).unwrap(), 4);
        let event2 = Event::new(now.checked_add(Duration::from_secs(2)).unwrap(), 5);
        let event3 = Event::new(now.checked_add(Duration::from_secs(3)).unwrap(), 6);
        let event4 = Event::new(now.checked_add(Duration::from_secs(3)).unwrap(), 7);
        source_sender2.send(Event::new_null(now, 1)).unwrap();
        source_sender.send(event1).unwrap();
        source_sender.send(event3).unwrap();
        source_sender.send(event4).unwrap();

        simulator.run_once().unwrap();
        assert_eq!(commit_state.get(), 3);
        assert_eq!(state.get(), 6);
        simulator.run_once_without_occ().unwrap();
        assert_eq!(state.get(), 6);

        source_sender2.send(event2).unwrap();
        simulator.run_once_without_occ().unwrap();
        // 4 unsafe process + 1 normal process
        assert_eq!(state.get(), 5);
        assert_eq!(commit_state.get(), 4);

        simulator.run_once().unwrap();
        assert_eq!(state.get(), 7);
    }
}
