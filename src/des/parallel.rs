use std::{cell::RefCell, cmp::max, rc::Rc, time::SystemTime};

use super::{Event, EventQueue, EventReceiver, EventWrap, SourceReceiver};
use std::fmt::Debug;
use tokio::sync::mpsc::{error::TryRecvError, UnboundedReceiver, UnboundedSender};

pub trait EventProperty {
    fn is_read_only(&self) -> bool;
}

pub trait EventReadOnlyProcess: Clone + Send + 'static {
    type T: EventProperty + Send + 'static;
    type S: Debug + Clone + Send + 'static;
    fn process_event(
        &mut self,
        event: &mut Event<Self::T>,
        result: &mut Vec<Event<Self::T>>,
        state: &Self::S,
    ) -> anyhow::Result<()>;
}

pub trait EventReadWriteProcess {
    type T: EventProperty + Send + 'static;
    type S: Clone + Send + 'static;
    type ROP: EventReadOnlyProcess<T = Self::T, S = Self::S>;
    fn process_event(
        &mut self,
        event: &mut EventWrap<Self::T>,
        event_queue: &mut EventQueue<Self::T>,
    ) -> anyhow::Result<()>;
    fn process_read_only_event_batch(
        &mut self,
        event: &mut [EventWrap<Self::T>],
        event_queue: &mut EventQueue<Self::T>,
        state: &Self::S,
    ) -> anyhow::Result<()>;
    fn clone_current_state(&self) -> Self::S;
    fn clone_read_only_process(&self) -> Self::ROP;
}

struct EventWorker;

impl EventWorker {
    async fn run<T: EventReadOnlyProcess>(
        mut control_receiver: UnboundedReceiver<T::S>,
        result_sender: UnboundedSender<Vec<Event<T::T>>>,
        mut event_receiver: UnboundedReceiver<Vec<Event<T::T>>>,
        mut event_process: T,
    ) -> Self {
        loop {
            let state = control_receiver.recv().await.unwrap();
            let mut result = Vec::new();
            let mut event = event_receiver.recv().await.unwrap();
            for event in event.iter_mut() {
                event_process
                    .process_event(event, &mut result, &state)
                    .unwrap();
            }
            result_sender.send(result).unwrap();
        }
    }
}

pub struct EventSimulator<T: EventReadWriteProcess> {
    control_receiver: UnboundedReceiver<EventReceiver<T::T>>,
    source_receiver: SourceReceiver<T::T>,

    event_queue: EventQueue<T::T>,
    event_process: T,

    event_worker: Vec<(
        UnboundedSender<T::S>,
        UnboundedReceiver<Vec<Event<T::T>>>,
        UnboundedSender<Vec<Event<T::T>>>,
    )>,

    runtime: tokio::runtime::Runtime,
}

impl<T: EventReadWriteProcess> EventSimulator<T> {
    pub fn new(
        event_process: T,
        control_receiver: UnboundedReceiver<EventReceiver<T::T>>,
        parallel: usize,
    ) -> Self {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        Self {
            control_receiver,
            source_receiver: SourceReceiver::new(),
            event_queue: EventQueue::new(),
            event_worker: (0..parallel)
                .map(|_| {
                    let (control_sender, control_receiver) = tokio::sync::mpsc::unbounded_channel();
                    let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
                    let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel();
                    let event_process = event_process.clone_read_only_process();
                    runtime.spawn(EventWorker::run(
                        control_receiver,
                        result_sender,
                        event_receiver,
                        event_process,
                    ));
                    (control_sender, result_receiver, event_sender)
                })
                .collect(),
            event_process,
            runtime,
        }
    }

    fn receive(&mut self) -> anyhow::Result<SystemTime> {
        let event_cnt = Rc::new(RefCell::new(0));
        let (next_least, _min_event_ts) = self.source_receiver.try_receive(|event| {
            *event_cnt.borrow_mut() += 1;
            self.event_queue.enqueu_event(event);
        })?;
        Ok(next_least)
    }

    fn submit_event(&mut self, event: Vec<EventWrap<T::T>>) -> anyhow::Result<()> {
        // partition event into parallel workers
        let _min_ts = *event.first().unwrap().ts();
        let max_ts = *event.last().unwrap().ts();
        let min_batch = max(120, event.len() / self.event_worker.len());

        // partition event for each worker
        let mut event = event.into_iter();
        let mut join_handle = Vec::new();
        let current_state = self.event_process.clone_current_state();
        for (control_sender, result_receiver, event_sender) in self.event_worker.iter_mut() {
            let mut event_cnt = 0;
            let mut send_events = Vec::with_capacity(min_batch);
            while event_cnt < min_batch
                && let Some(event) = event.next()
            {
                send_events.push(event.event);
                event_cnt += 1;
            }
            event_sender.send(send_events).unwrap();
            control_sender.send(current_state.clone()).unwrap();
            join_handle.push(|| loop {
                match result_receiver.try_recv() {
                    Ok(result) => {
                        return result;
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(_) => {
                        unimplemented!()
                    }
                };
            });
        }
        for mut handle in join_handle {
            let res = handle();
            for mut event in res {
                // Modify event timestamp
                if event.ts() < &max_ts {
                    event.ts = max_ts;
                }

                self.event_queue
                    .enqueu_event(Box::new(EventWrap::from(event)));
            }
        }

        Ok(())
    }

    #[inline]
    fn run_once(&mut self) -> anyhow::Result<()> {
        // Receive
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
        let next_least = self.receive()?;

        // Process
        let mut read_only_event_buffer = Vec::new();
        while let Some(mut event) = self
            .event_queue
            .dequeue_event(|event| event.ts() <= &next_least)
        {
            if !event.event.inner.is_read_only() {
                if !read_only_event_buffer.is_empty() {
                    if read_only_event_buffer.len() < 100 {
                        for mut read_only_event in std::mem::take(&mut read_only_event_buffer) {
                            self.event_process
                                .process_event(&mut read_only_event, &mut self.event_queue)?;
                        }
                    } else {
                        self.submit_event(std::mem::take(&mut read_only_event_buffer))?;
                    }
                }
                self.event_process
                    .process_event(&mut event, &mut self.event_queue)?;
            } else {
                read_only_event_buffer.push(*event);
            }
        }

        if !read_only_event_buffer.is_empty() {
            if read_only_event_buffer.len() < 500 {
                for mut read_only_event in std::mem::take(&mut read_only_event_buffer) {
                    self.event_process
                        .process_event(&mut read_only_event, &mut self.event_queue)?;
                }
            } else {
                self.submit_event(std::mem::take(&mut read_only_event_buffer))?;
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
