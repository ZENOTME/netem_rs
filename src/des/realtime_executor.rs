use std::{sync::Arc, time::SystemTime};

use intrusive_collections::{intrusive_adapter, LinkedList, LinkedListLink};
use log::error;
use tokio::sync::mpsc::error::TryRecvError;

pub struct Event<T> {
    link: LinkedListLink,
    pub data: T,
    pub time: SystemTime,
}

impl<T> Event<T> {
    pub fn new(data: T, time: SystemTime) -> Event<T> {
        Event {
            data,
            time,
            link: LinkedListLink::new(),
        }
    }
}

intrusive_adapter!(EventAdpator<T> = Box<Event<T>>: Event<T> { link: LinkedListLink });

pub struct RealTimeQueue<T> {
    queue: LinkedList<EventAdpator<T>>,
}

impl<T> RealTimeQueue<T> {
    pub fn new() -> Self {
        Self {
            queue: LinkedList::new(EventAdpator::new()),
        }
    }

    pub fn enqueue_event(&mut self, event: Box<Event<T>>) {
        let mut last_cursor = self.queue.back_mut();
        while let Some(element) = last_cursor.get() {
            if element.time > event.time {
                last_cursor.move_prev();
            } else {
                break;
            }
        }
        last_cursor.insert_after(event);
    }

    // dequeue the first event statisfied the check
    pub fn dequeue_event(&mut self) -> Option<Box<Event<T>>> {
        let mut cursor = self.queue.front_mut();
        while let Some(event) = cursor.get() {
            if event.time <= SystemTime::now() {
                return Some(cursor.remove().unwrap());
            }
        }
        None
    }

    pub fn is_empy(&self) -> bool {
        self.queue.is_empty()
    }
}

pub trait EventProcess: Sized + Send + 'static + Clone {
    type T: Send + 'static;
    fn process_event(
        &mut self,
        event: Event<Self::T>,
        queue: &mut RealTimeQueue<Self::T>,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}

pub struct EventRumtime<EP: EventProcess> {
    tx: Vec<tokio::sync::mpsc::UnboundedSender<Box<Event<EP::T>>>>,
    _parallel: usize,
}

impl<EP: EventProcess> EventRumtime<EP> {
    pub fn start(
        parallel: usize,
        event_process: EP,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let mut tx_vec = Vec::with_capacity(parallel);
        for _ in 0..parallel {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Box<Event<EP::T>>>();
            let mut event_process = event_process.clone();
            runtime_handle.spawn(async move {
                let mut queue = RealTimeQueue::new();
                loop {
                    match rx.try_recv() {
                        Ok(event) => queue.enqueue_event(event),
                        Err(TryRecvError::Empty) => {}
                        Err(err) => {
                            error!("Error: {:?}", err);
                            break;
                        }
                    }
                    while let Some(event) = queue.dequeue_event() {
                        event_process
                            .process_event(*event, &mut queue)
                            .await
                            .unwrap();
                    }
                }
            });
            tx_vec.push(tx);
        }
        Self {
            tx: tx_vec,
            _parallel: parallel,
        }
    }

    pub fn handle(&self) -> EventRuntimeHandle<EP::T> {
        EventRuntimeHandle {
            tx: self.tx.clone(),
        }
    }
}

pub struct EventRuntimeHandle<T> {
    tx: Vec<tokio::sync::mpsc::UnboundedSender<Box<Event<T>>>>,
}

impl<T> Clone for EventRuntimeHandle<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<T> EventRuntimeHandle<T> {
    pub fn spawn_event(&self, event: Event<T>, idx: usize) {
        if let Err(err) = self.tx[idx].send(Box::new(event)) {
            error!("Error: {:?}", err);
        }
    }
}
