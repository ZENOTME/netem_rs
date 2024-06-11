use std::{cell::RefCell, rc::Rc, time::SystemTime};

use tokio::sync::mpsc::{error::TryRecvError, UnboundedReceiver};

use super::{
    parallel::{EventProperty, EventReadWriteProcess},
    EventQueue, EventReceiver, SourceReceiver,
};

pub struct EventSimulator<T: EventReadWriteProcess> {
    control_receiver: UnboundedReceiver<EventReceiver<T::T>>,
    source_receiver: SourceReceiver<T::T>,
    event_queue: EventQueue<T::T>,
    event_process: T,
}

impl<T: EventReadWriteProcess> EventSimulator<T> {
    pub fn new(event_process: T, control_receiver: UnboundedReceiver<EventReceiver<T::T>>) -> Self {
        Self {
            control_receiver,
            source_receiver: SourceReceiver::new(),
            event_queue: EventQueue::new(),
            event_process,
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
                    self.event_process.process_read_only_event_batch(
                        &mut read_only_event_buffer,
                        &mut self.event_queue,
                        &self.event_process.clone_current_state(),
                    )?;
                }
                self.event_process
                    .process_event(&mut event, &mut self.event_queue)?;
            } else {
                read_only_event_buffer.push(*event);
            }
        }

        if !read_only_event_buffer.is_empty() {
            self.event_process.process_read_only_event_batch(
                &mut read_only_event_buffer,
                &mut self.event_queue,
                &self.event_process.clone_current_state(),
            )?;
        }

        Ok(())
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        loop {
            self.run_once()?;
        }
    }
}
