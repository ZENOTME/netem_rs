use std::{
    cmp::max,
    sync::{
        atomic::{AtomicPtr, AtomicU64},
        Arc,
    },
    time::SystemTime,
};

use log::warn;
use tokio::sync::Mutex;

#[derive(Debug)]
struct StateNode<T> {
    next: AtomicPtr<StateNode<T>>,
    state: Arc<T>,
    ts: SystemTime,
}

impl<T> StateNode<T> {
    pub(crate) fn new(state: T, ts: SystemTime) -> Self {
        StateNode {
            next: AtomicPtr::new(std::ptr::null_mut()),
            state: Arc::new(state),
            ts,
        }
    }

    pub(crate) fn attach_next(&self, next: Arc<StateNode<T>>) {
        let ptr = Arc::into_raw(next) as *mut StateNode<T>;
        self.next.store(ptr, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn get_next(&self) -> Option<Arc<StateNode<T>>> {
        let ptr = self.next.load(std::sync::atomic::Ordering::Relaxed);
        if ptr.is_null() {
            None
        } else {
            unsafe { Arc::increment_strong_count(ptr) }
            Some(unsafe { Arc::from_raw(ptr) })
        }
    }
}

impl<T> Drop for StateNode<T> {
    fn drop(&mut self) {
        let ptr = self.next.load(std::sync::atomic::Ordering::Relaxed);
        if !ptr.is_null() {
            unsafe { Arc::decrement_strong_count(ptr) }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ShareState<T: Send + Sync + 'static + Clone> {
    tx_id: Arc<AtomicU64>,
    cur_state_node: Arc<StateNode<T>>,
    cur_write: Arc<Mutex<Option<(u64, SystemTime)>>>,
    cur_max_commited_ts: Arc<Mutex<SystemTime>>,
    cur_wait: Arc<tokio::sync::Notify>,
}

impl<T: Send + Sync + 'static + Clone> ShareState<T> {
    pub fn new(init_state: T) -> Self {
        let ts = SystemTime::UNIX_EPOCH;
        let cur_state = Arc::new(StateNode::new(init_state, ts));
        ShareState {
            tx_id: Arc::new(AtomicU64::new(0)),
            cur_state_node: cur_state,
            cur_write: Arc::new(Mutex::new(None)),
            cur_wait: Arc::new(tokio::sync::Notify::new()),
            cur_max_commited_ts: Arc::new(Mutex::new(ts)),
        }
    }

    fn find_closest_state_node_before(&self, upper_ts: SystemTime) -> Arc<StateNode<T>> {
        assert!(
            self.cur_state_node.ts <= upper_ts,
            "Timestamp should always increase, current ts is {:?}, upper ts is {:?}",
            self.cur_state_node.ts,
            upper_ts
        );
        let mut current_node = self.cur_state_node.clone();

        while let Some(next_node) = current_node.get_next() {
            if next_node.ts > upper_ts {
                break;
            }
            current_node = next_node;
        }

        current_node
    }

    pub async fn init_read_only_transaction(&mut self, ts: SystemTime) -> ReadOnlyTranscation<T> {
        // wait for the current write to finish if necessary
        loop {
            let cur_write = self.cur_write.lock().await;
            // need to wait for the write to finish
            if let Some((_, write_ts)) = *cur_write
                && write_ts <= ts
            {
                let wait_fut = self.cur_wait.notified();
                drop(cur_write);
                wait_fut.await;
            } else {
                let mut cur_max_commited_ts = self.cur_max_commited_ts.lock().await;
                let state_node = self.find_closest_state_node_before(ts);
                self.cur_state_node = state_node;
                *cur_max_commited_ts = max(*cur_max_commited_ts, ts);
                return ReadOnlyTranscation {
                    ts,
                    state_node: self.cur_state_node.clone(),
                };
            }
        }
    }

    pub async fn init_read_write_transaction(
        &mut self,
        mut ts: SystemTime,
    ) -> ReadWriteTranscation<T> {
        // wait for the current write to finish if necessary
        loop {
            let mut cur_write = self.cur_write.lock().await;
            // need to wait for the write to finish
            if let Some((_, write_ts)) = *cur_write
                && write_ts <= ts
            {
                let wait_fut = self.cur_wait.notified();
                drop(cur_write);
                wait_fut.await;
            } else {
                let cur_max_commited_ts = self.cur_max_commited_ts.lock().await;
                if *cur_max_commited_ts > ts {
                    if cur_max_commited_ts.duration_since(ts).unwrap()
                        > std::time::Duration::from_micros(10)
                    {
                        warn!("There is event delay, the current ts is {:?}, the max commited ts is {:?}", ts, *cur_max_commited_ts);
                    }
                    ts = cur_max_commited_ts
                        .checked_add(std::time::Duration::from_nanos(1))
                        .unwrap();
                }
                let new_tx_id = self
                    .tx_id
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let state_node = self.find_closest_state_node_before(ts);
                self.cur_state_node = state_node;
                cur_write.replace((new_tx_id, ts));
                return ReadWriteTranscation {
                    tx_id: new_tx_id,
                    ts,
                    state: self.cur_state_node.state.as_ref().clone(),
                };
            }
        }
    }

    pub async fn commit_read_write(&mut self, tx: ReadWriteTranscation<T>) -> bool {
        let fail = {
            let mut cur_write = self.cur_write.lock().await;
            let mut cur_max_commited_ts = self.cur_max_commited_ts.lock().await;
            if cur_write.unwrap().0 != tx.tx_id || cur_write.unwrap().1 != tx.ts {
                true
            } else {
                *cur_max_commited_ts = tx.ts;
                cur_write.take();
                self.cur_state_node
                    .attach_next(Arc::new(StateNode::new(tx.state, tx.ts)));
                self.cur_wait.notify_waiters();
                false
            }
        };

        !fail
    }

    pub async fn abort_read_write(&mut self, tx: ReadWriteTranscation<T>) {
        let mut cur_write = self.cur_write.lock().await;
        if cur_write.unwrap().0 == tx.tx_id && cur_write.unwrap().1 == tx.ts {
            cur_write.take();
            self.cur_wait.notify_waiters();
        }
    }
}

pub struct ReadOnlyTranscation<T: Send + Sync + 'static + Clone> {
    ts: SystemTime,
    state_node: Arc<StateNode<T>>,
}

impl<T: Send + Sync + 'static + Clone> ReadOnlyTranscation<T> {
    pub fn get_state(&self) -> &T {
        self.state_node.state.as_ref()
    }

    pub fn ts(&self) -> SystemTime {
        self.ts
    }
}

pub struct ReadWriteTranscation<T: Send + Sync + 'static + Clone> {
    tx_id: u64,
    ts: SystemTime,
    state: T,
}

impl<T: Send + Sync + 'static + Clone> ReadWriteTranscation<T> {
    pub fn get_state(&self) -> &T {
        &self.state
    }

    pub fn get_state_mut(&mut self) -> &mut T {
        &mut self.state
    }

    pub fn ts(&self) -> SystemTime {
        self.ts
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::spawn;
    use tokio::sync::Mutex;

    fn assert_value_for_node_list<T: PartialEq + Eq + Debug>(
        node: Arc<super::StateNode<T>>,
        value: Vec<T>,
    ) {
        let mut cur_node = Some(node);
        for v in value {
            assert_eq!(*cur_node.as_ref().unwrap().state, v);
            cur_node = cur_node.unwrap().get_next();
        }
    }

    #[test]
    fn test_state_node_list() {
        let ts = std::time::SystemTime::now();
        let node_1 = Arc::new(super::StateNode::new(1, ts));
        {
            let node_2 = Arc::new(super::StateNode::new(2, ts));
            node_1.attach_next(node_2.clone());
            assert_value_for_node_list(node_1.clone(), vec![1, 2]);

            let node_3 = Arc::new(super::StateNode::new(3, ts));
            let node_4 = Arc::new(super::StateNode::new(4, ts));
            let node_5 = Arc::new(super::StateNode::new(5, ts));
            std::thread::scope(|s| {
                s.spawn(|| {
                    node_2.attach_next(node_3.clone());
                    node_3.attach_next(node_4.clone());
                });
                s.spawn(|| {
                    node_4.attach_next(node_5.clone());
                });
            });
            assert_value_for_node_list(node_1.clone(), vec![1, 2, 3, 4, 5]);
        }

        // test ref count
        assert_eq!(Arc::strong_count(&node_1), 1);
        let node_2 = node_1.get_next().unwrap();
        assert_eq!(Arc::strong_count(&node_2), 2);
        let node_3 = node_2.get_next().unwrap();
        assert_eq!(Arc::strong_count(&node_3), 2);
        drop(node_1);
        assert_eq!(Arc::strong_count(&node_2), 1);
        drop(node_2);
        assert_eq!(Arc::strong_count(&node_3), 1);
    }

    #[tokio::test]
    async fn test_share_state() {
        // test:
        // read write
        // commit
        // read only
        // commit
        let now = std::time::SystemTime::now();
        let mut share_state = super::ShareState::new(1);
        let mut tx = share_state.init_read_write_transaction(now).await;
        assert_eq!(*tx.get_state(), 1);
        *tx.get_state_mut() = 2;
        assert!(share_state.commit_read_write(tx).await);

        let tx = share_state
            .init_read_only_transaction(now.checked_add(Duration::from_nanos(1)).unwrap())
            .await;
        assert_eq!(*tx.get_state(), 2);

        // test:
        // read write
        // read only
        // read write commit
        // read only commit
        let mut tx = share_state
            .init_read_write_transaction(now.checked_add(Duration::from_nanos(2)).unwrap())
            .await;
        let mut share_state_clone = share_state.clone();
        spawn(async move {
            let tx = share_state_clone
                .init_read_only_transaction(now.checked_add(Duration::from_nanos(3)).unwrap())
                .await;
            assert_eq!(*tx.get_state(), 3);
        });
        assert_eq!(*tx.get_state(), 2);
        *tx.get_state_mut() = 3;
        assert!(share_state.commit_read_write(tx).await);

        // test:
        // read write 1
        // read write 2
        // read write 1 commit
        // read write 2 commit
        let mut tx = share_state
            .init_read_write_transaction(now.checked_add(Duration::from_nanos(4)).unwrap())
            .await;
        let mut share_state_clone = share_state.clone();
        let join = spawn(async move {
            let mut tx = share_state_clone
                .init_read_write_transaction(now.checked_add(Duration::from_nanos(5)).unwrap())
                .await;
            assert_eq!(*tx.get_state(), 4);
            *tx.get_state_mut() = 5;
            assert!(share_state_clone.commit_read_write(tx).await);
        });
        assert_eq!(*tx.get_state(), 3);
        *tx.get_state_mut() = 4;
        assert!(share_state.commit_read_write(tx).await);
        join.await.unwrap();
        let tx = share_state
            .init_read_only_transaction(now.checked_add(Duration::from_nanos(6)).unwrap())
            .await;
        assert_eq!(*tx.get_state(), 5);

        // test:
        // read write 2
        // read write 1
        // read write 2 commit fail
        let mut tx0 = share_state
            .init_read_write_transaction(now.checked_add(Duration::from_nanos(9)).unwrap())
            .await;
        let mut tx1 = share_state
            .init_read_write_transaction(now.checked_add(Duration::from_nanos(8)).unwrap())
            .await;
        let mut tx2 = share_state
            .init_read_write_transaction(now.checked_add(Duration::from_nanos(7)).unwrap())
            .await;
        *tx0.get_state_mut() = 7;
        *tx1.get_state_mut() = 8;
        *tx2.get_state_mut() = 6;
        assert!(!share_state.commit_read_write(tx0).await);
        assert!(!share_state.commit_read_write(tx1).await);
        assert!(share_state.commit_read_write(tx2).await);
        let tx = share_state
            .init_read_only_transaction(now.checked_add(Duration::from_nanos(10)).unwrap())
            .await;
        assert_eq!(*tx.get_state(), 6);

        // test:
        // read 2
        // read write 1 get ts larger than 2
        let tx = share_state
            .init_read_only_transaction(now.checked_add(Duration::from_nanos(11)).unwrap())
            .await;
        assert_eq!(*tx.get_state(), 6);
        let tx = share_state
            .init_read_write_transaction(now.checked_add(Duration::from_nanos(10)).unwrap())
            .await;
        assert_eq!(*tx.get_state(), 6);
        assert!(tx.ts()> now.checked_add(Duration::from_nanos(11)).unwrap());
        share_state.abort_read_write(tx).await;

        // test
        // read write 1
        // read 2 
        // read write 1 abort 
        let mut tx = share_state
            .init_read_write_transaction(now.checked_add(Duration::from_nanos(12)).unwrap())
            .await;
        let mut share_state_clone = share_state.clone();
        let join = spawn(async move {
            let tx = share_state_clone
                .init_read_only_transaction(now.checked_add(Duration::from_nanos(13)).unwrap())
                .await;
            assert_eq!(*tx.get_state(), 6);
        });
        *tx.get_state_mut() = 7;
        share_state.abort_read_write(tx).await;
        join.await.unwrap();
    }

    #[tokio::test]
    async fn test_share_state_perf() {
        let mut share_state = super::ShareState::new(1);

        // test rw modify 
        let now = std::time::SystemTime::now();
        let mut tx = share_state.init_read_write_transaction(now).await;
        println!("rw init: {:?}", now.elapsed().unwrap());
        *tx.get_state_mut() = 2;
        share_state.commit_read_write(tx).await;
        println!("rw modify: {:?}", now.elapsed().unwrap());

        let mut i = 1;
        let now = std::time::SystemTime::now();
        i = 2;
        println!("rw modify: {:?}", now.elapsed().unwrap());

        let mut i = Arc::new(Mutex::new(1));
        let now = std::time::SystemTime::now();
        *i.lock().await = 2;
        println!("rw modify: {:?}", now.elapsed().unwrap());

        // let mut i = Arc::new(Atomic)
    }
}
