use hwaddr::HwAddr;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::atomic::Ordering;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
};

use crate::{HostAddr, PortSendHandleImpl};

struct ControlFn(
    Box<
        dyn Fn(
                &mut HashMap<HwAddr, u32>,
                &mut HashMap<u32, PortSendHandleImpl>,
                &mut HashMap<HostAddr, u32>,
            ) + Send
            + Sync,
    >,
);

impl Debug for ControlFn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ControlFn")
    }
}

/// A table map port id to send handle. It can be shared between multiple owners and access concurrently.
///
/// mac_addr -> port_id -> send_handle
pub struct PortTable {
    port_id: Arc<AtomicUsize>,

    global_len: Arc<AtomicUsize>,
    log: Arc<spin::Mutex<Vec<ControlFn>>>,
    local_len: usize,

    mac_to_id: HashMap<HwAddr, u32>,
    id_to_ports: HashMap<u32, PortSendHandleImpl>,
    remote_addr_to_id: HashMap<HostAddr, u32>,
}

impl Clone for PortTable {
    fn clone(&self) -> Self {
        Self {
            port_id: self.port_id.clone(),
            global_len: self.global_len.clone(),
            log: self.log.clone(),
            local_len: self.local_len,
            mac_to_id: self.mac_to_id.clone(),
            id_to_ports: self.id_to_ports.clone(),
            remote_addr_to_id: self.remote_addr_to_id.clone(),
        }
    }
}

impl Default for PortTable {
    fn default() -> Self {
        Self::new()
    }
}

impl PortTable {
    pub fn new() -> Self {
        Self {
            port_id: Arc::new(AtomicUsize::new(0)),
            mac_to_id: HashMap::new(),
            id_to_ports: HashMap::new(),
            remote_addr_to_id: HashMap::new(),
            global_len: Arc::new(AtomicUsize::new(0)),
            log: Arc::new(spin::Mutex::new(Vec::with_capacity(2048))),
            local_len: 0,
        }
    }

    pub fn fetch_new_port_id(&self) -> u32 {
        self.port_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u32
    }

    fn add_new_log_entry(&self, func: ControlFn) {
        let mut lock = self.log.lock();
        lock.push(func);
        let new_len = lock.len();
        let old_len = self.global_len.fetch_add(1, Ordering::Relaxed);
        assert_eq!(new_len, old_len + 1);
    }

    pub fn add_local_port(&self, port_mac: HwAddr, send_handle: PortSendHandleImpl) {
        assert!(
            send_handle.port_id() < self.port_id.load(std::sync::atomic::Ordering::Relaxed) as u32
        );
        let func = move |mac_to_id: &mut HashMap<HwAddr, u32>,
                         id_to_ports: &mut HashMap<u32, PortSendHandleImpl>,
                         _remote_addr_to_id: &mut HashMap<HostAddr, u32>| {
            let send_handle = send_handle.clone();
            let port_id = send_handle.port_id();
            mac_to_id.insert(port_mac, port_id);
            id_to_ports.insert(port_id, send_handle);
        };

        self.add_new_log_entry(ControlFn(Box::new(func)));
    }

    /// For remote port, it will reuse a same send handle for all remote ports with different mac whihin the same remote addr(remote mode).
    /// Usually, the port manager will use this function to add the send handle for remote node first.
    /// Then, the `ActorManager` will accpet the remote actor info and add the remote port using `add_remote_port`.
    pub fn add_remote_handle(&self, remote_addr: HostAddr, send_handle: PortSendHandleImpl) {
        assert!(
            send_handle.port_id() < self.port_id.load(std::sync::atomic::Ordering::Relaxed) as u32
        );
        let func = move |_mac_to_id: &mut HashMap<HwAddr, u32>,
                         id_to_ports: &mut HashMap<u32, PortSendHandleImpl>,
                         remote_addr_to_id: &mut HashMap<HostAddr, u32>| {
            let send_handle = send_handle.clone();
            let port_id = send_handle.port_id();
            remote_addr_to_id.insert(remote_addr.clone(), port_id);
            id_to_ports.insert(port_id, send_handle);
        };
        
        self.add_new_log_entry(ControlFn(Box::new(func)));
    }

    pub fn add_remote_port(&self, remote_addr: HostAddr, port_mac: HwAddr) -> anyhow::Result<()> {
        let func = move |mac_to_id: &mut HashMap<HwAddr, u32>,
                         _id_to_ports: &mut HashMap<u32, PortSendHandleImpl>,
                         remote_addr_to_id: &mut HashMap<HostAddr, u32>| {
            let port_id = *remote_addr_to_id.get(&remote_addr).unwrap();
            mac_to_id.insert(port_mac, port_id);
        };

        self.add_new_log_entry(ControlFn(Box::new(func)));
        Ok(())
    }

    #[inline]
    fn try_apply_control(&mut self) {
        let new_len = self.global_len.load(Ordering::Relaxed);
        if new_len > self.local_len {
            let lock = self.log.lock();
            assert!(new_len <= lock.len());
            for i in self.local_len..new_len {
                let func = lock.get(i).unwrap();
                func.0(
                    &mut self.mac_to_id,
                    &mut self.id_to_ports,
                    &mut self.remote_addr_to_id,
                );
            }
            self.local_len = new_len;
        }
    }

    pub fn get_port_id(&mut self, mac_addr: HwAddr) -> Option<u32> {
        self.try_apply_control();
        self.mac_to_id.get(&mac_addr).cloned()
    }

    pub fn get_send_handle(&mut self, mac_addr: HwAddr) -> Option<&PortSendHandleImpl> {
        self.try_apply_control();
        let port_id = self.mac_to_id.get(&mac_addr)?;
        self.id_to_ports.get(port_id)
    }

    pub fn get_send_handle_by_id(&mut self, port_id: u32) -> Option<&PortSendHandleImpl> {
        self.try_apply_control();
        self.id_to_ports.get(&port_id)
    }

    pub fn for_each_port<F>(&mut self, mut f: F) -> anyhow::Result<()>
    where
        F: FnMut(&u32, &PortSendHandleImpl) -> anyhow::Result<()>,
    {
        self.try_apply_control();
        for (port_id, send_handle) in &self.id_to_ports {
            f(port_id, send_handle)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use hwaddr::HwAddr;

    fn test_get_port(table: &mut PortTable, mac: HwAddr, expect_port_id: u32) {
        assert_eq!(table.get_port_id(mac), Some(expect_port_id));
        let send_handle = table.get_send_handle(mac).unwrap();
        assert_eq!(send_handle.port_id(), expect_port_id);
    }

    #[test]
    fn test_local() {
        let mut table1 = PortTable::new();
        let mut table2 = table1.clone();

        let mac1 = HwAddr::from([0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
        let id = table1.fetch_new_port_id();
        let local_port = PortSendHandleImpl::new_mock(id);
        table1.add_local_port(mac1, local_port.clone());

        let mac2 = HwAddr::from([0x00, 0x00, 0x00, 0x00, 0x00, 0x02]);
        let id = table2.fetch_new_port_id();
        let local_port = PortSendHandleImpl::new_mock(id);
        table2.add_local_port(mac2, local_port.clone());

        assert_eq!(table1.get_port_id(mac1), Some(0));
        assert_eq!(table1.get_port_id(mac2), Some(1));
        assert_eq!(table2.get_port_id(mac1), Some(0));
        assert_eq!(table2.get_port_id(mac2), Some(1));

        let mut table3 = table1.clone();
        assert_eq!(table3.get_port_id(mac1), Some(0));
        assert_eq!(table3.get_port_id(mac2), Some(1));

        let mac3 = HwAddr::from([0x00, 0x00, 0x00, 0x00, 0x00, 0x03]);
        let id = table3.fetch_new_port_id();
        let local_port = PortSendHandleImpl::new_mock(id);
        table3.add_local_port(mac3, local_port.clone());

        assert_eq!(table1.get_port_id(mac3), Some(2));
        assert_eq!(table2.get_port_id(mac3), Some(2));
        assert_eq!(table3.get_port_id(mac3), Some(2));

        let mut table4 = table3.clone();
        let mut count = 0;
        table4.for_each_port(|_port_id, _send_handle| {
            count += 1;
            Ok(())
        }).unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_remote() {
        let mut table1 = PortTable::new();
        let mut table2 = table1.clone();

        let remote_id1 = table1.fetch_new_port_id();
        let remote_host1 = HostAddr::from_str("http://192.168.202.1:4405").unwrap();
        let remote_port = PortSendHandleImpl::new_mock(remote_id1);
        table1.add_remote_handle(remote_host1.clone(), remote_port.clone());

        let remote_id2 = table2.fetch_new_port_id();
        let remote_host2 = HostAddr::from_str("http://192.168.2.2:4405").unwrap();
        let remote_port = PortSendHandleImpl::new_mock(remote_id2);
        table2.add_remote_handle(remote_host2.clone(), remote_port.clone());

        let mut table3 = table2.clone();
        let mac1 = HwAddr::from([0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
        table3.add_remote_port(remote_host1.clone(), mac1).unwrap();
        let mac2 = HwAddr::from([0x00, 0x00, 0x00, 0x00, 0x00, 0x02]);
        table3.add_remote_port(remote_host2.clone(), mac2).unwrap();
        let mac3 = HwAddr::from([0x00, 0x00, 0x00, 0x00, 0x00, 0x03]);
        let local_id1 = table3.fetch_new_port_id();
        let local_port = PortSendHandleImpl::new_mock(local_id1);
        table3.add_local_port(mac3, local_port.clone());

        test_get_port(&mut table1, mac1, remote_id1);
        test_get_port(&mut table1, mac2, remote_id2);
        test_get_port(&mut table1, mac3, local_id1);
        
        test_get_port(&mut table2, mac1, remote_id1);
        test_get_port(&mut table2, mac2, remote_id2);
        test_get_port(&mut table2, mac3, local_id1);

        test_get_port(&mut table3, mac1, remote_id1);
        test_get_port(&mut table3, mac2, remote_id2);
        test_get_port(&mut table3, mac3, local_id1);

        let mut table4 = table3.clone();

        test_get_port(&mut table4, mac1, remote_id1);
        test_get_port(&mut table4, mac2, remote_id2);
        test_get_port(&mut table4, mac3, local_id1);
    }
}
