use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
};

use anyhow::anyhow;
use hwaddr::HwAddr;
use tokio::sync::RwLock;

use crate::{HostAddr, PortSendHandleImpl};

/// A table map port id to send handle. It can be shared between multiple owners and access concurrently.
///
/// mac_addr -> port_id -> send_handle
#[derive(Clone)]
pub struct PortTable {
    port_id: Arc<AtomicUsize>,

    // # NOTE
    // To avoid deadlocks, we should always acquire the lock in the following order as the order of the fields.
    mac_to_id: Arc<RwLock<HashMap<HwAddr, u32>>>,
    id_to_ports: Arc<RwLock<HashMap<u32, PortSendHandleImpl>>>,
    // map remote addr(IP) to port id. Used to help find remote port id for a remote node.
    remote_addr_to_id: Arc<RwLock<HashMap<HostAddr, u32>>>,

    ports_cache: HashMap<HwAddr, PortSendHandleImpl>,
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
            id_to_ports: Arc::new(RwLock::new(HashMap::new())),
            mac_to_id: Arc::new(RwLock::new(HashMap::new())),
            remote_addr_to_id: Arc::new(RwLock::new(HashMap::new())),
            ports_cache: HashMap::new(),
        }
    }

    pub fn fetch_new_port_id(&self) -> u32 {
        self.port_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u32
    }

    pub async fn add_local_port(&self, port_mac: HwAddr, send_handle: PortSendHandleImpl) {
        assert!(
            send_handle.port_id() < self.port_id.load(std::sync::atomic::Ordering::Relaxed) as u32
        );
        let mut mac_to_id = self.mac_to_id.write().await;
        let mut id_to_ports = self.id_to_ports.write().await;
        let port_id = send_handle.port_id();
        mac_to_id.insert(port_mac, port_id);
        id_to_ports.insert(port_id, send_handle);
    }

    /// For remote port, it will reuse a same send handle for all remote ports with different mac whihin the same remote addr(remote mode).
    /// Usually, the port manager will use this function to add the send handle for remote node first.
    /// Then, the `ActorManager` will accpet the remote actor info and add the remote port using `add_remote_port`.
    pub async fn add_remote_handle(&self, remote_addr: HostAddr, send_handle: PortSendHandleImpl) {
        assert!(
            send_handle.port_id() < self.port_id.load(std::sync::atomic::Ordering::Relaxed) as u32
        );
        let mut id_to_ports = self.id_to_ports.write().await;
        let mut remote_addr_to_id = self.remote_addr_to_id.write().await;
        let port_id = send_handle.port_id();
        remote_addr_to_id.insert(remote_addr.clone(), port_id);
        id_to_ports.insert(port_id, send_handle);
    }

    pub async fn add_remote_port(
        &self,
        remote_addr: HostAddr,
        port_mac: HwAddr,
    ) -> anyhow::Result<()> {
        let mut mac_to_id = self.mac_to_id.write().await;
        let remote_addr_to_id = self.remote_addr_to_id.read().await;
        let port_id = *remote_addr_to_id
            .get(&remote_addr)
            .ok_or(anyhow!("Remote addr not found"))?;
        mac_to_id.insert(port_mac, port_id);
        Ok(())
    }

    pub async fn get_send_handle(&mut self, mac_addr: HwAddr) -> Option<&PortSendHandleImpl> {
        if let std::collections::hash_map::Entry::Vacant(e) = self.ports_cache.entry(mac_addr) {
            let port_id = *self.mac_to_id.read().await.get(&mac_addr)?;
            let handle = self.id_to_ports.read().await.get(&port_id).cloned();
            if let Some(handle) = handle {
                e.insert(handle.clone());
            }
        }
        self.ports_cache.get(&mac_addr)
    }

    pub async fn for_each_port<F>(&self, mut f: F) -> anyhow::Result<()>
    where
        F: FnMut(&u32, &PortSendHandleImpl) -> anyhow::Result<()>,
    {
        // # TODO
        // Optimize it using cache
        let ports = self.id_to_ports.read().await;
        for (port_id, handle) in ports.iter() {
            f(port_id, handle)?;
        }
        Ok(())
    }
}
