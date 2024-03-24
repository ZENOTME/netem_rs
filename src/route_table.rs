use std::{collections::HashMap, sync::Arc};

use hwaddr::HwAddr;
use tokio::sync::RwLock;

// A table map mac address to port id. It can be shared between multiple owners and access concurrently.
#[derive(Clone, Debug)]
pub struct RouteTable {
    inner: Arc<RwLock<HashMap<HwAddr, u32>>>,
}

impl Default for RouteTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RouteTable {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_route(&self, mac_addr: HwAddr, port_id: u32) {
        self.inner.write().await.insert(mac_addr, port_id);
    }

    pub async fn remove_route(&self, mac_addr: HwAddr) {
        self.inner.write().await.remove(&mac_addr);
    }

    pub async fn get_port_id(&self, mac_addr: HwAddr) -> Option<u32> {
        self.inner.read().await.get(&mac_addr).copied()
    }
}
