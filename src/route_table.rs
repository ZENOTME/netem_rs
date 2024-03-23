use std::{collections::HashMap, sync::Arc};

use hwaddr::HwAddr;
use tokio::sync::RwLock;

// Map mac address to port id
#[derive(Clone)]
pub struct RouteTable {
    inner: Arc<RwLock<HashMap<HwAddr, u32>>>,
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
