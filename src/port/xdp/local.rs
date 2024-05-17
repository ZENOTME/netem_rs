use crate::{PortReceiveHandleImpl, PortSendHandleImpl, PortTable, XdpConfig};

use super::XdpManagerRef;

pub struct LocalXdpManager {
    inner: XdpManagerRef,
    port_table: PortTable,
}

impl LocalXdpManager {
    pub fn new(inner: XdpManagerRef, port_table: PortTable) -> Self {
        Self { inner, port_table }
    }

    pub fn create_xdp(&self, xdp_config: XdpConfig) -> anyhow::Result<PortReceiveHandleImpl> {
        let port_id = self.port_table.fetch_new_port_id();
        let port_mac = xdp_config.mac_addr;

        let context = self.inner.create_xdp(xdp_config)?;
        let receive_handle = context.receive_handle()?;
        let send_handle = context.send_handle();

        self.port_table.add_local_port(
            port_mac,
            PortSendHandleImpl::new_local(port_id, send_handle),
        );

        Ok(PortReceiveHandleImpl::new_local(port_id, receive_handle))
    }
}
