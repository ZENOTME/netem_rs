use hwaddr::HwAddr;

use crate::{PortTable, XdpManagerRef};

pub struct RemoteXdpManager {
    inner: XdpManagerRef,
    port_table: PortTable,
}

impl RemoteXdpManager {
    pub fn new(
        inner: XdpManagerRef,
        port_table: PortTable,
        xdp_program: &str,
        xdp_prog_session: &str,
        if_name: &str,
        mac_addr: HwAddr,
    ) -> Self {
        Self { inner, port_table }
    }

    pub async fn create_xdp(&self, xdp_config: XdpConfig) -> anyhow::Result<PortReceiveHandleImpl> {
        let port_id = self.port_table.fetch_new_port_id();

        let context = self.inner.create_xdp(xdp_config)?;
        let receive_handle = context.receive_handle()?;
        let send_handle = context.send_handle();

        self.port_table
            .add_local_port(PortSendHandleImpl::new_local(port_id, send_handle))
            .await;

        Ok(PortReceiveHandleImpl::new_local(port_id, receive_handle))
    }
}
