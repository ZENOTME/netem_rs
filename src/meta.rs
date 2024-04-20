use std::{net::SocketAddr, str::FromStr};

use anyhow::Context;
use hwaddr::HwAddr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HostAddr {
    pub host: String,
    pub port: u16,
}

impl FromStr for HostAddr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl TryFrom<&String> for HostAddr {
    type Error = anyhow::Error;

    fn try_from(s: &String) -> Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

impl TryFrom<&str> for HostAddr {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let s = if !s.starts_with("http") {
            format!("http://{s}", s = s)
        } else {
            s.to_string()
        };
        let addr = url::Url::parse(&s).with_context(|| format!("failed to parse address: {s}"))?;
        Ok(HostAddr {
            host: addr.host().context("invalid host")?.to_string(),
            port: addr.port().context("invalid port")?,
        })
    }
}

impl ToString for HostAddr {
    fn to_string(&self) -> String {
        format!("http://{s}:{port}", s = self.host, port = self.port)
    }
}

impl TryFrom<SocketAddr> for HostAddr {
    type Error = anyhow::Error;

    fn try_from(addr: SocketAddr) -> Result<Self, Self::Error> {
        Ok(HostAddr {
            host: addr.ip().to_string(),
            port: addr.port(),
        })
    }
}

impl TryInto<tonic::transport::Endpoint> for HostAddr {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<tonic::transport::Endpoint, Self::Error> {
        let addr = format!("http://{s}:{port}", s = self.host, port = self.port);
        tonic::transport::Endpoint::try_from(addr).context("failed to parse endpoint")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeInfo {
    pub addr: HostAddr,
    pub eth_mac_addr: Option<HwAddr>,
    pub xdp_subnet_id: i32,
}

impl From<NodeInfo> for crate::proto::NodeInfo {
    fn from(val: NodeInfo) -> Self {
        crate::proto::NodeInfo {
            host_addr: val.addr.to_string(),
            eth_mac_addr: val
                .eth_mac_addr
                .map(|val| val.octets().to_vec())
                .unwrap_or_default(),
            xdp_subnet_id: val.xdp_subnet_id,
        }
    }
}

impl TryFrom<crate::proto::NodeInfo> for NodeInfo {
    type Error = anyhow::Error;

    fn try_from(info: crate::proto::NodeInfo) -> Result<Self, Self::Error> {
        if !((info.eth_mac_addr.len() == 6 && info.xdp_subnet_id != -1)
            || (info.host_addr.is_empty() && info.xdp_subnet_id == -1))
        {
            return Err(anyhow::anyhow!("Invalid node info"));
        }
        Ok(NodeInfo {
            addr: info.host_addr.parse()?,
            eth_mac_addr: if info.eth_mac_addr.is_empty() {
                None
            } else {
                Some(HwAddr::from(&info.eth_mac_addr[..6]))
            },
            xdp_subnet_id: info.xdp_subnet_id,
        })
    }
}

pub trait MetaClient {
    fn connet(meta_addr: HostAddr) -> Self;
    fn register(&self, addr: HostAddr) -> impl std::future::Future<Output = Vec<NodeInfo>> + Send;
}
