#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeInfo {
    #[prost(string, tag = "1")]
    pub host_addr: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "2")]
    pub eth_mac_addr: ::prost::alloc::vec::Vec<u8>,
    #[prost(int32, tag = "3")]
    pub xdp_subnet_id: i32,
}
