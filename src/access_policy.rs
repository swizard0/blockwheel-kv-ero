use crate::{
    proto,
    ftd_sklave,
};

pub struct AccessPolicy;

impl blockwheel_kv::AccessPolicy for AccessPolicy {
    type Order = ftd_sklave::Order;
    type Info = proto::RequestInfoReplyTx;
    type Insert = proto::RequestInsertReplyTx;
    type LookupRange = proto::RequestLookupKind;
    type Remove = proto::RequestRemoveReplyTx;
    type Flush = proto::RequestFlushReplyTx;
}
