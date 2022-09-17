use std::{
    ops::{
        Bound,
    },
};

use futures::{
    channel::{
        oneshot,
    },
};

use crate::{
    kv,
    Info,
    Inserted,
    Removed,
    Flushed,
    LookupRange,
};

pub enum Request {
    Info(RequestInfo),
    Insert(RequestInsert),
    LookupRange(RequestLookupKind),
    Remove(RequestRemove),
    FlushAll(RequestFlush),
}

pub type RequestInfoReplyTx = oneshot::Sender<Info>;
pub type RequestInsertReplyTx = oneshot::Sender<Inserted>;
pub type RequestRemoveReplyTx = oneshot::Sender<Removed>;
pub type RequestFlushReplyTx = oneshot::Sender<Flushed>;

pub enum RequestLookupKind {
    Single(RequestLookupKindSingle),
    Range(RequestLookupKindRange),
}

pub struct RequestLookupKindSingle {
    pub key: kv::Key,
    pub reply_tx: oneshot::Sender<Option<kv::ValueCell<kv::Value>>>,
}

pub struct RequestLookupKindRange {
    pub range_from: Bound<kv::Key>,
    pub range_to: Bound<kv::Key>,
    pub reply_tx: oneshot::Sender<LookupRange>,
}

#[derive(Debug)]
pub struct RequestInfo {
    pub reply_tx: RequestInfoReplyTx,
}

#[derive(Debug)]
pub struct RequestInsert {
    pub key: kv::Key,
    pub value: kv::Value,
    pub reply_tx: RequestInsertReplyTx,
}

#[derive(Debug)]
pub struct RequestRemove {
    pub key: kv::Key,
    pub reply_tx: RequestRemoveReplyTx,
}

#[derive(Debug)]
pub struct RequestFlush {
    pub reply_tx: RequestFlushReplyTx,
}
