use std::{
    ops::{
        Bound,
        RangeBounds,
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
    LookupRange(RequestLookupRange),
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
    pub reply_tx: oneshot::Sender<Option<kv::ValueCell<kv::Value>>>,
}

pub struct RequestLookupKindRange {
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

pub struct RequestLookupRange {
    pub search_range: SearchRangeBounds,
    pub reply_kind: RequestLookupKind,
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

#[derive(Clone, Debug)]
pub struct SearchRangeBounds {
    pub range_from: Bound<kv::Key>,
    pub range_to: Bound<kv::Key>,
}

impl SearchRangeBounds {
    pub fn single(key: kv::Key) -> SearchRangeBounds {
        SearchRangeBounds {
            range_from: Bound::Included(key.clone()),
            range_to: Bound::Included(key),
        }
    }
}

impl<R> From<R> for SearchRangeBounds where R: RangeBounds<kv::Key> {
    fn from(range: R) -> SearchRangeBounds {
        SearchRangeBounds {
            range_from: match range.start_bound() {
                Bound::Unbounded =>
                    Bound::Unbounded,
                Bound::Included(key) =>
                    Bound::Included(key.clone()),
                Bound::Excluded(key) =>
                    Bound::Excluded(key.clone()),
            },
            range_to: match range.end_bound() {
                Bound::Unbounded =>
                    Bound::Unbounded,
                Bound::Included(key) =>
                    Bound::Included(key.clone()),
                Bound::Excluded(key) =>
                    Bound::Excluded(key.clone()),
            },
        }
    }
}
