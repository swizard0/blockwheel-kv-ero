#[forbid(unsafe_code)]

use std::{
    ops::{
        RangeBounds,
    },
};

use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    stream,
    SinkExt,
};

use alloc_pool::{
    bytes::{
        Bytes,
        BytesPool,
    },
};

pub use blockwheel_kv::{
    kv,
    wheels,
    Params,
    Inserted,
    Removed,
    Flushed,
    Info,
    WheelInfo,
};

mod proto;

pub struct GenServer {
    request_tx: mpsc::Sender<proto::Request>,
    fused_request_rx: stream::Fuse<mpsc::Receiver<proto::Request>>,
}

#[derive(Clone)]
pub struct Pid {
    request_tx: mpsc::Sender<proto::Request>,
}

#[derive(Debug)]
pub enum InfoError {
    GenServer(ero::NoProcError),
}

#[derive(Debug)]
pub enum InsertError {
    GenServer(ero::NoProcError),
}

#[derive(Debug)]
pub enum LookupError {
    GenServer(ero::NoProcError),
}

#[derive(Debug)]
pub enum LookupRangeError {
    GenServer(ero::NoProcError),
}

#[derive(Debug)]
pub enum RemoveError {
    GenServer(ero::NoProcError),
}

#[derive(Debug)]
pub enum FlushError {
    GenServer(ero::NoProcError),
}

pub struct LookupRange {
    pub key_values_rx: mpsc::Receiver<KeyValueStreamItem>,
}

#[derive(Clone)]
pub enum KeyValueStreamItem {
    KeyValue(kv::KeyValuePair<kv::Value>),
    NoMore,
}

impl Pid {
    pub async fn info(&mut self) -> Result<Info, InfoError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(proto::Request::Info(proto::RequestInfo { reply_tx, })).await
                .map_err(|_send_error| InfoError::GenServer(ero::NoProcError))?;
            match reply_rx.await {
                Ok(info) =>
                    return Ok(info),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn insert(&mut self, key: kv::Key, value: kv::Value) -> Result<Inserted, InsertError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::Insert(proto::RequestInsert {
                    key: key.clone(),
                    value: value.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| InsertError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(inserted) =>
                    return Ok(inserted),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn lookup(&mut self, key: kv::Key) -> Result<Option<kv::ValueCell<kv::Value>>, LookupError> {
        let search_range = proto::SearchRangeBounds::single(key);
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::LookupRange(proto::RequestLookupRange {
                    search_range: search_range.clone(),
                    reply_kind: proto::RequestLookupKind::Single(
                        proto::RequestLookupKindSingle { reply_tx, },
                    ),
                }))
                .await
                .map_err(|_send_error| LookupError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(result) =>
                    return Ok(result),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn lookup_range<R>(&mut self, range: R) -> Result<LookupRange, LookupRangeError> where R: RangeBounds<kv::Key> {
        let search_range: proto::SearchRangeBounds = range.into();
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::LookupRange(proto::RequestLookupRange {
                    search_range: search_range.clone(),
                    reply_kind: proto::RequestLookupKind::Range(
                        proto::RequestLookupKindRange { reply_tx, },
                    ),
                }))
                .await
                .map_err(|_send_error| LookupRangeError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(result) =>
                    return Ok(result),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn remove(&mut self, key: kv::Key) -> Result<Removed, RemoveError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::Remove(proto::RequestRemove {
                    key: key.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| RemoveError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(result) =>
                    return Ok(result),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn flush_all(&mut self) -> Result<Flushed, FlushError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::FlushAll(proto::RequestFlush { reply_tx, })).await
                .map_err(|_send_error| FlushError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Flushed) =>
                    return Ok(Flushed),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}
