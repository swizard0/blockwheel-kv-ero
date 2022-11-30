use futures::{
    channel::{
        oneshot,
    },
};

use arbeitssklave::{
    komm,
};

use crate::{
    kv,
    job,
    proto,
    Info,
    Inserted,
    Removed,
    Flushed,
};

pub type SklaveJob = arbeitssklave::komm::SklaveJob<Welt, Order>;

pub enum Order {
    InfoCancel(komm::UmschlagAbbrechen<proto::RequestInfoReplyTx>),
    Info(komm::Umschlag<Info, proto::RequestInfoReplyTx>),
    InsertCancel(komm::UmschlagAbbrechen<proto::RequestInsertReplyTx>),
    Insert(komm::Umschlag<Inserted, proto::RequestInsertReplyTx>),
    LookupRangeCancel(komm::UmschlagAbbrechen<LookupKind>),
    LookupRange(komm::Umschlag<komm::Streamzeug<kv::KeyValuePair<kv::Value>>, LookupKind>),
    RemoveCancel(komm::UmschlagAbbrechen<proto::RequestRemoveReplyTx>),
    Remove(komm::Umschlag<Removed, proto::RequestRemoveReplyTx>),
    FlushCancel(komm::UmschlagAbbrechen<proto::RequestFlushReplyTx>),
    Flushed(komm::Umschlag<Flushed, proto::RequestFlushReplyTx>),
}

#[derive(Default)]
pub struct Welt;

pub enum LookupKind {
    Single(LookupKindSingle),
    Range(LookupKindRange),
}

pub struct LookupKindSingle {
    pub reply_tx: oneshot::Sender<Option<kv::ValueCell<kv::Value>>>,
    pub feedback_tx: oneshot::Sender<komm::StreamId>,
}

pub struct LookupKindRange {
    pub kv_items_stream_tx: oneshot::Sender<komm::Streamzeug<kv::KeyValuePair<kv::Value>>>,
}

pub fn job<P>(sklave_job: SklaveJob, thread_pool: &P) where P: edeltraud::ThreadPool<job::Job> {
    if let Err(error) = run_job(sklave_job, thread_pool) {
        log::error!("job terminated with error: {:?}", error);
    }
}

#[derive(Debug)]
pub enum Error {
    ReceiveOrder(arbeitssklave::Error),
    GenServerIsLostOnRequestInfo,
    GenServerIsLostOnRequestInsert,
    GenServerIsLostOnRequestRemove,
    GenServerIsLostOnRequestFlush,
    GenServerIsLostOnRequestLookupSingle,
    GenServerIsLostOnRequestLookupRange,
}

fn run_job<P>(mut sklave_job: SklaveJob, _thread_pool: &P) -> Result<(), Error> where P: edeltraud::ThreadPool<job::Job> {
    loop {
        let mut befehle = match sklave_job.zu_ihren_diensten() {
            Ok(arbeitssklave::Gehorsam::Machen { befehle, }) =>
                befehle,
            Ok(arbeitssklave::Gehorsam::Rasten) =>
                return Ok(()),
            Err(error) =>
                return Err(Error::ReceiveOrder(error)),
        };
        loop {
            match befehle.befehl() {
                arbeitssklave::SklavenBefehl::Mehr { befehl, mehr_befehle, } => {
                    befehle = mehr_befehle;
                    match befehl {
                        Order::InfoCancel(komm::UmschlagAbbrechen { .. }) =>
                            return Err(Error::GenServerIsLostOnRequestInfo),
                        Order::Info(komm::Umschlag { inhalt: info, stamp: reply_tx, }) =>
                            if let Err(_send_error) = reply_tx.send(info) {
                                log::debug!("client is gone during RequestInfo");
                            },
                        Order::InsertCancel(komm::UmschlagAbbrechen { .. }) =>
                            return Err(Error::GenServerIsLostOnRequestInsert),
                        Order::Insert(komm::Umschlag { inhalt: inserted, stamp: reply_tx, }) =>
                            if let Err(_send_error) = reply_tx.send(inserted) {
                                log::debug!("client is gone during RequestInsert");
                            },
                        Order::LookupRangeCancel(komm::UmschlagAbbrechen { .. }) =>
                            return Err(Error::GenServerIsLostOnRequestLookupRange),
                        Order::LookupRange(komm::Umschlag {
                            stamp: LookupKind::Single(LookupKindSingle {
                                reply_tx,
                                feedback_tx,
                            }),
                            inhalt: komm::Streamzeug::NichtMehr(mehr),
                        }) => {
                            if let Err(_send_error) = reply_tx.send(None) {
                                log::debug!("client is gone during RequestLookup (None)");
                            }
                            if let Err(_send_error) = feedback_tx.send(mehr.stream_id().clone()) {
                                return Err(Error::GenServerIsLostOnRequestLookupSingle);
                            }
                        },
                        Order::LookupRange(komm::Umschlag {
                            stamp: LookupKind::Single(LookupKindSingle {
                                reply_tx,
                                feedback_tx,
                            }),
                            inhalt: komm::Streamzeug::Zeug {
                                zeug: key_value_pair,
                                mehr,
                            },
                        }) => {
                            if let Err(_send_error) = reply_tx.send(Some(key_value_pair.value_cell)) {
                                log::debug!("client is gone during RequestLookup (Some)");
                            }
                            if let Err(_send_error) = feedback_tx.send(mehr.stream_id().clone()) {
                                return Err(Error::GenServerIsLostOnRequestLookupSingle);
                            }
                        },
                        Order::LookupRange(komm::Umschlag {
                            stamp: LookupKind::Range(LookupKindRange { kv_items_stream_tx, }),
                            inhalt: streamzeug,
                        }) =>
                            if let Err(_send_error) = kv_items_stream_tx.send(streamzeug) {
                                log::debug!("lookup range process is gone during RequestLookupRange");
                            },
                        Order::RemoveCancel(komm::UmschlagAbbrechen { .. }) =>
                            return Err(Error::GenServerIsLostOnRequestRemove),
                        Order::Remove(komm::Umschlag { inhalt: removed, stamp: reply_tx, }) =>
                            if let Err(_send_error) = reply_tx.send(removed) {
                                log::debug!("client is gone during RequestRemove");
                            },
                        Order::FlushCancel(komm::UmschlagAbbrechen { .. }) =>
                            return Err(Error::GenServerIsLostOnRequestFlush),
                        Order::Flushed(komm::Umschlag { inhalt: Flushed, stamp: reply_tx, }) =>
                            if let Err(_send_error) = reply_tx.send(Flushed) {
                                log::debug!("client is gone during RequestFlush");
                            },
                    }
                },
                arbeitssklave::SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                    sklave_job = next_sklave_job;
                    break;
                },
            }
        }
    }
}

impl From<komm::UmschlagAbbrechen<proto::RequestInfoReplyTx>> for Order {
    fn from(v: komm::UmschlagAbbrechen<proto::RequestInfoReplyTx>) -> Order {
        Order::InfoCancel(v)
    }
}

impl From<komm::Umschlag<Info, proto::RequestInfoReplyTx>> for Order {
    fn from(v: komm::Umschlag<Info, proto::RequestInfoReplyTx>) -> Order {
        Order::Info(v)
    }
}

impl From<komm::UmschlagAbbrechen<proto::RequestInsertReplyTx>> for Order {
    fn from(v: komm::UmschlagAbbrechen<proto::RequestInsertReplyTx>) -> Order {
        Order::InsertCancel(v)
    }
}

impl From<komm::Umschlag<Inserted, proto::RequestInsertReplyTx>> for Order {
    fn from(v: komm::Umschlag<Inserted, proto::RequestInsertReplyTx>) -> Order {
        Order::Insert(v)
    }
}

impl From<komm::UmschlagAbbrechen<LookupKind>> for Order {
    fn from(v: komm::UmschlagAbbrechen<LookupKind>) -> Order {
        Order::LookupRangeCancel(v)
    }
}

impl From<komm::Umschlag<komm::Streamzeug<kv::KeyValuePair<kv::Value>>, LookupKind>> for Order {
    fn from(v: komm::Umschlag<komm::Streamzeug<kv::KeyValuePair<kv::Value>>, LookupKind>) -> Order {
        Order::LookupRange(v)
    }
}

impl From<komm::UmschlagAbbrechen<proto::RequestRemoveReplyTx>> for Order {
    fn from(v: komm::UmschlagAbbrechen<proto::RequestRemoveReplyTx>) -> Order {
        Order::RemoveCancel(v)
    }
}

impl From<komm::Umschlag<Removed, proto::RequestRemoveReplyTx>> for Order {
    fn from(v: komm::Umschlag<Removed, proto::RequestRemoveReplyTx>) -> Order {
        Order::Remove(v)
    }
}

impl From<komm::UmschlagAbbrechen<proto::RequestFlushReplyTx>> for Order {
    fn from(v: komm::UmschlagAbbrechen<proto::RequestFlushReplyTx>) -> Order {
        Order::FlushCancel(v)
    }
}

impl From<komm::Umschlag<Flushed, proto::RequestFlushReplyTx>> for Order {
    fn from(v: komm::Umschlag<Flushed, proto::RequestFlushReplyTx>) -> Order {
        Order::Flushed(v)
    }
}
