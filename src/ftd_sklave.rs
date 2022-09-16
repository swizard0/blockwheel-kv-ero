use arbeitssklave::{
    komm,
};

use crate::{
    job,
    proto,
    access_policy::{
        AccessPolicy,
    },
    Info,
    Inserted,
    Removed,
    Flushed,
};

pub type SklaveJob = arbeitssklave::SklaveJob<Welt, Order>;

pub enum Order {
    InfoCancel(komm::UmschlagAbbrechen<proto::RequestInfoReplyTx>),
    Info(komm::Umschlag<Info, proto::RequestInfoReplyTx>),
    InsertCancel(komm::UmschlagAbbrechen<proto::RequestInsertReplyTx>),
    Insert(komm::Umschlag<Inserted, proto::RequestInsertReplyTx>),
    LookupRangeCancel(komm::UmschlagAbbrechen<proto::RequestLookupKind>),
    LookupRange(komm::Umschlag<blockwheel_kv::KeyValueStreamItem<AccessPolicy>, proto::RequestLookupKind>),
    RemoveCancel(komm::UmschlagAbbrechen<proto::RequestRemoveReplyTx>),
    Remove(komm::Umschlag<Removed, proto::RequestRemoveReplyTx>),
    FlushCancel(komm::UmschlagAbbrechen<proto::RequestFlushReplyTx>),
    Flushed(komm::Umschlag<Flushed, proto::RequestFlushReplyTx>),
}

pub struct Welt;

pub fn job<P>(sklave_job: SklaveJob, thread_pool: &P) where P: edeltraud::ThreadPool<job::Job> {
    if let Err(error) = run_job(sklave_job, thread_pool) {
        log::error!("job terminated with error: {:?}", error);
    }
}

#[derive(Debug)]
pub enum Error {
}

fn run_job<P>(mut sklave_job: SklaveJob, _thread_pool: &P) -> Result<(), Error> where P: edeltraud::ThreadPool<job::Job> {

    todo!()
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

impl From<komm::UmschlagAbbrechen<proto::RequestLookupKind>> for Order {
    fn from(v: komm::UmschlagAbbrechen<proto::RequestLookupKind>) -> Order {
        Order::LookupRangeCancel(v)
    }
}

impl From<komm::Umschlag<blockwheel_kv::KeyValueStreamItem<AccessPolicy>, proto::RequestLookupKind>> for Order {
    fn from(v: komm::Umschlag<blockwheel_kv::KeyValueStreamItem<AccessPolicy>, proto::RequestLookupKind>) -> Order {
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
