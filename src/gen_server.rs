use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    stream::{
        self,
        FuturesUnordered,
    },
    select,
    SinkExt,
    StreamExt,
};

use alloc_pool::{
    bytes::{
        BytesPool,
    },
};

use arbeitssklave::{
    komm,
};

use ero::{
    restart,
    supervisor::{
        SupervisorPid,
    },
    ErrorSeverity,
    RestartStrategy,
};

use crate::{
    job,
    proto,
    wheels,
    version,
    ftd_sklave,
    access_policy::{
        AccessPolicy,
    },
    Params,
    LookupRange,
    KeyValueStreamItem,
};

#[derive(Debug)]
pub enum Error {
    Wheels(wheels::Error),
    BlockwheelKvVersklaven(blockwheel_kv::Error),
    FtdSendegeraetStarten(komm::Error),
    FtdVersklaven(arbeitssklave::Error),
    RequestInfoBefehl(arbeitssklave::Error),
    RequestInsertBefehl(arbeitssklave::Error),
    RequestRemoveBefehl(arbeitssklave::Error),
    RequestFlushBefehl(arbeitssklave::Error),
    LookupRangeNext(komm::Error),
    BlockwheelKvMeisterHasGoneDuringLookupRange,
}

pub async fn run<P>(
    fused_request_rx: stream::Fuse<mpsc::Receiver<proto::Request>>,
    parent_supervisor: SupervisorPid,
    params: Params,
    blocks_pool: BytesPool,
    version_provider: version::Provider,
    wheels: wheels::Wheels,
    thread_pool: P,
)
where P: edeltraud::ThreadPool<job::Job> + Clone + Send + 'static,
{
    let terminate_result =
        restart::restartable(
            ero::Params {
                name: "blockwheel_kv".to_string(),
                restart_strategy: RestartStrategy::InstantCrash,
            },
            State {
                parent_supervisor,
                params,
                blocks_pool,
                version_provider,
                wheels,
                thread_pool,
                fused_request_rx,
            },
            |mut state| async move {
                let child_supervisor_gen_server = state.parent_supervisor.child_supervisor();
                let child_supervisor_pid = child_supervisor_gen_server.pid();
                state.parent_supervisor.spawn_link_temporary(
                    child_supervisor_gen_server.run(),
                );
                busyloop_init(child_supervisor_pid, state).await
            },
        )
        .await;
    if let Err(error) = terminate_result {
        log::error!("fatal error: {:?}", error);
    }
}

struct State<P> {
    parent_supervisor: SupervisorPid,
    params: Params,
    blocks_pool: BytesPool,
    version_provider: version::Provider,
    wheels: wheels::Wheels,
    thread_pool: P,
    fused_request_rx: stream::Fuse<mpsc::Receiver<proto::Request>>,
}

impl<P> From<Error> for ErrorSeverity<State<P>, Error> {
    fn from(error: Error) -> Self {
        ErrorSeverity::Fatal(error)
    }
}

async fn busyloop_init<P>(supervisor_pid: SupervisorPid, state: State<P>) -> Result<(), ErrorSeverity<State<P>, Error>>
where P: edeltraud::ThreadPool<job::Job> + Clone + Send + 'static,
{
    let wheels = state.wheels
        .create(&state.blocks_pool, &state.thread_pool)
        .map_err(Error::Wheels)?;

    let blockwheel_kv_meister = blockwheel_kv::Freie::new()
        .versklaven(
            state.params,
            state.blocks_pool,
            state.version_provider,
            wheels,
            &edeltraud::ThreadPoolMap::new(state.thread_pool.clone()),
        )
        .map_err(Error::BlockwheelKvVersklaven)?;

    let ftd_sklave_freie = arbeitssklave::Freie::new();
    let ftd_sendegeraet = komm::Sendegeraet::starten(&ftd_sklave_freie, state.thread_pool.clone())
        .map_err(Error::FtdSendegeraetStarten)?;
    let _ftd_sklave_meister = ftd_sklave_freie
        .versklaven(ftd_sklave::Welt, &state.thread_pool)
        .map_err(Error::FtdVersklaven)?;

    busyloop(
        supervisor_pid,
        blockwheel_kv_meister,
        ftd_sendegeraet,
        state.fused_request_rx,
        state.thread_pool,
    ).await
}

async fn busyloop<P>(
    _supervisor_pid: SupervisorPid,
    blockwheel_kv_meister: blockwheel_kv::Meister<AccessPolicy>,
    ftd_sendegeraet: komm::Sendegeraet<ftd_sklave::Order>,
    mut fused_request_rx: stream::Fuse<mpsc::Receiver<proto::Request>>,
    thread_pool: P,
)
    -> Result<(), ErrorSeverity<State<P>, Error>>
where P: edeltraud::ThreadPool<job::Job> + Clone + Send + 'static,
{
    let mut lookup_tasks = FuturesUnordered::new();

    loop {
        enum Event<R, T> {
            Request(R),
            Task(T),
        }

        let event = select! {
            result = fused_request_rx.next() =>
                Event::Request(result),
            result = lookup_tasks.next() =>
                Event::Task(result.unwrap()),
        };

        match event {
            Event::Request(None) =>
                break,
            Event::Request(Some(proto::Request::Info(proto::RequestInfo { reply_tx, }))) => {
                blockwheel_kv_meister
                    .info(
                        ftd_sendegeraet.rueckkopplung(reply_tx),
                        &edeltraud::ThreadPoolMap::new(&thread_pool),
                    )
                    .map_err(Error::RequestInfoBefehl)?;
            },
            Event::Request(Some(proto::Request::Insert(proto::RequestInsert { key, value, reply_tx, }))) => {
                blockwheel_kv_meister
                    .insert(
                        key,
                        value,
                        ftd_sendegeraet.rueckkopplung(reply_tx),
                        &edeltraud::ThreadPoolMap::new(&thread_pool),
                    )
                    .map_err(Error::RequestInsertBefehl)?;
            },
            Event::Request(Some(
                proto::Request::LookupRange(
                    proto::RequestLookupKind::Single(
                        proto::RequestLookupKindSingle { key, reply_tx, },
                    ),
                ),
            )) => {
                blockwheel_kv_meister
                    .lookup_range(
                        key.clone() ..= key,
                        ftd_sendegeraet.rueckkopplung(
                            ftd_sklave::LookupKind::Single(
                                ftd_sklave::LookupKindSingle { reply_tx, },
                            ),
                        ),
                        &edeltraud::ThreadPoolMap::new(&thread_pool),
                    )
                    .map_err(Error::RequestInsertBefehl)?;
            },
            Event::Request(Some(
                proto::Request::LookupRange(proto::RequestLookupKind::Range(
                    proto::RequestLookupKindRange {
                        range_from,
                        range_to,
                        reply_tx,
                    },
                )),
            )) => {
                let blockwheel_kv_meister = blockwheel_kv_meister.clone();
                let ftd_sendegeraet = ftd_sendegeraet.clone();
                let thread_pool = thread_pool.clone();
                lookup_tasks.push(async move {
                    let (mut key_values_tx, key_values_rx) = mpsc::channel(0);
                    if let Err(_send_error) = reply_tx.send(LookupRange { key_values_rx, }) {
                        log::debug!("client has canceled lookup range request");
                        return Ok(());
                    }

                    let (mut kv_items_stream_tx, mut kv_items_stream_rx) = oneshot::channel();
                    blockwheel_kv_meister
                        .lookup_range(
                            (range_from, range_to),
                            ftd_sendegeraet.rueckkopplung(
                                ftd_sklave::LookupKind::Range(
                                    ftd_sklave::LookupKindRange { kv_items_stream_tx, },
                                ),
                            ),
                            &edeltraud::ThreadPoolMap::new(&thread_pool),
                        )
                        .map_err(Error::RequestInsertBefehl)?;
                    loop {
                        match kv_items_stream_rx.await {
                            Ok(blockwheel_kv::KeyValueStreamItem::KeyValue { key_value_pair, next, }) => {
                                if let Err(_send_error) = key_values_tx.send(KeyValueStreamItem::KeyValue(key_value_pair)).await {
                                    log::debug!("client has dropped kv items stream tx, canceling");
                                    return Ok(());
                                }
                                (kv_items_stream_tx, kv_items_stream_rx) = oneshot::channel();
                                next.next(
                                    ftd_sendegeraet.rueckkopplung(
                                        ftd_sklave::LookupKind::Range(
                                            ftd_sklave::LookupKindRange { kv_items_stream_tx, },
                                        ),
                                    ),
                                ).map_err(Error::LookupRangeNext)?;
                            },
                            Ok(blockwheel_kv::KeyValueStreamItem::NoMore) => {
                                if let Err(_send_error) = key_values_tx.send(KeyValueStreamItem::NoMore).await {
                                    log::debug!("client has dropped kv items stream tx, canceling");
                                }
                                return Ok(());
                            },
                            Err(oneshot::Canceled) =>
                                return Err(Error::BlockwheelKvMeisterHasGoneDuringLookupRange),
                        }
                    }
                });
            },
            Event::Request(Some(proto::Request::Remove(proto::RequestRemove { key, reply_tx, }))) => {
                blockwheel_kv_meister
                    .remove(
                        key,
                        ftd_sendegeraet.rueckkopplung(reply_tx),
                        &edeltraud::ThreadPoolMap::new(&thread_pool),
                    )
                    .map_err(Error::RequestRemoveBefehl)?;
            },
            Event::Request(Some(proto::Request::FlushAll(proto::RequestFlush { reply_tx, }))) => {
                blockwheel_kv_meister
                    .flush(
                        ftd_sendegeraet.rueckkopplung(reply_tx),
                        &edeltraud::ThreadPoolMap::new(&thread_pool),
                    )
                    .map_err(Error::RequestFlushBefehl)?;
            },
            Event::Task(Ok(())) =>
                (),
            Event::Task(Err(error)) =>
                return Err(ErrorSeverity::Fatal(error)),
        }
    }

    log::debug!("request channel is depleted: terminating busyloop");
    Ok(())
}
