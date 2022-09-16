use futures::{
    channel::{
        mpsc,
    },
    stream,
};

use alloc_pool::{
    bytes::{
        BytesPool,
    },
};

use ero::{
    supervisor::{
        SupervisorPid,
    },
};

use crate::{
    job,
    proto,
    wheels,
    version,
    Params,
};

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

    todo!()
}
