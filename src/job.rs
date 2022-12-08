use crate::{
    echo_policy::{
        EchoPolicy,
    },
    ftd_sklave,
};

pub type BlockwheelFsJob =
    blockwheel_fs::job::Job<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>;
pub type BlockwheelFsSklaveJob =
    blockwheel_fs::job::SklaveJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>;
pub type BlockwheelFsBlockPrepareWriteJob =
    blockwheel_fs::job::BlockPrepareWriteJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>;
pub type BlockwheelFsBlockPrepareDeleteJob =
    blockwheel_fs::job::BlockPrepareDeleteJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>;
pub type BlockwheelFsBlockProcessReadJob =
    blockwheel_fs::job::BlockProcessReadJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>;

pub type BlockwheelKvJob =
    blockwheel_kv::job::Job<EchoPolicy>;
pub type BlockwheelKvFlushButcherSklaveJob =
    blockwheel_kv::job::FlushButcherSklaveJob<EchoPolicy>;
pub type BlockwheelKvLookupRangeMergeSklaveJob =
    blockwheel_kv::job::LookupRangeMergeSklaveJob<EchoPolicy>;
pub type BlockwheelKvMergeSearchTreesSklaveJob =
    blockwheel_kv::job::MergeSearchTreesSklaveJob<EchoPolicy>;
pub type BlockwheelKvDemolishSearchTreeSklaveJob =
    blockwheel_kv::job::DemolishSearchTreeSklaveJob<EchoPolicy>;
pub type BlockwheelKvPerformerSklaveJob =
    blockwheel_kv::job::PerformerSklaveJob<EchoPolicy>;
pub type FtdSklaveJob =
    ftd_sklave::SklaveJob;

pub enum Job {
    BlockwheelKv(BlockwheelKvJob),
    BlockwheelFs(BlockwheelFsJob),
    FtdSklave(FtdSklaveJob),
}

impl From<BlockwheelFsJob> for Job {
    fn from(job: BlockwheelFsJob) -> Self {
        Self::BlockwheelFs(job)
    }
}

impl From<BlockwheelFsSklaveJob> for Job {
    fn from(job: BlockwheelFsSklaveJob) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<BlockwheelFsBlockPrepareWriteJob> for Job {
    fn from(job: BlockwheelFsBlockPrepareWriteJob) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<BlockwheelFsBlockPrepareDeleteJob> for Job {
    fn from(job: BlockwheelFsBlockPrepareDeleteJob) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<BlockwheelFsBlockProcessReadJob> for Job {
    fn from(job: BlockwheelFsBlockProcessReadJob) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<BlockwheelKvJob> for Job {
    fn from(job: BlockwheelKvJob) -> Self {
        Self::BlockwheelKv(job)
    }
}

impl From<BlockwheelKvFlushButcherSklaveJob> for Job {
    fn from(job: BlockwheelKvFlushButcherSklaveJob) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<BlockwheelKvLookupRangeMergeSklaveJob> for Job {
    fn from(job: BlockwheelKvLookupRangeMergeSklaveJob) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<BlockwheelKvMergeSearchTreesSklaveJob> for Job {
    fn from(job: BlockwheelKvMergeSearchTreesSklaveJob) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<BlockwheelKvDemolishSearchTreeSklaveJob> for Job {
    fn from(job: BlockwheelKvDemolishSearchTreeSklaveJob) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<BlockwheelKvPerformerSklaveJob> for Job {
    fn from(job: BlockwheelKvPerformerSklaveJob) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<FtdSklaveJob> for Job {
    fn from(job: FtdSklaveJob) -> Self {
        Self::FtdSklave(job)
    }
}

pub struct JobUnit<J>(edeltraud::JobUnit<J, Job>);

impl<J> From<edeltraud::JobUnit<J, Job>> for JobUnit<J> {
    fn from(job_unit: edeltraud::JobUnit<J, Job>) -> Self {
        Self(job_unit)
    }
}

impl<J> edeltraud::Job for JobUnit<J>
where J: From<BlockwheelFsSklaveJob>,
      J: From<BlockwheelFsBlockPrepareWriteJob>,
      J: From<BlockwheelFsBlockPrepareDeleteJob>,
      J: From<BlockwheelFsBlockProcessReadJob>,
      J: From<BlockwheelKvFlushButcherSklaveJob>,
      J: From<BlockwheelKvLookupRangeMergeSklaveJob>,
      J: From<BlockwheelKvMergeSearchTreesSklaveJob>,
      J: From<BlockwheelKvDemolishSearchTreeSklaveJob>,
      J: From<BlockwheelKvPerformerSklaveJob>,
      J: From<FtdSklaveJob>,
      J: Send + 'static,
{
    fn run(self) {
        match self.0.job {
            Job::BlockwheelFs(job) => {
                let job_unit = blockwheel_fs::job::JobUnit::from(edeltraud::JobUnit {
                    handle: self.0.handle,
                    job,
                });
                job_unit.run();
            },
            Job::BlockwheelKv(job) => {
                let job_unit = blockwheel_kv::job::JobUnit::from(edeltraud::JobUnit {
                    handle: self.0.handle,
                    job,
                });
                job_unit.run();
            },
            Job::FtdSklave(job) => {
                ftd_sklave::job(job, &self.0.handle);
            },
        }
    }
}
