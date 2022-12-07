use crate::{
    echo_policy::{
        EchoPolicy,
    },
    ftd_sklave,
};

pub enum Job {
    BlockwheelKv(blockwheel_kv::job::Job<EchoPolicy>),
    BlockwheelFs(blockwheel_fs::job::Job<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>),
    FtdSklave(ftd_sklave::SklaveJob),
}

impl From<blockwheel_fs::job::Job<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>> for Job {
    fn from(job: blockwheel_fs::job::Job<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>) -> Self {
        Self::BlockwheelFs(job)
    }
}

impl From<blockwheel_fs::job::SklaveJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>> for Job {
    fn from(job: blockwheel_fs::job::SklaveJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<blockwheel_fs::job::BlockPrepareWriteJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>> for Job {
    fn from(job: blockwheel_fs::job::BlockPrepareWriteJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<blockwheel_fs::job::BlockPrepareDeleteJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>> for Job {
    fn from(job: blockwheel_fs::job::BlockPrepareDeleteJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<blockwheel_fs::job::BlockProcessReadJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>> for Job {
    fn from(job: blockwheel_fs::job::BlockProcessReadJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<blockwheel_kv::job::Job<EchoPolicy>> for Job {
    fn from(job: blockwheel_kv::job::Job<EchoPolicy>) -> Self {
        Self::BlockwheelKv(job)
    }
}

impl From<blockwheel_kv::job::FlushButcherSklaveJob<EchoPolicy>> for Job {
    fn from(job: blockwheel_kv::job::FlushButcherSklaveJob<EchoPolicy>) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<blockwheel_kv::job::LookupRangeMergeSklaveJob<EchoPolicy>> for Job {
    fn from(job: blockwheel_kv::job::LookupRangeMergeSklaveJob<EchoPolicy>) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<blockwheel_kv::job::MergeSearchTreesSklaveJob<EchoPolicy>> for Job {
    fn from(job: blockwheel_kv::job::MergeSearchTreesSklaveJob<EchoPolicy>) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<blockwheel_kv::job::DemolishSearchTreeSklaveJob<EchoPolicy>> for Job {
    fn from(job: blockwheel_kv::job::DemolishSearchTreeSklaveJob<EchoPolicy>) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<blockwheel_kv::job::PerformerSklaveJob<EchoPolicy>> for Job {
    fn from(job: blockwheel_kv::job::PerformerSklaveJob<EchoPolicy>) -> Self {
        Self::BlockwheelKv(job.into())
    }
}

impl From<ftd_sklave::SklaveJob> for Job {
    fn from(job: ftd_sklave::SklaveJob) -> Self {
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
where J: From<blockwheel_fs::job::SklaveJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>>,
      J: From<blockwheel_fs::job::BlockPrepareWriteJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>>,
      J: From<blockwheel_fs::job::BlockPrepareDeleteJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>>,
      J: From<blockwheel_fs::job::BlockProcessReadJob<blockwheel_kv::wheels::WheelEchoPolicy<EchoPolicy>>>,
      J: From<blockwheel_kv::job::FlushButcherSklaveJob<EchoPolicy>>,
      J: From<blockwheel_kv::job::LookupRangeMergeSklaveJob<EchoPolicy>>,
      J: From<blockwheel_kv::job::MergeSearchTreesSklaveJob<EchoPolicy>>,
      J: From<blockwheel_kv::job::DemolishSearchTreeSklaveJob<EchoPolicy>>,
      J: From<blockwheel_kv::job::PerformerSklaveJob<EchoPolicy>>,
      J: From<ftd_sklave::SklaveJob>,
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
