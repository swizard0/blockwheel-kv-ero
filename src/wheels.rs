use alloc_pool::{
    bytes::{
        BytesPool,
    },
};

use crate::{
    job,
    echo_policy::{
        EchoPolicy,
    },
};

pub use blockwheel_kv::{
    wheels::{
        WheelFilename,
    },
};

#[derive(Debug)]
pub enum Error {
    NoWheelsParams,
    Wheels(blockwheel_kv::wheels::BuilderError),
    BlockwheelFsVersklaven(blockwheel_fs::Error),
}

pub struct WheelRef {
    pub blockwheel_filename: WheelFilename,
    pub blockwheel_fs_params: blockwheel_fs::Params,
}

pub struct WheelsBuilder {
    wheels: Vec<WheelRef>,
}

pub struct Wheels {
    wheels: Vec<WheelRef>,
}

pub type WheelsJob = job::BlockwheelFsJob;

impl Default for WheelsBuilder {
    fn default() -> Self {
        Self { wheels: Vec::new(), }
    }
}

impl WheelsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_wheel_ref(&mut self, wheel_ref: WheelRef) -> &mut Self {
        self.wheels.push(wheel_ref);
        self
    }

    pub fn build(self) -> Result<Wheels, Error> {
        if self.wheels.is_empty() {
            return Err(Error::NoWheelsParams);
        }

        Ok(Wheels { wheels: self.wheels, })
    }
}

impl Wheels {
    pub(crate) fn create<J>(
        self,
        blocks_pool: &BytesPool,
        thread_pool: &edeltraud::Handle<J>,
    )
        -> Result<blockwheel_kv::wheels::Wheels<EchoPolicy>, Error>
    where J: From<job::BlockwheelFsSklaveJob>,
          J: Send + 'static,
    {
        let mut wheels_builder = blockwheel_kv::wheels::WheelsBuilder::new();

        for WheelRef { blockwheel_filename, blockwheel_fs_params, } in self.wheels {
            let meister =
                blockwheel_fs::Meister::versklaven(
                    blockwheel_fs_params,
                    blocks_pool.clone(),
                    thread_pool,
                )
                .map_err(Error::BlockwheelFsVersklaven)?;
            wheels_builder = wheels_builder
                .add_wheel_ref(blockwheel_kv::wheels::WheelRef {
                    blockwheel_filename,
                    meister,
                });
        }

        let wheels = wheels_builder
            .build()
            .map_err(Error::Wheels)?;
        Ok(wheels)
    }
}
