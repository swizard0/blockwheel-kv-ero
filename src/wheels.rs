use alloc_pool::{
    bytes::{
        BytesPool,
    },
};

use crate::{
    job,
    access_policy::{
        AccessPolicy,
    },
};

#[derive(Debug)]
pub enum Error {
    NoWheelsParams,
    Wheels(blockwheel_kv::wheels::BuilderError),
    BlockwheelFsVersklaven(blockwheel_fs::Error),
}

pub struct WheelRef {
    pub blockwheel_filename: blockwheel_kv::wheels::WheelFilename,
    pub blockwheel_fs_params: blockwheel_fs::Params,
}

pub struct WheelsBuilder {
    wheels: Vec<WheelRef>,
}

pub struct Wheels {
    wheels: Vec<WheelRef>,
}

impl WheelsBuilder {
    pub fn new() -> Self {
        Self { wheels: Vec::new(), }
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
    pub(crate) fn create<P>(
        self,
        blocks_pool: &BytesPool,
        thread_pool: &P,
    )
        -> Result<blockwheel_kv::wheels::Wheels<AccessPolicy>, Error>
    where P: edeltraud::ThreadPool<job::Job> + Clone + Send + 'static
    {
        let mut wheels_builder = blockwheel_kv::wheels::WheelsBuilder::new();

        for WheelRef { blockwheel_filename, blockwheel_fs_params, } in self.wheels {
            let meister = blockwheel_fs::Freie::new()
                .versklaven(
                    blockwheel_fs_params,
                    blocks_pool.clone(),
                    &edeltraud::ThreadPoolMap::new(thread_pool.clone()),
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
