use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use log::*;
use object_pool::{Pool, Reusable};
use once_cell::sync::Lazy;
use rdedup_lib::aio::{Backend, BackendThread, Local};

static BACKEND: Lazy<Arc<Local>> = Lazy::new(|| Arc::new(Local::new(PathBuf::from_str("/home/jenda/dev/rbackup2-poc/data").unwrap())));

static BACKEND_POOL: Lazy<Pool<PooledBackend>> = Lazy::new(|| {
    Pool::new(20, || {
        let backend = Arc::clone(&BACKEND);
        let thread = backend.new_thread().expect("Could not create new backend thread");

        PooledBackend { backend, thread }
    })
});

pub fn pull<'a>() -> Option<Reusable<'a, PooledBackend>> {
    BACKEND_POOL.try_pull().map(|mut b| {
        trace!("Borrowing pooled backend");
        b
    })
}

pub struct PooledBackend {
    pub backend: Arc<dyn Backend>,
    pub thread: Box<dyn BackendThread>,
}

impl Deref for PooledBackend {
    type Target = dyn Backend;

    fn deref(&self) -> &Self::Target {
        &*self.backend
    }
}
