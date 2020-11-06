use url::Url;

use rdedup_lib::Backend;
use rdedup_lib::BackendThread;
use std::io;

pub struct RemoteBackend {
    server_url: Url
}

impl RemoteBackend {
    pub fn new(url: Url) -> RemoteBackend {
        RemoteBackend { server_url }
    }
}

impl Backend for RemoteBackend {
    fn lock_exclusive(&self) -> io::Result<Box<dyn Lock>> {
        unimplemented!()
    }

    fn lock_shared(&self) -> io::Result<Box<dyn Lock>> {
        unimplemented!()
    }

    fn new_thread(&self) -> io::Result<Box<dyn BackendThread>> {
        unimplemented!()
    }
}
