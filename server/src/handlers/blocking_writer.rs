use std::sync::mpsc;
use std::{io, thread};

use actix_web::web::Bytes;
use futures::{SinkExt, StreamExt};

pub fn create() -> (impl io::Write, impl futures::Stream<Item = Result<Bytes, ()>>) {
    let (tx, rx) = futures::channel::mpsc::channel::<Vec<u8>>(1024);
    let w = BlockingWriter(tx);

    (w, rx.map(Bytes::from).map(Ok::<Bytes, ()>))
}

struct BlockingWriter<T>(futures::channel::mpsc::Sender<T>);

impl<T> io::Write for BlockingWriter<T>
where
    T: for<'a> From<&'a [u8]> + Send + Sync + 'static,
{
    fn write(&mut self, d: &[u8]) -> io::Result<usize> {
        let len = d.len();

        futures::executor::block_on(self.0.send(d.into()))
            .map(|()| len)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    fn flush(&mut self) -> io::Result<()> {
        futures::executor::block_on(self.0.flush()).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}
