// use actix_web::web;
// use err_context::AnyError;
// use futures::executor::BlockingStream;
// use futures::StreamExt;
// use log::trace;
// use std::io;
// use std::io::{Error, ErrorKind, Read, Write};
// use vmap::io::{Ring, SeqRead};
//
// pub struct AsyncBufReader {
//     input: BlockingStream<web::Payload>,
//     buffer: Ring,
// }
//
// impl AsyncBufReader {
//     pub fn new(payload: web::Payload) -> Result<AsyncBufReader, AnyError> {
//         let buffer = Ring::new(1_000_000)?;
//
//         Ok(AsyncBufReader {
//             input: futures::executor::block_on_stream(payload),
//             buffer,
//         })
//     }
// }
//
// impl Read for AsyncBufReader {
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//         trace!("PRE bytes available: {}", self.buffer.read_len());
//         if self.buffer.is_empty() {
//             if let Some(chunk) = self.input.next() {
//                 let chunk = chunk.map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
//                 let av = chunk.len();
//                 trace!("Copying chunk of {} bytes", av);
//                 let l = self.buffer.write(&chunk)?;
//                 if l < av {
//                     panic!("Buffer not big enough")
//                 }
//             } else {
//                 trace!("No more data to load")
//             }
//         }
//
//         trace!("POST bytes available: {}", self.buffer.read_len());
//
//         self.buffer.read(buf)
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use actix_http::PayloadStream;
//     use actix_web::test;
//     use actix_web::web::{Bytes, Payload};
//     use futures::prelude::*;
//
//     #[test]
//     fn test_async_buf_reader_simple() {
//         let original = Vec::from("ahoj");
//
//         let chunks = vec![Ok(Bytes::from(original.clone()))];
//         let payload = Payload(actix_http::Payload::from(
//             Box::pin(stream::iter(chunks.into_iter())) as PayloadStream
//         ));
//
//         let reader = AsyncBufReader::new(payload).unwrap();
//
//         let bytes: Vec<u8> = reader.bytes().filter_map(Result::ok).collect();
//
//         assert_eq!(bytes, original)
//     }
//
//     #[test]
//     fn test_async_buf_reader_multi_chunks() {
//         let _ = env_logger::try_init();
//
//         let original = vec![Vec::from("ahoj"), Vec::from("ahoj")];
//         let chunks = original.clone().into_iter().map(|p| Ok(Bytes::from(p)));
//
//         let payload = Payload(actix_http::Payload::from(
//             Box::pin(stream::iter(chunks.into_iter())) as PayloadStream
//         ));
//
//         let reader = AsyncBufReader::new(payload).unwrap();
//
//         let bytes: Vec<u8> = reader.bytes().filter_map(Result::ok).collect();
//
//         assert_eq!(bytes, original.into_iter().flatten().collect::<Vec<u8>>())
//     }
// }
