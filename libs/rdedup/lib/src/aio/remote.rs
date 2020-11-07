use std::io;
use std::io::{Cursor, Error, ErrorKind, Read};
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use err_context::AnyError;
use libcommon::structs::{ListResponse, ReadMetadataResponse, SharedLockResponse};
use log::*;
use once_cell::sync::Lazy;
use reqwest::blocking::{Body, Client};
use reqwest::StatusCode;
use sgdata::SGData;
use sha2::*;
use url2::Url;

use uuid::Uuid;

use crate::{Backend, BackendThread, Lock, Metadata};

static CLIENT: Lazy<Client> = Lazy::new(|| Client::builder().connection_verbose(false).build().unwrap());

pub struct RemoteBackend {
    inner: Arc<RemoteBackendInner>,
}

pub struct RemoteBackendInner {
    server_url: Url,
}

pub struct RemoteLock {
    id: Uuid,
    backend: Arc<RemoteBackendInner>,
}

impl Drop for RemoteLock {
    fn drop(&mut self) {
        trace!("Dropping RemoteLock");

        let mut url = self.backend.server_url.clone();
        url.set_path("lock-shared");
        url.query_pairs_mut().append_pair("lock_id", self.id.to_string().as_str());

        let resp = CLIENT.delete(url).send().expect("Could not drop RemoteLock");

        if resp.status() != StatusCode::OK {
            let status = resp.status();
            let body = resp.bytes().unwrap().to_vec();
            let body_str = std::str::from_utf8(body.as_slice());

            trace!("Could not remove remote lock: {:?} {:?}", status, body_str);
        }
    }
}

impl RemoteBackend {
    pub fn new(url: Url) -> RemoteBackend {
        RemoteBackend {
            inner: Arc::new(RemoteBackendInner { server_url: url }),
        }
    }
}

pub struct RemoteBackendThread {
    backend: Arc<RemoteBackendInner>,
}

impl Backend for RemoteBackend {
    fn lock_exclusive(&self) -> io::Result<Box<dyn Lock>> {
        unimplemented!()
    }

    fn lock_shared(&self) -> io::Result<Box<dyn Lock>> {
        trace!("Dropping RemoteLock");

        let mut url = self.inner.server_url.clone();
        url.set_path("lock-shared");

        let resp = CLIENT.put(url).send().expect("Could not drop RemoteLock");

        if resp.status() != StatusCode::CREATED {
            let status = resp.status();
            let body = resp.bytes().unwrap().to_vec();
            let body_str = std::str::from_utf8(body.as_slice());

            trace!("Could not create remote lock: {:?} {:?}", status, body_str);

            return Err(Error::new(ErrorKind::InvalidData, AnyError::from("Invalid response")));
        }

        let lr = resp.json::<SharedLockResponse>().unwrap();

        trace!("Created remote shared lock {}", lr.lock_id);

        Ok(Box::new(RemoteLock {
            id: lr.lock_id,
            backend: Arc::clone(&self.inner),
        }))
    }

    fn new_thread(&self) -> io::Result<Box<dyn BackendThread>> {
        Ok(Box::new(RemoteBackendThread {
            backend: Arc::clone(&self.inner),
        }))
    }
}

fn calculate_digest(sg: &SGData) -> Vec<u8> {
    let mut sha256 = sha2::Sha256::default();

    for sg_part in sg.as_parts() {
        sha256.update(sg_part);
    }

    let mut vec_result = vec![0u8; 32];
    vec_result.copy_from_slice(&sha256.finalize());

    vec_result
}

impl BackendThread for RemoteBackendThread {
    fn remove_dir_all(&mut self, path: PathBuf) -> io::Result<()> {
        unimplemented!()
    }

    fn rename(&mut self, src_path: PathBuf, dst_path: PathBuf) -> io::Result<()> {
        unimplemented!()
    }

    fn write(&mut self, path: PathBuf, sg: SGData, idempotent: bool) -> io::Result<()> {
        let hash = hex::encode(calculate_digest(&sg)); // TODO calculate streaming

        trace!("remote write: path={:?} hash={} len={}B idem={}", path, hash, sg.len(), idempotent);

        let mut url = self.backend.server_url.clone();
        url.set_path("write");

        let data = SGDataWrapper::new(sg);

        let resp = CLIENT
            .post(url)
            .header("path", path.to_str().unwrap())
            .header("hash", hash)
            .body(Body::new(data))
            .send()
            .map_err(|e| (Error::new(ErrorKind::BrokenPipe, AnyError::from("Invalid response"))))?;

        if resp.status() != StatusCode::OK {
            trace!("Received: {:?}", resp);
            trace!("Error: {:?}", std::str::from_utf8(resp.bytes().unwrap().to_vec().as_slice()));
            return Err(Error::new(ErrorKind::InvalidData, AnyError::from("Invalid response")));
        }

        Ok(())
    }

    fn read(&mut self, path: PathBuf) -> io::Result<SGData> {
        trace!("remote read: {:?}", path);

        let mut url = self.backend.server_url.clone();
        url.set_path("read");
        url.query_pairs_mut()
            .append_pair("path", path.to_str().expect("Invalid utf-8 path"));

        let resp = CLIENT
            .get(url)
            .send()
            .map_err(|e| (Error::new(ErrorKind::BrokenPipe, AnyError::from("Invalid response"))))?;

        if resp.status() != StatusCode::OK {
            trace!("Received: {:?}", resp);
            return Err(Error::new(ErrorKind::InvalidData, AnyError::from("Invalid response")));
        }

        Ok(SGData::from_single(resp.bytes().unwrap().to_vec()))
    }

    fn remove(&mut self, path: PathBuf) -> io::Result<()> {
        unimplemented!()
    }

    fn read_metadata(&mut self, path: PathBuf) -> io::Result<Metadata> {
        trace!("remote read metadata: {:?}", path);

        let mut url = self.backend.server_url.clone();
        url.set_path("read-metadata");
        url.query_pairs_mut()
            .append_pair("path", path.to_str().expect("Invalid utf-8 path"));

        let resp = CLIENT
            .get(url)
            .send()
            .map_err(|e| (Error::new(ErrorKind::BrokenPipe, AnyError::from("Invalid response"))))?;

        if resp.status() != StatusCode::OK && resp.status() != StatusCode::NOT_FOUND {
            trace!("Received: {:?}", resp);
        }

        match resp.status() {
            StatusCode::OK => {
                let rmr = resp.json::<ReadMetadataResponse>().unwrap();

                debug!("Received {:?}", rmr);

                Ok(Metadata {
                    _len: rmr.len,
                    _is_file: rmr.is_file,
                })
            }
            StatusCode::NOT_FOUND => Err(Error::new(ErrorKind::NotFound, AnyError::from("File not found"))),
            _ => Err(Error::new(ErrorKind::InvalidData, AnyError::from("Invalid response"))),
        }
    }

    fn list(&mut self, path: PathBuf) -> io::Result<Vec<PathBuf>> {
        trace!("remote list: {:?}", path);

        let mut url = self.backend.server_url.clone();
        url.set_path("list");
        url.query_pairs_mut()
            .append_pair("path", path.to_str().expect("Invalid utf-8 path"));

        let resp = CLIENT
            .get(url)
            .send()
            .map_err(|e| (Error::new(ErrorKind::BrokenPipe, AnyError::from("Invalid response"))))?;

        if resp.status() != StatusCode::OK {
            trace!("Received: {:?}", resp);
            return Err(Error::new(ErrorKind::InvalidData, AnyError::from("Invalid response")));
        }

        let lr = resp.json::<ListResponse>().unwrap();

        trace!("Received {:?}", lr);

        Ok(lr.paths)
    }

    fn list_recursively(&mut self, path: PathBuf, tx: Sender<io::Result<Vec<PathBuf>>>) {
        trace!("remote list: {:?}", path);

        let mut url = self.backend.server_url.clone();
        url.set_path("list-recursively");
        url.query_pairs_mut()
            .append_pair("path", path.to_str().expect("Invalid utf-8 path"));

        let resp = CLIENT
            .get(url)
            .send()
            .map_err(|e| (Error::new(ErrorKind::BrokenPipe, AnyError::from("Invalid response"))))
            .unwrap();

        if resp.status() != StatusCode::OK {
            trace!("Received: {:?}", resp);
        }

        let bytes = resp.bytes().unwrap();

        let stream = serde_json::Deserializer::from_slice(bytes.as_ref()).into_iter::<ListResponse>();

        for values in stream {
            tx.send(values.map(|r| r.paths).map_err(|e| Error::new(ErrorKind::InvalidData, e)));
        }
    }
}

impl Lock for RemoteLock {}

struct SGDataWrapper {
    data: Cursor<Vec<u8>>,
}

impl SGDataWrapper {
    pub fn new(data: SGData) -> SGDataWrapper {
        SGDataWrapper {
            data: Cursor::new(data.to_linear_vec()),
        }
    }
}

impl Read for SGDataWrapper {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }
}
