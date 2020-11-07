use std::io;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::mpsc;
use std::thread;

use actix_http::body::Body;
use actix_web::body::BodyStream;
use actix_web::{delete, error, get, post, put, web, HttpRequest, HttpResponse, Responder};
use futures::io::{BufReader, Error};
use futures::StreamExt;
use libcommon::structs::{ListResponse, ReadMetadataResponse, SharedLockResponse};
use log::*;
use once_cell::sync::Lazy;
use rdedup_lib::aio::{Backend, Local, Lock};
use serde::ser::{SerializeSeq, Serializer};
use serde::{Deserialize, Serialize};
use sgdata::SGData;
use sha2::*;
use uuid::Uuid;

mod blocking_writer;

const MAX_SIZE: usize = 1_000_000; // up to 1M chunks

static BACKEND: Lazy<Local> = Lazy::new(|| Local::new(PathBuf::from_str("/home/jenda/dev/rbackup2-poc/data").unwrap()));

#[derive(Debug, Deserialize)]
pub struct PathQuery {
    pub path: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct UnlockQuery {
    pub lock_id: Uuid,
}

#[get("/list")]
pub async fn list(query: web::Query<PathQuery>) -> impl Responder {
    trace!("list {:?}", *query);

    let mut thread = BACKEND.new_thread().unwrap();

    match thread.list(query.path.clone()) {
        Ok(result) => HttpResponse::Ok().json(ListResponse { paths: result }),
        Err(e) => {
            warn!("Error while listing path {:?}: {}", query.path, e);
            HttpResponse::InternalServerError().body(format!("Error: {:?}", e))
        }
    }
    .await
}

#[get("/list-recursively")]
pub async fn list_recursively(query: web::Query<PathQuery>) -> impl Responder {
    trace!("list recursively {:?}", *query);

    let mut thread = BACKEND.new_thread().unwrap();

    let (tx, rx) = mpsc::channel::<io::Result<Vec<PathBuf>>>();

    thread.list_recursively(query.path.clone(), tx);

    let (w, stream) = blocking_writer::create();

    thread::spawn(move || {
        let iter = rx.into_iter();
        let mut ser = serde_json::Serializer::new(w);

        for items in iter {
            if let Ok(paths) = items {
                ListResponse { paths }.serialize(&mut ser);
            }
        }
    });

    HttpResponse::Ok().body(BodyStream::new(stream))
}

#[get("/read-metadata")]
pub async fn read_metadata(query: web::Query<PathQuery>) -> impl Responder {
    trace!("read_metadata {:?}", *query);

    let mut thread = BACKEND.new_thread().unwrap();

    match thread.read_metadata(query.path.clone()) {
        Ok(result) => HttpResponse::Ok().json(ReadMetadataResponse {
            len: result._len,
            is_file: result._is_file,
        }),
        Err(e) if e.kind() == io::ErrorKind::NotFound => HttpResponse::NotFound().finish(),
        Err(e) => {
            warn!("Error while reading metadata for {:?}: {}", query.path, e);
            HttpResponse::InternalServerError().body(format!("Error: {:?}", e))
        }
    }
    .await
}

#[get("/read")]
pub async fn read(query: web::Query<PathQuery>) -> impl Responder {
    trace!("read {:?}", *query);

    let mut thread = BACKEND.new_thread().unwrap();

    match thread.read(query.path.clone()) {
        Ok(result) => HttpResponse::Ok().body(Body::from(result.to_linear_vec())), // TODO streaming?
        Err(e) => {
            warn!("Error while reading {:?}: {}", query.path, e);
            HttpResponse::InternalServerError().body(format!("Error: {:?}", e))
        }
    }
    .await
}

#[post("/write")]
pub async fn write(request: HttpRequest, mut payload: web::Payload) -> impl Responder {
    let headers = request.headers();
    let path = PathBuf::from_str(headers.get("path").unwrap().to_str().unwrap()).unwrap();
    let hash_reported = headers.get("hash").unwrap().to_str().unwrap();

    trace!("write {:?} {}", path, hash_reported);

    let mut body = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(error::ErrorPayloadTooLarge(format!(
                "Max {}B supported, {:?}B sent",
                MAX_SIZE,
                headers.get("content-length")
            )));
        }
        body.extend_from_slice(&chunk);
    }

    let bytes = body.to_vec();

    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let hash = hex::encode(&hasher.finalize());

    trace!(
        "Writing path {:?} length {}B hash {} reported hash {}",
        path,
        bytes.len(),
        hash,
        hash_reported
    );

    let mut thread = BACKEND.new_thread().unwrap();

    match thread.write(path.clone(), SGData::from_single(bytes), true) {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(e) => {
            warn!("Error while writing path {:?}: {}", path, e);
            HttpResponse::InternalServerError().body(format!("Error: {:?}", e))
        }
    }
    .await
}

#[put("/lock-shared")]
pub async fn lock_shared_add() -> impl Responder {
    trace!("lock shared add");

    match BACKEND.lock_shared() {
        Ok(_) => HttpResponse::Created().json(SharedLockResponse { lock_id: Uuid::new_v4() }),
        Err(e) => {
            warn!("Error while creating shared lock: {}", e);
            HttpResponse::InternalServerError().body(format!("Error: {:?}", e))
        }
    }
}

#[delete("/lock-shared")]
pub async fn lock_shared_remove(query: web::Query<UnlockQuery>) -> impl Responder {
    trace!("lock shared remove {:?}", *query);

    HttpResponse::Ok()
}
