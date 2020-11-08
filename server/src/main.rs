use std::net::SocketAddr;
use std::str::FromStr;

use actix_web::{App, HttpServer};
use log::*;

mod backend_pool;
mod handlers;

#[actix_rt::main]
async fn main() {
    env_logger::init();

    let addr = SocketAddr::from_str("0.0.0.0:8090").expect("Could not parse listen address!"); // let it fail

    info!("Starting server on {}", addr);

    HttpServer::new(move || {
        App::new()
            .service(handlers::list)
            .service(handlers::write)
            .service(handlers::read)
            .service(handlers::read_metadata)
            .service(handlers::lock_shared_add)
            .service(handlers::lock_shared_remove)
    })
    .bind(addr)
    .unwrap() // let it fail
    .run()
    .await
    .unwrap();
}
