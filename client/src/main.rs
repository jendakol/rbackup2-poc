use std::io;

use err_context::AnyError;
use log::debug;
use rdedup_lib::backends::Backend;
use rdedup_lib::{PassphraseFn, Repo as RdedupRepo};

use crate::remote::RemoteBackend;

mod remote;

fn create_backend(u: &url1::Url) -> io::Result<Box<dyn Backend + Send + Sync>> {
    Ok(Box::new(RemoteBackend::new(url::Url::parse(&u.to_string()).unwrap())))
}

fn main() -> Result<(), AnyError> {
    env_logger::init();

    let passfn: PassphraseFn = &|| Ok("prdel".to_owned());

    // let repo = RdedupRepo::init_custom(
    //     &url1::Url::parse("http://localhost:8090")?,
    //     &backendfn,
    //     &passfn,
    //     rdedup_lib::settings::Repo::new(),
    //     None,
    // )?;

    let repo = RdedupRepo::open_custom(&url1::Url::parse("http://localhost:8090")?, &create_backend, None)?;

    let source = "/data/Fotky/A7III/DSC00383.ARW";
    // let source = "/data/Fotky/DSC27456.ARW";
    let dest = "filename4.dat";

    let wh = repo.unlock_encrypt(&passfn)?;
    let file = std::fs::File::open(source)?;
    let stats = repo.write(dest, &file, &wh)?;
    debug!("File {:?} stats {:?}", file, stats);

    let rh = repo.unlock_decrypt(&passfn)?;
    let mut file = std::fs::File::create("/tmp/file.dat")?;
    repo.read(dest, &mut file, &rh)?;

    let meta = file.metadata()?;
    println!("Meta: {:?}", meta);

    Ok(())
}
