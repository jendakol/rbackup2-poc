use err_context::AnyError;
use log::*;
use rdedup_lib::Repo as RdedupRepo;
use std::path::PathBuf;
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    env_logger::init();

    let passfn = || Ok("prdel".to_owned());

    // let repo = RdedupRepo::init(
    //     &url1::Url::parse("http://localhost:8090")?,
    //     &passfn,
    //     rdedup_lib::settings::Repo::new(),
    //     None,
    // )?;

    let repo = RdedupRepo::open(&url1::Url::parse("http://localhost:8090")?, None)?;

    let source = "/data/Fotky/A7III/DSC00382.ARW";
    // let source = "/data/Fotky/DSC27456.ARW";
    let dest = "filename3.dat";

    let wh = repo.unlock_encrypt(&passfn)?;
    let file = std::fs::File::open(source)?;
    let stats = repo.write(dest, &file, &wh)?;
    debug!("File {:?} stats {:?}", file, stats);

    let rh = repo.unlock_decrypt(&passfn)?;
    let mut file = std::fs::File::create("/tmp/file.dat")?;
    repo.read(dest, &mut file, &rh)?;

    let meta = file.metadata()?;
    println!("Meta: {:?}", meta);

    // let iter = repo.aio.list_recursively(PathBuf::from_str(".").unwrap());
    //
    // for item in iter {
    //     trace!("Recursive listed: {:?}", item)
    // }

    Ok(())
}
