//! Primitives used for reading the chunked data stored in the `Repo`
// {{{ use and mod
use std::cell::RefCell;
use std::collections::HashSet;
use std::io;
use std::io::Write;

use slog::{trace, warn, FnValue, Logger};

use crate::Generation;
use crate::VerifyResults;
use crate::{ArcCompression, ArcDecrypter};
use crate::{
    DataAddressRef, DataType, Digest, DigestRef, Error, Repo, DIGEST_SIZE,
};
// }}}

/// Translates index stream into data stream
///
/// This type implements `io::Write` and interprets what's written to it as a
/// stream of digests.
///
/// For every digest written to it, it will access the corresponding chunk and
/// write it into `writer` that it wraps.
struct IndexTranslator<'a, 'b> {
    writer: Option<&'b mut dyn Write>,
    digest_buf: Digest,
    data_type: DataType,
    read_context: &'a ReadContext<'a>,
    log: Logger,
}

impl<'a, 'b> IndexTranslator<'a, 'b> {
    pub(crate) fn new(
        writer: Option<&'b mut dyn Write>,
        data_type: DataType,
        read_context: &'a ReadContext<'a>,
        log: Logger,
    ) -> Self {
        IndexTranslator {
            data_type,
            digest_buf: Digest(Vec::with_capacity(DIGEST_SIZE)),
            read_context,
            writer,
            log,
        }
    }
}

impl<'a, 'b> Write for IndexTranslator<'a, 'b> {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
        assert!(!bytes.is_empty());

        let total_len = bytes.len();
        loop {
            let has_already = self.digest_buf.0.len();
            if (has_already + bytes.len()) < DIGEST_SIZE {
                self.digest_buf.0.extend_from_slice(bytes);

                trace!(
                    self.log,
                    "left with a buffer";
                    "digest" => FnValue(|_| hex::encode(&self.digest_buf.0)),
                );
                return Ok(total_len);
            }

            let &mut IndexTranslator {
                ref mut digest_buf,
                data_type,
                ref mut writer,
                read_context,
                ..
            } = self;
            let needs = DIGEST_SIZE - has_already;

            if digest_buf.0.is_empty() {
                let digest = &bytes[..needs];
                debug_assert_eq!(digest.len(), DIGEST_SIZE);
                bytes = &bytes[needs..];

                read_context.read_recursively(ReadRequest::new(
                    data_type,
                    DataAddressRef {
                        digest: DigestRef(digest),
                        index_level: 0,
                    },
                    writer.as_mut().map(|w| w as &mut dyn io::Write),
                    self.log.clone(),
                ))?;
            } else {
                digest_buf.0.extend_from_slice(&bytes[..needs]);
                debug_assert_eq!(digest_buf.0.len(), DIGEST_SIZE);
                bytes = &bytes[needs..];

                let res = read_context.read_recursively(ReadRequest::new(
                    data_type,
                    DataAddressRef {
                        digest: digest_buf.as_digest_ref(),
                        index_level: 0,
                    },
                    writer.as_mut().map(|w| w as &mut dyn io::Write),
                    self.log.clone(),
                ));
                digest_buf.0.clear();
                res?;
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a, 'b> Drop for IndexTranslator<'a, 'b> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            debug_assert_eq!(self.digest_buf.0.len(), 0);
        }
    }
}

/// Information specific to a given read operation
/// of a data in the Repo
pub(crate) struct ReadRequest<'a> {
    data_address: DataAddressRef<'a>,
    data_type: DataType,
    writer: Option<&'a mut dyn Write>,
    log: Logger,
}

impl<'a> ReadRequest<'a> {
    pub(crate) fn new(
        data_type: DataType,
        data_address: DataAddressRef<'a>,
        writer: Option<&'a mut dyn Write>,
        log: Logger,
    ) -> Self {
        ReadRequest {
            data_type,
            data_address,
            writer,
            log,
        }
    }
}

/// Read Context
///
/// Information about the `Repo` that is open for reaading
pub(crate) struct ReadContext<'a> {
    /// Writer to write the data to; `None` will discard the data
    accessor: &'a dyn ChunkAccessor,
}

impl<'a> ReadContext<'a> {
    pub(crate) fn new(accessor: &'a dyn ChunkAccessor) -> Self {
        ReadContext { accessor }
    }

    fn on_index(&self, mut req: ReadRequest<'_>) -> io::Result<()> {
        trace!(
            req.log,
            "Traversing index";
            "digest" => FnValue(|_| hex::encode(req.data_address.digest.0)),
        );

        let mut translator = IndexTranslator::new(
            req.writer.take(),
            req.data_type,
            self,
            req.log.clone(),
        );

        let da = DataAddressRef {
            digest: req.data_address.digest,
            index_level: req.data_address.index_level - 1,
        };
        let req = ReadRequest::new(
            DataType::Index,
            da,
            Some(&mut translator),
            req.log,
        );
        self.read_recursively(req)
    }

    fn on_data(&self, mut req: ReadRequest<'_>) -> io::Result<()> {
        trace!(
            req.log,
            "Traversing data";
            "digest" => FnValue(|_| hex::encode(req.data_address.digest.0)),
        );
        if let Some(writer) = req.writer.take() {
            self.accessor.read_chunk_into(
                req.data_address.digest,
                req.data_type,
                writer,
            )
        } else {
            self.accessor.touch(req.data_address.digest)
        }
    }

    pub(crate) fn read_recursively(
        &self,
        req: ReadRequest<'_>,
    ) -> io::Result<()> {
        trace!(
            req.log,
            "Reading recursively";
            "digest" => FnValue(|_| hex::encode(req.data_address.digest.0)),
        );

        if req.data_address.index_level == 0 {
            self.on_data(req)
        } else {
            self.on_index(req)
        }
    }
}

/// Abstraction over accessing chunks stored in the repository
pub(crate) trait ChunkAccessor {
    fn repo(&self) -> &Repo;

    /// Read a chunk identified by `digest` into `writer`
    fn read_chunk_into(
        &self,
        digest: DigestRef<'_>,
        data_type: DataType,
        writer: &mut dyn Write,
    ) -> io::Result<()>;

    fn touch(&self, _digest: DigestRef<'_>) -> io::Result<()>;
}

/// `ChunkAccessor` that just reads the chunks as requested, without doing
/// anything
pub(crate) struct DefaultChunkAccessor<'a> {
    repo: &'a Repo,
    decrypter: Option<ArcDecrypter>,
    compression: ArcCompression,
    gen_strings: Vec<String>,
}

impl<'a> DefaultChunkAccessor<'a> {
    pub(crate) fn new(
        repo: &'a Repo,
        decrypter: Option<ArcDecrypter>,
        compression: ArcCompression,
        generations: Vec<Generation>,
    ) -> Self {
        DefaultChunkAccessor {
            repo,
            decrypter,
            compression,
            gen_strings: generations.iter().map(|g| g.to_string()).collect(),
        }
    }
}

impl<'a> ChunkAccessor for DefaultChunkAccessor<'a> {
    fn repo(&self) -> &Repo {
        self.repo
    }

    fn read_chunk_into(
        &self,
        digest: DigestRef<'_>,
        data_type: DataType,
        writer: &mut dyn Write,
    ) -> io::Result<()> {
        let mut data = None;
        let cur_gen_str = self.gen_strings.last().unwrap();
        let mut data_gen_str = None;

        for gen_str in self.gen_strings.iter().rev() {
            let path = self.repo.chunk_rel_path_by_digest(digest, gen_str);
            match self.repo.aio.read(path).wait() {
                Ok(d) => {
                    data = Some(d);
                    data_gen_str = Some(gen_str);
                    break;
                }
                Err(_e) => {}
            }
        }

        if data.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Couldn't not find chunk: {}", hex::encode(digest.0),),
            ));
        }

        let data_gen_str = data_gen_str.unwrap();

        if cur_gen_str != data_gen_str {
            let data_gen_path =
                self.repo.chunk_rel_path_by_digest(digest, data_gen_str);
            let cur_gen_path =
                self.repo.chunk_rel_path_by_digest(digest, cur_gen_str);

            // `rename` is best effort
            //
            // Should we fail if we're GCing, and we want to make sure
            // everything reachable has been moved? Well, if it wa
            let res = self
                .repo
                .aio
                .rename(data_gen_path.clone(), cur_gen_path.clone())
                .wait();
            if let Err(e) = res {
                if e.kind() != io::ErrorKind::NotFound {
                    warn!(self.repo.log, "Couldn't move chunk to the current generation";
                          "src-path" => data_gen_path.display(),
                          "dst-path" => cur_gen_path.display(),
                          "err" => %e);
                    return Err(e);
                }
            }
        }

        let data = data.unwrap();
        let data = if data_type.should_encrypt() {
            self.decrypter
                .as_ref()
                .expect("Decrypter expected")
                .decrypt(data, digest.0)?
        } else {
            data
        };

        let data = if data_type.should_compress() {
            self.compression.decompress(data)?
        } else {
            data
        };

        let vec_result = self.repo.hasher.calculate_digest(&data);

        if vec_result != digest.0 {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} corrupted, data read: {}",
                    hex::encode(digest.0),
                    hex::encode(vec_result)
                ),
            ))
        } else {
            for part in data.as_parts() {
                writer.write_all(&*part)?;
            }
            Ok(())
        }
    }

    fn touch(&self, _digest: DigestRef<'_>) -> io::Result<()> {
        Ok(())
    }
}

/// `ChunkAccessor` that records which chunks
/// were accessed
///
/// This is useful for chunk garbage-collection
pub(crate) struct RecordingChunkAccessor<'a> {
    raw: DefaultChunkAccessor<'a>,
    accessed: RefCell<&'a mut HashSet<Vec<u8>>>,
}

impl<'a> RecordingChunkAccessor<'a> {
    pub(crate) fn new(
        repo: &'a Repo,
        accessed: &'a mut HashSet<Vec<u8>>,
        decrypter: Option<ArcDecrypter>,
        compression: ArcCompression,
        generations: Vec<Generation>,
    ) -> Self {
        RecordingChunkAccessor {
            raw: DefaultChunkAccessor::new(
                repo,
                decrypter,
                compression,
                generations,
            ),
            accessed: RefCell::new(accessed),
        }
    }
}

impl<'a> ChunkAccessor for RecordingChunkAccessor<'a> {
    fn repo(&self) -> &Repo {
        self.raw.repo()
    }

    fn read_chunk_into(
        &self,
        digest: DigestRef<'_>,
        data_type: DataType,
        writer: &mut dyn Write,
    ) -> io::Result<()> {
        self.touch(digest)?;
        self.raw.read_chunk_into(digest, data_type, writer)
    }

    fn touch(&self, digest: DigestRef<'_>) -> io::Result<()> {
        self.accessed.borrow_mut().insert(digest.0.into());
        Ok(())
    }
}

/// `ChunkAccessor` that verifies the chunks
/// that are accessed
///
/// This is used to verify a name / index
pub(crate) struct VerifyingChunkAccessor<'a> {
    raw: DefaultChunkAccessor<'a>,
    accessed: RefCell<HashSet<Vec<u8>>>,
    errors: RefCell<Vec<(Vec<u8>, Error)>>,
}

impl<'a> VerifyingChunkAccessor<'a> {
    pub(crate) fn new(
        repo: &'a Repo,
        decrypter: Option<ArcDecrypter>,
        compression: ArcCompression,
        generations: Vec<Generation>,
    ) -> Self {
        VerifyingChunkAccessor {
            raw: DefaultChunkAccessor::new(
                repo,
                decrypter,
                compression,
                generations,
            ),
            accessed: RefCell::new(HashSet::new()),
            errors: RefCell::new(Vec::new()),
        }
    }

    pub(crate) fn get_results(self) -> VerifyResults {
        VerifyResults {
            scanned: self.accessed.borrow().len(),
            errors: self.errors.into_inner(),
        }
    }
}

impl<'a> ChunkAccessor for VerifyingChunkAccessor<'a> {
    fn repo(&self) -> &Repo {
        self.raw.repo()
    }

    fn read_chunk_into(
        &self,
        digest: DigestRef<'_>,
        data_type: DataType,
        writer: &mut dyn Write,
    ) -> io::Result<()> {
        {
            let mut accessed = self.accessed.borrow_mut();
            if accessed.contains(digest.0) {
                return Ok(());
            }
            accessed.insert(digest.0.into());
        }
        let res = self.raw.read_chunk_into(digest, data_type, writer);

        if res.is_err() {
            self.errors
                .borrow_mut()
                .push((digest.0.into(), res.err().unwrap()));
        }
        Ok(())
    }

    fn touch(&self, digest: DigestRef<'_>) -> io::Result<()> {
        self.raw.touch(digest)
    }
}

/// `ChunkAccessor` that update accessed chunks
/// to the latest generation
pub(crate) struct GenerationUpdateChunkAccessor<'a> {
    raw: DefaultChunkAccessor<'a>,
}

impl<'a> GenerationUpdateChunkAccessor<'a> {
    pub(crate) fn new(
        repo: &'a Repo,
        compression: ArcCompression,
        generations: Vec<Generation>,
    ) -> Self {
        GenerationUpdateChunkAccessor {
            raw: DefaultChunkAccessor::new(
                repo,
                None,
                compression,
                generations,
            ),
        }
    }
}

impl<'a> ChunkAccessor for GenerationUpdateChunkAccessor<'a> {
    fn repo(&self) -> &Repo {
        self.raw.repo()
    }

    fn read_chunk_into(
        &self,
        digest: DigestRef<'_>,
        data_type: DataType,
        writer: &mut dyn Write,
    ) -> io::Result<()> {
        self.raw.read_chunk_into(digest, data_type, writer)
    }

    fn touch(&self, digest: DigestRef<'_>) -> io::Result<()> {
        let cur_gen_str = self.raw.gen_strings.last().unwrap();
        let mut data_gen_str = None;

        for gen_str in self.raw.gen_strings.iter().rev() {
            let path = self.raw.repo.chunk_rel_path_by_digest(digest, gen_str);
            match self.raw.repo.aio.read_metadata(path).wait() {
                Ok(_metadata) => {
                    data_gen_str = Some(gen_str);
                    break;
                }
                Err(_e) => {}
            }
        }

        if data_gen_str.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Couldn't not find chunk: {}", hex::encode(digest.0),),
            ));
        }

        let data_gen_str = data_gen_str.unwrap();

        if cur_gen_str != data_gen_str {
            let data_gen_path =
                self.raw.repo.chunk_rel_path_by_digest(digest, data_gen_str);
            let cur_gen_path =
                self.raw.repo.chunk_rel_path_by_digest(digest, cur_gen_str);

            // `rename` is best effort
            let res = self
                .raw
                .repo
                .aio
                .rename(data_gen_path.clone(), cur_gen_path.clone())
                .wait();
            if let Err(e) = res {
                if e.kind() != io::ErrorKind::NotFound {
                    warn!(self.raw.repo.log, "Couldn't move chunk to the current generation";
                          "src-path" => data_gen_path.display(),
                          "dst-path" => cur_gen_path.display(),
                          "err" => %e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}
// vim: foldmethod=marker foldmarker={{{,}}}
