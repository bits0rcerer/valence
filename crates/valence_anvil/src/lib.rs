use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::io::Read;
use tokio::{
    fs::{File, OpenOptions},
    io,
    io::{ErrorKind, AsyncSeekExt, AsyncReadExt, SeekFrom},
    sync::Mutex,
    time::Instant,
};
use std::path::PathBuf;
use std::time::Duration;

use flate2::bufread::{GzDecoder, ZlibDecoder};
use rand::prelude::ThreadRng;
use rand::Rng;
use thiserror::Error;
use tracing::warn;
#[cfg(feature = "valence")]
pub use to_valence::*;
use valence::prelude::ChunkPos;
use valence_nbt::Compound;

#[cfg(feature = "valence")]
mod to_valence;

type Regions = BTreeMap<(i32, i32), Region>;

#[derive(Debug)]
pub struct AnvilWorld {
    /// Path to the "region" subdirectory in the world root.
    region_root: PathBuf,
    /// Maps region (x, z) positions to region files.
    regions: Mutex<Regions>,
    /// Limit for open files,
    max_open_files: usize,
    /// Defines the duration after which a Region is seen as inactive
    region_retention: Duration,
}

#[derive(Clone, PartialEq, Debug)]
pub struct AnvilChunk {
    /// This chunk's NBT data.
    pub data: Compound,
    /// The time this chunk was last modified measured in seconds since the
    /// epoch.
    pub timestamp: u32,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ReadChunkError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Nbt(#[from] valence_nbt::Error),
    #[error("invalid chunk sector offset")]
    BadSectorOffset,
    #[error("invalid chunk size")]
    BadChunkSize,
    #[error("unknown compression scheme number of {0}")]
    UnknownCompressionScheme(u8),
    #[error("not all chunk NBT data was read")]
    IncompleteNbtRead,
}

#[derive(Debug)]
struct Region {
    file: File,
    last_use: Instant,
    /// The first 8 KiB in the file.
    header: [u8; SECTOR_SIZE * 2],
}

const SECTOR_SIZE: usize = 4096;

impl AnvilWorld {
    pub fn new(world_root: impl Into<PathBuf>, max_open_files: usize, region_retention: Duration) -> Self {
        assert!(max_open_files > 0);

        let mut region_root = world_root.into();
        region_root.push("region");

        Self {
            region_root,
            regions: Mutex::new(BTreeMap::new()),
            max_open_files,
            region_retention,
        }
    }

    pub async fn has_chunk(&self, pos: ChunkPos) -> Result<bool, ReadChunkError> {
        let mut regions = self.regions.lock().await;
        let region = match self.region(&mut regions, chunk_pos_to_region(pos)).await {
            Ok(Some(region)) => region,
            Ok(None) => return Ok(false),
            Err(ReadChunkError::Io(e)) if e.kind() == ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        };

        let chunk_idx = (pos.x.rem_euclid(32) + pos.x.rem_euclid(32) * 32) as usize;

        let location_bytes = (&region.header[chunk_idx * 4..]).read_u32().await?;
        Ok(location_bytes != 0)
    }

    /// Reads a chunk from the file system with the given chunk coordinates. If
    /// no chunk exists at the position, then `None` is returned.
    pub async fn read_chunk(
        &mut self,
        pos: ChunkPos,
    ) -> Result<Option<AnvilChunk>, ReadChunkError> {
        let mut regions = self.regions.lock().await;
        let region = match self.region(&mut regions, chunk_pos_to_region(pos)).await {
            Ok(Some(region)) => region,
            Ok(None) => return Ok(None),
            Err(ReadChunkError::Io(e)) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let chunk_idx = (pos.x.rem_euclid(32) + pos.z.rem_euclid(32) * 32) as usize;

        let location_bytes = (&region.header[chunk_idx * 4..]).read_u32().await?;
        let timestamp = (&region.header[chunk_idx * 4 + SECTOR_SIZE..]).read_u32().await?;

        if location_bytes == 0 {
            // No chunk exists at this position.
            return Ok(None);
        }

        let sector_offset = (location_bytes >> 8) as u64;
        let sector_count = (location_bytes & 0xff) as usize;

        if sector_offset < 2 {
            // If the sector offset was <2, then the chunk data would be inside the region
            // header. That doesn't make any sense.
            return Err(ReadChunkError::BadSectorOffset);
        }

        // Seek to the beginning of the chunk's data.
        region
            .file
            .seek(SeekFrom::Start(sector_offset * SECTOR_SIZE as u64)).await?;

        let exact_chunk_size = region.file.read_u32().await? as usize;

        if exact_chunk_size > sector_count * SECTOR_SIZE {
            // Sector size of this chunk must always be >= the exact size.
            return Err(ReadChunkError::BadChunkSize);
        }

        let mut data_buf = vec![0; exact_chunk_size].into_boxed_slice();
        region.file.read_exact(&mut data_buf).await?;

        let mut r = data_buf.as_ref();

        let mut decompress_buf = vec![];

        // What compression does the chunk use?
        let mut nbt_slice = match r.read_u8().await? {
            // GZip
            1 => {
                let mut z = GzDecoder::new(r);
                z.read_to_end(&mut decompress_buf)?;
                decompress_buf.as_slice()
            }
            // Zlib
            2 => {
                let mut z = ZlibDecoder::new(r);
                z.read_to_end(&mut decompress_buf)?;
                decompress_buf.as_slice()
            }
            // Uncompressed
            3 => r,
            // Unknown
            b => return Err(ReadChunkError::UnknownCompressionScheme(b)),
        };

        let (data, _) = valence_nbt::from_binary_slice(&mut nbt_slice)?;

        if !nbt_slice.is_empty() {
            return Err(ReadChunkError::IncompleteNbtRead);
        }

        Ok(Some(AnvilChunk { data, timestamp }))
    }

    async fn region<'a>(&self, regions: &'a mut Regions, region: (i32, i32)) -> Result<Option<&'a mut Region>, ReadChunkError> {
        if regions.len() >= self.max_open_files {
            regions.retain(|_, r| r.last_use.elapsed() < self.region_retention);

            if regions.len() >= self.max_open_files {
                warn!("reached open region files limit while all region are active");
                /*
                There are two possible options here.
                    1. Remove a random region.
                    2. Remove the closest to inactive region

                1. is fast
                2. might be more useful
                 */

                let mut rng = ThreadRng::default();
                let idx = rng.gen_range(0..=(regions.len()));
                let (key, _) = regions.iter()
                    .nth(idx)
                    .unwrap();

                let key = key.clone();
                regions.remove(&key);
            }
        }

        let region = match regions.entry(region) {
            Entry::Occupied(oe) => oe.into_mut(),
            Entry::Vacant(ve) => {
                // Load the region file if it exists. Otherwise, the chunk is considered absent.

                let path = self
                    .region_root
                    .join(format!("r.{}.{}.mca", region.0, region.1));

                let mut file = match OpenOptions::new().read(true).write(true).open(path).await {
                    Ok(file) => file,
                    Err(e) => return Err(e.into()),
                };

                let mut header = [0; SECTOR_SIZE * 2];

                file.read_exact(&mut header).await?;

                ve.insert(Region { file, header, last_use: Instant::now() })
            }
        };

        region.last_use = Instant::now();
        Ok(Some(region))
    }
}

fn chunk_pos_to_region(pos: ChunkPos) -> (i32, i32) {
    (pos.x.div_euclid(32), pos.z.div_euclid(32))
}