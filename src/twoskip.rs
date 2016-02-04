use std::path::Path;
use std::fs::File;
use std::error::{Error as StdError};
use std::io;
use std::fmt;
use std::os::unix::io::IntoRawFd;
use std::slice;
use std::mem;
use std::ops::{Add,Sub,Rem};
use std::num::Zero;
use std::str;
use mmap;
use mmap::{MemoryMap, MapOption};
use byteorder::{BigEndian, ByteOrder};
use crc::crc32;

const MAX_LEVEL: u8 = 31;

enum RecordType {
  Dummy,
  Record,
  Delete,
  Commit,
}

impl From<u8> for RecordType {
  fn from(c: u8) -> RecordType {
    match c {
      b'=' => RecordType::Dummy,
      b'+' => RecordType::Record,
      b'-' => RecordType::Delete,
      b'$' => RecordType::Commit,
      _  => panic!("no conversion for RecordType from 0x{:0x}", c),
    }
  }
}

struct Record<'a> {
  db:         &'a Db,
  offset:     usize,
  len:        usize,
  typ:        RecordType,
  level:      u8,
  key_len:    usize,
  val_len:    usize,
  next_loc:   Vec<usize>,
  crc32_head: u32,
  crc32_tail: u32,
  key_offset: usize,
  val_offset: usize,
}

/*
struct Location {
  key:            String,
  is_exact_match: bool,
  record:         Record,
  back_loc:       Vec<usize>,
  forward_loc:    Vec<usize>,
  generation:     u64,
  end:            usize,
}
*/

const HEADER_MAGIC: &'static [u8; 20] = b"\xa1\x02\x8b\x0dtwoskip file\x00\x00\x00\x00";
const HEADER_SIZE: usize = 64;

const HEADER_VERSION: u32 = 1;

const OFFSET_HEADER:       usize = 0;
const OFFSET_VERSION:      usize = 20;
const OFFSET_GENERATION:   usize = 24;
const OFFSET_NUM_RECORDS:  usize = 32;
const OFFSET_REPACK_SIZE:  usize = 40;
const OFFSET_CURRENT_SIZE: usize = 48;
const OFFSET_FLAGS:        usize = 56;
const OFFSET_CRC32:        usize = 60;

const START_OFFSET: usize = HEADER_SIZE;

struct Header {
  version:      u32,
  flags:        u32, // XXX bitflags
  generation:   u64,
  num_records:  u64,
  repack_size:  usize,
  current_size: usize,
}

type Txn = usize;

pub struct Db {
  map:          MemoryMap,
  header:       Header,
/*
  loc:          Location,
  is_open:      bool,
  end:          usize,
  next_txn_num: usize,
  current_txn:  Txn,
*/
}

#[derive(Debug)]
pub enum Error {
  InvalidFileSize,
  InvalidHeaderMagic,
  VersionMismatch,
  ChecksumMismatch,
  InvalidLevel,
  InternalError(Box<StdError>),
}

impl StdError for Error {
  fn description(&self) -> &str {
    match *self {
      Error::InvalidFileSize    => "invalid file size",
      Error::InvalidHeaderMagic => "invalid header magic",
      Error::VersionMismatch    => "version mismatch",
      Error::ChecksumMismatch   => "checksum mismatch",
      Error::InvalidLevel       => "invalid level",
      Error::InternalError(_)   => "internal error",
    }
  }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}", match *self {
      Error::InternalError(ref err) => format!("{} ({})", self.description(), err.description()),
      ref e => e.description().to_string(),
    })
  }
}

impl From<io::Error> for Error {
  fn from(err: io::Error) -> Error {
    Error::InternalError(Box::new(err))
  }
}

impl From<mmap::MapError> for Error {
  fn from(err: mmap::MapError) -> Error {
    Error::InternalError(Box::new(err))
  }
}

fn read_header(map: &MemoryMap) -> Result<Header,Error> {
  if map.len() < HEADER_SIZE {
    return Err(Error::InvalidFileSize);
  }

  let base = map.data();

  let magic = unsafe { slice::from_raw_parts(base.offset(OFFSET_HEADER as isize), HEADER_MAGIC.len()) };
  if magic != HEADER_MAGIC {
    return Err(Error::InvalidHeaderMagic);
  }

  let version = BigEndian::read_u32(unsafe { slice::from_raw_parts(base.offset(OFFSET_VERSION as isize), mem::size_of::<u32>()) });
  if version != HEADER_VERSION {
    return Err(Error::VersionMismatch);
  }

  let generation   = BigEndian::read_u64(unsafe { slice::from_raw_parts(base.offset(OFFSET_GENERATION as isize),   mem::size_of::<u64>()) });
  let num_records  = BigEndian::read_u64(unsafe { slice::from_raw_parts(base.offset(OFFSET_NUM_RECORDS as isize),  mem::size_of::<u64>()) });
  let repack_size  = BigEndian::read_u64(unsafe { slice::from_raw_parts(base.offset(OFFSET_REPACK_SIZE as isize),  mem::size_of::<u64>()) }) as usize;
  let current_size = BigEndian::read_u64(unsafe { slice::from_raw_parts(base.offset(OFFSET_CURRENT_SIZE as isize), mem::size_of::<u64>()) }) as usize;

  // XXX flags

  let crc = BigEndian::read_u32(unsafe { slice::from_raw_parts(base.offset(OFFSET_CRC32 as isize), mem::size_of::<u32>()) });
  if crc != crc32::checksum_ieee(unsafe { slice::from_raw_parts(base, OFFSET_CRC32 as usize) }) {
    return Err(Error::ChecksumMismatch);
  }

  let header = Header {
    version:      version,
    flags:        0,
    generation:   generation,
    num_records:  num_records,
    repack_size:  repack_size,
    current_size: current_size,
  };

  Ok(header)
}

pub fn open<P: AsRef<Path>>(path: P) -> Result<Db,Error> {
  let f = try!(File::open(path));

  let md = try!(f.metadata());
  let len = md.len() as usize;
  let fd = f.into_raw_fd();

  let map = try!(MemoryMap::new(len, &[MapOption::MapReadable, MapOption::MapFd(fd)]));

  let header = try!(read_header(&map));

  let db = Db {
    map:  map,
    header: header,
  };

  Ok(db)
}

fn round_up<T>(n: T, to: T) -> T where T: Add<Output=T> + Sub<Output=T> + Rem<Output=T> + Zero + PartialEq + Copy {
  let r = n % to;
  match r == T::zero() {
    true  => n,
    false => n + to - r,
  }
}

impl Db {
  pub fn dump(&self) -> Result<(),Error> {
    println!("HEADER: v={version} fl={flags:x} num={num_records} sz={current_size:08x}/{repack_size:08x}",
      version      = self.header.version,
      flags        = self.header.flags,
      num_records  = self.header.num_records,
      current_size = self.header.current_size,
      repack_size  = self.header.repack_size,
    );

    let mut offset = START_OFFSET;
    while offset < self.header.current_size {
      let r = try!(self.record_at(offset));
      offset += r.len;
      println!("{:08x} {}", offset, r.dump());
    }

    Ok(())
  }

  fn record_at<'a>(&'a self, offset: usize) -> Result<Record,Error> {
    let base: *mut u8 = self.map.data();

    let mut next = offset;

    // XXX consts or sizeofs or whatever through here

    let raw_type = unsafe { *(base.offset(next as isize)) }; next += 1;
    let level  = unsafe { *(base.offset(next as isize)) }; next += 1;
    if level > MAX_LEVEL {
      return Err(Error::InvalidLevel);
    }

    let mut key_len = BigEndian::read_u16(unsafe { slice::from_raw_parts(base.offset(next as isize), mem::size_of::<u16>()) }) as usize; next += mem::size_of::<u16>();
    let mut val_len = BigEndian::read_u32(unsafe { slice::from_raw_parts(base.offset(next as isize), mem::size_of::<u32>()) }) as usize; next += mem::size_of::<u32>();

    if key_len == u16::max_value() as usize {
      key_len = BigEndian::read_u64(unsafe { slice::from_raw_parts(base.offset(next as isize), mem::size_of::<u64>()) }) as usize;
      next += mem::size_of::<u64>();
    }

    if val_len == u32::max_value() as usize {
      val_len = BigEndian::read_u64(unsafe { slice::from_raw_parts(base.offset(next as isize), mem::size_of::<u64>()) }) as usize;
      next += mem::size_of::<u64>();
    }

    let len =
      (next - offset) +               // header including lengths
      8 * (level+1) as usize +        // ptrs
      8 +                             // crc32s
      round_up(key_len + val_len, 8); // key/val

    if offset + len > self.map.len() {
      return Err(Error::InvalidFileSize);
    }

    let mut next_loc: Vec<usize> = vec!();
    for i in 0..level+1 {
      next_loc.push(BigEndian::read_u64(unsafe { slice::from_raw_parts(base.offset(next as isize), mem::size_of::<u64>()) }) as usize);
      next += mem::size_of::<u64>();
    }

    let crc32_head = BigEndian::read_u32(unsafe { slice::from_raw_parts(base.offset(next as isize), mem::size_of::<u32>()) });
    if crc32_head != crc32::checksum_ieee(unsafe { slice::from_raw_parts(base.offset(offset as isize), next-offset) }) {
      return Err(Error::ChecksumMismatch);
    }
    next += mem::size_of::<u32>();

    let crc32_tail = BigEndian::read_u32(unsafe { slice::from_raw_parts(base.offset(next as isize), mem::size_of::<u32>()) }); next += mem::size_of::<u32>();

    let key_offset = next;
    let val_offset = next + key_len;

    let r = Record {
      db:         self,
      offset:     offset,
      len:        len,
      typ:        RecordType::from(raw_type),
      level:      level,
      key_len:    key_len,
      val_len:    val_len,
      next_loc:   next_loc,
      crc32_head: crc32_head,
      crc32_tail: crc32_tail,
      key_offset: key_offset,
      val_offset: val_offset,
    };

    Ok(r)
  }
}

impl<'a> Record<'a> {
  fn key(&self) -> &[u8] {
    let base: *mut u8 = self.db.map.data();
    unsafe { slice::from_raw_parts(base.offset(self.key_offset as isize), self.key_len) }
  }

  fn format_data_record(&self, name: &str) -> String {
    format!("{name} kl={key_len:08x} dl={val_len:08x} lvl={level} ({key})\n    {next_loc}",
      name  = name,
      key_len = self.key_len,
      val_len = self.val_len,
      level   = self.level,
      key     = match str::from_utf8(self.key()) {
        Ok(s)  => s,
        Err(_) => "[Utf8Error]",
      },
      next_loc = self.format_next_loc(),
    )
  }

  fn format_next_loc(&self) -> String {
    (0..self.level+1).
      map(|l| self.next_loc[l as usize]).
      map(|loc| format!("{:08x}", loc)).
      collect::<Vec<String>>().
        chunks(8).
        map(|v| v.join(" ")).
        collect::<Vec<String>>().
          join("\n    ")
  }

  pub fn dump(&self) -> String {
    match self.typ {
      RecordType::Dummy  => self.format_data_record("DUMMY"),
      RecordType::Record => self.format_data_record("RECORD"),

      RecordType::Delete =>
        format!("DELETE ptr={next_loc:08x}",
          next_loc = self.next_loc[0],
        ),

      RecordType::Commit =>
        format!("COMMIT start={next_loc:08x}",
          next_loc = self.next_loc[0],
        ),
    }
  }
}
