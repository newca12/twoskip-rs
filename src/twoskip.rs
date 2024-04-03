use byteorder::{BigEndian, ByteOrder};
use crc::Crc;
use memmap2::Mmap;
use num::Zero;
use std::cmp::Ordering;
use std::error::Error as StdError;
use std::fmt;
use std::fs::File;
use std::io;
use std::mem;
use std::ops::{Add, Rem, Sub};
use std::os::unix::io::IntoRawFd;
use std::path::Path;
use std::slice;

const CRC32: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

const MAX_LEVEL: u8 = 31;

#[derive(Debug)]
pub enum RecordType {
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
            _ => panic!("no conversion for RecordType from 0x{:0x}", c),
        }
    }
}

#[derive(Debug)]
pub struct Record<'a> {
    db: &'a Db,
    offset: usize,
    len: usize,
    pub typ: RecordType,
    level: u8,
    key_len: usize,
    val_len: usize,
    next_loc: Vec<usize>,
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

const HEADER_MAGIC: &[u8; 20] = b"\xa1\x02\x8b\x0dtwoskip file\x00\x00\x00\x00";
const HEADER_SIZE: usize = 64;

const HEADER_VERSION: u32 = 1;

const OFFSET_HEADER: usize = 0;
const OFFSET_VERSION: usize = 20;
const OFFSET_GENERATION: usize = 24;
const OFFSET_NUM_RECORDS: usize = 32;
const OFFSET_REPACK_SIZE: usize = 40;
const OFFSET_CURRENT_SIZE: usize = 48;
const OFFSET_FLAGS: usize = 56;
const OFFSET_CRC32: usize = 60;

const START_OFFSET: usize = HEADER_SIZE;

const BLANK: &[u8; 8] = b" BLANK\x07\xa0";

#[derive(Debug)]
struct Header {
    version: u32,
    flags: u32, // XXX bitflags
    generation: u64,
    num_records: u64,
    repack_size: usize,
    current_size: usize,
}

type Txn = usize;

#[derive(Debug)]
pub struct Db {
    map: Mmap,
    header: Header,
    /*
      loc:          Location,
      is_open:      bool,
      end:          usize,
      next_txn_num: usize,
      current_txn:  Txn,
    */
}

pub struct DbIter<'a> {
    db: &'a Db,
    offset: usize,
}

#[derive(Debug)]
pub enum Error {
    InvalidFileSize,
    InvalidHeaderMagic,
    VersionMismatch,
    ChecksumMismatch,
    InvalidLevel,
    InternalError(Box<dyn StdError>),
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self {
            Error::InvalidFileSize => "invalid file size",
            Error::InvalidHeaderMagic => "invalid header magic",
            Error::VersionMismatch => "version mismatch",
            Error::ChecksumMismatch => "checksum mismatch",
            Error::InvalidLevel => "invalid level",
            Error::InternalError(_) => "internal error",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                Error::InternalError(ref err) => format!("{:?} ({})", self, err),
                ref e => e.to_string(),
            }
        )
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::InternalError(Box::new(err))
    }
}

fn read_header(map: &Mmap) -> Result<Header, Error> {
    if map.len() < HEADER_SIZE {
        return Err(Error::InvalidFileSize);
    }

    let base = map.as_ptr(); //. .data();

    let magic = unsafe { slice::from_raw_parts(base.add(OFFSET_HEADER), HEADER_MAGIC.len()) };
    if magic != HEADER_MAGIC {
        return Err(Error::InvalidHeaderMagic);
    }

    let version = BigEndian::read_u32(unsafe {
        slice::from_raw_parts(base.add(OFFSET_VERSION), mem::size_of::<u32>())
    });
    if version != HEADER_VERSION {
        return Err(Error::VersionMismatch);
    }

    let generation = BigEndian::read_u64(unsafe {
        slice::from_raw_parts(base.add(OFFSET_GENERATION), mem::size_of::<u64>())
    });
    let num_records = BigEndian::read_u64(unsafe {
        slice::from_raw_parts(base.add(OFFSET_NUM_RECORDS), mem::size_of::<u64>())
    });
    let repack_size = BigEndian::read_u64(unsafe {
        slice::from_raw_parts(base.add(OFFSET_REPACK_SIZE), mem::size_of::<u64>())
    }) as usize;
    let current_size = BigEndian::read_u64(unsafe {
        slice::from_raw_parts(base.add(OFFSET_CURRENT_SIZE), mem::size_of::<u64>())
    }) as usize;

    // XXX flags

    let crc = BigEndian::read_u32(unsafe {
        slice::from_raw_parts(base.add(OFFSET_CRC32), mem::size_of::<u32>())
    });
    if crc != CRC32.checksum(unsafe { slice::from_raw_parts(base, OFFSET_CRC32) }) {
        return Err(Error::ChecksumMismatch);
    }

    let header = Header {
        version,
        flags: 0,
        generation,
        num_records,
        repack_size,
        current_size,
    };

    Ok(header)
}

pub fn open<P: AsRef<Path>>(path: P) -> Result<Db, Error> {
    let f = File::open(path)?;
    let fd = f.into_raw_fd();

    let map = unsafe { Mmap::map(fd)? };
    let header = read_header(&map)?;

    let db = Db { map, header };

    Ok(db)
}

fn round_up<T>(n: T, to: T) -> T
where
    T: Add<Output = T> + Sub<Output = T> + Rem<Output = T> + Zero + PartialEq + Copy,
{
    let r = n % to;
    match r == T::zero() {
        true => n,
        false => n + to - r,
    }
}

impl Db {
    pub fn get(&self, key: &[u8]) -> Result<Option<Record>, Error> {
        let mut r = self.record_at(START_OFFSET)?;
        let mut level = r.level;

        loop {
            println!("loop iter level {}", level);

            let mut offset = 0;
            while offset == 0 && level > 0 {
                offset = r.next_loc[level as usize];
                if offset == 0 {
                    level -= 1
                };
            }
            if level == 0 || offset == 0 {
                return Ok(None);
            }

            let next = self.record_at(offset)?;

            println!("next key {:?}", next.key());

            match key.cmp(next.key()) {
                Ordering::Equal => return Ok(Some(next)),
                Ordering::Less => {
                    level -= 1;
                    if level == 0 {
                        return Ok(None);
                    }
                }
                Ordering::Greater => {
                    r = next;
                    level = r.level;
                }
            };
        }
    }

    pub fn dump(&self) -> Result<(), Error> {
        println!("HEADER: v={version} fl={flags:x} num={num_records} sz=({current_size:08X}/{repack_size:08X})",
      version      = self.header.version,
      flags        = self.header.flags,
      num_records  = self.header.num_records,
      current_size = self.header.current_size,
      repack_size  = self.header.repack_size,
    );

        let mut offset = START_OFFSET;
        while offset < self.header.current_size {
            let maybe_blank =
                unsafe { slice::from_raw_parts(self.map.as_ptr().add(offset), BLANK.len()) };
            if maybe_blank == BLANK {
                println!("{:08X} BLANK", offset);
                offset += 8;
            } else {
                let r = self.record_at(offset)?;
                println!("{:08X} {}", offset, r.dump());
                offset += r.len;
            }
        }

        Ok(())
    }

    fn record_at(&self, offset: usize) -> Result<Record, Error> {
        let base = self.map.as_ptr();

        let mut next = offset;

        // XXX consts or sizeofs or whatever through here

        let raw_type = unsafe { *(base.add(next)) };
        next += 1;
        let level = unsafe { *(base.add(next)) };
        next += 1;
        if level > MAX_LEVEL {
            return Err(Error::InvalidLevel);
        }

        let mut key_len = BigEndian::read_u16(unsafe {
            slice::from_raw_parts(base.add(next), mem::size_of::<u16>())
        }) as usize;
        next += mem::size_of::<u16>();
        let mut val_len = BigEndian::read_u32(unsafe {
            slice::from_raw_parts(base.add(next), mem::size_of::<u32>())
        }) as usize;
        next += mem::size_of::<u32>();

        if key_len == u16::max_value() as usize {
            key_len = BigEndian::read_u64(unsafe {
                slice::from_raw_parts(base.add(next), mem::size_of::<u64>())
            }) as usize;
            next += mem::size_of::<u64>();
        }

        if val_len == u32::max_value() as usize {
            val_len = BigEndian::read_u64(unsafe {
                slice::from_raw_parts(base.add(next), mem::size_of::<u64>())
            }) as usize;
            next += mem::size_of::<u64>();
        }

        let len = (next - offset) +               // header including lengths
      8 * (level+1) as usize +        // ptrs
      8 +                             // crc32s
      round_up(key_len + val_len, 8); // key/val

        if offset + len > self.map.len() {
            return Err(Error::InvalidFileSize);
        }

        let mut next_loc: Vec<usize> = vec![];
        for _ in 0..level + 1 {
            next_loc.push(BigEndian::read_u64(unsafe {
                slice::from_raw_parts(base.add(next), mem::size_of::<u64>())
            }) as usize);
            next += mem::size_of::<u64>();
        }

        let crc32_head = BigEndian::read_u32(unsafe {
            slice::from_raw_parts(base.add(next), mem::size_of::<u32>())
        });
        if crc32_head
            != CRC32.checksum(unsafe { slice::from_raw_parts(base.add(offset), next - offset) })
        {
            return Err(Error::ChecksumMismatch);
        }
        next += mem::size_of::<u32>();

        let crc32_tail = BigEndian::read_u32(unsafe {
            slice::from_raw_parts(base.add(next), mem::size_of::<u32>())
        });
        next += mem::size_of::<u32>();

        let key_offset = next;
        let val_offset = next + key_len;

        let r = Record {
            db: self,
            offset,
            len,
            typ: RecordType::from(raw_type),
            level,
            key_len,
            val_len,
            next_loc,
            crc32_head,
            crc32_tail,
            key_offset,
            val_offset,
        };

        Ok(r)
    }

    pub fn iter(&self) -> DbIter<'_> {
        DbIter {
            db: self,
            offset: START_OFFSET,
        }
    }
}

impl<'a> Record<'a> {
    pub fn key(&self) -> &[u8] {
        let base = self.db.map.as_ptr();
        unsafe { slice::from_raw_parts(base.add(self.key_offset), self.key_len) }
    }

    pub fn value(&self) -> &[u8] {
        let base = self.db.map.as_ptr();
        unsafe { slice::from_raw_parts(base.add(self.val_offset), self.val_len) }
    }

    fn format_data_record(&self, name: &str) -> String {
        format!(
            "{name} kl={key_len} dl={val_len} lvl={level} ({key})\n\t{next_loc}",
            name = name,
            key_len = self.key_len,
            val_len = self.val_len,
            level = self.level,
            key = std::str::from_utf8(self.key()).unwrap_or("[Utf8Error]"),
            next_loc = self.format_next_loc(),
        )
    }

    fn format_next_loc(&self) -> String {
        let first = format!("{:08X}", self.next_loc[0]);
        let next = (1..self.level + 1)
            .map(|l| self.next_loc[l as usize])
            .map(|loc| format!("{:08X}", loc))
            .collect::<Vec<String>>()
            .chunks(8)
            .map(|v| v.join(" "))
            .collect::<Vec<String>>()
            .join(" \n\t");
        format!("{first} \n\t{next} ")
    }

    pub fn dump(&self) -> String {
        match self.typ {
            RecordType::Dummy => self.format_data_record("DUMMY"),
            RecordType::Record => self.format_data_record("RECORD"),

            RecordType::Delete => {
                format!("DELETE ptr={next_loc:08x}", next_loc = self.next_loc[0],)
            }

            RecordType::Commit => {
                format!("COMMIT start={next_loc:08x}", next_loc = self.next_loc[0],)
            }
        }
    }
}

impl<'a> Iterator for DbIter<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset < self.db.header.current_size {
            let maybe_blank = unsafe {
                slice::from_raw_parts(self.db.map.as_ptr().add(self.offset), BLANK.len())
            };
            if maybe_blank == BLANK {
                self.offset += 8;
                DbIter::next(self)
            } else {
                let r = self.db.record_at(self.offset).unwrap();
                self.offset += r.len;
                Some(r)
            }
        } else {
            None
        }
    }
}
