#![feature(zero_one)]

pub mod twoskip;

extern crate mmap;
extern crate byteorder;
extern crate crc;

#[test]
fn it_works() {
  let ts = twoskip::open("mailboxes.db");
  ts.unwrap().dump().ok();
}
