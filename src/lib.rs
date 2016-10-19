pub mod twoskip;

extern crate mmap;
extern crate byteorder;
extern crate crc;
extern crate num;

#[test]
fn it_works() {
  let ts = twoskip::open("mailboxes.db");
  ts.unwrap().dump().ok();
}
