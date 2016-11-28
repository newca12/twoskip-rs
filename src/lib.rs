pub mod twoskip;

extern crate mmap;
extern crate byteorder;
extern crate crc;
extern crate num;

use std::str;

#[test]
fn it_works() {
  let ts = twoskip::open("mailboxes.db");
  //ts.unwrap().dump().ok();
  let db = ts.unwrap();
  //let rr = db.get(b"$RACL");
  let rr = db.get(b"DELETED.user.pinguser254.#calendars.11883388-c851-4304-a31d-ed696d96815e.5671CEC6").ok();
  let r = rr.unwrap().unwrap();
  println!("{}", r.dump());
  println!("{:?}", r.value());
  match str::from_utf8(r.value()) {
    Ok(s) => println!("value: {}", s),
    _     => (),
  };
}
