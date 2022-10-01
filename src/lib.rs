pub mod twoskip;

#[test]
fn it_works() {
    let ts = twoskip::open("mailboxes.db");
    //ts.unwrap().dump().ok();
    let db = ts.unwrap();
    //let rr = db.get(b"$RACL");
    let rr = db
        .get(b"DELETED.user.pinguser254.#calendars.11883388-c851-4304-a31d-ed696d96815e.5671CEC6")
        .ok();
    let r = rr.unwrap().unwrap();
    assert_eq!(r.value(), b"%(A %(pinguser254 lrswipkxtecdn admin lrswipkxtecdan anyone p) I 2eababff-a28e-40bc-b00c-00d6ff6ad10b P default T c V 1450299080 F 17365878007025498411 M 1450299078)");
}
