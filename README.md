twoskip is a robust single-file KV store.

The original implementation of the on-disk format is in the [Cyrus mail server](http://cyrusimap.org/). This Rust version attempts to be compatible with the on-disk format, suitable for building tools to manipulate Cyrus data files.

More information about the format can be found at:

* C implementation in Cyrus: https://github.com/cyrusimap/cyrus-imapd/blob/master/lib/cyrusdb_twoskip.c
* Talk at YAPC::EU 2016 http://opera.brong.fastmail.fm.user.fm/talks/twoskip/twoskip-yapc12.pdf
* Talk at LCA 2016: https://www.youtube.com/watch?v=2XWUYPLUrSM

This code is unusable currently. It can dump database files and not much else.
