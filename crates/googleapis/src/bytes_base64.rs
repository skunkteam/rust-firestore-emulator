use base64::{display::Base64Display, prelude::BASE64_STANDARD_NO_PAD};
use bytes::Bytes;
use serde::Serializer;

pub(crate) fn serialize<S: Serializer>(v: &Bytes, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(&Base64Display::new(v, &BASE64_STANDARD_NO_PAD))
}
