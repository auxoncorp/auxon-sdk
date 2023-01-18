use crate::Error;
use std::ffi::{c_char, CStr};

fn owned_cstr(ptr: *const c_char) -> Result<String, Error> {
    unsafe {
        CStr::from_ptr(ptr)
            .to_str()
            .map(|s| s.to_string())
            .map_err(|_| Error::InvalidUtf8)
    }
}

pub(crate) fn default_owned_cstr(ptr: *const c_char) -> Result<String, Error> {
    if ptr.is_null() {
        Ok(String::new())
    } else {
        owned_cstr(ptr)
    }
}

pub(crate) fn opt_owned_cstr(ptr: *const c_char) -> Result<Option<String>, Error> {
    if ptr.is_null() {
        Ok(None)
    } else {
        Ok(Some(owned_cstr(ptr)?))
    }
}

pub(crate) fn require_owned_cstr(ptr: *const c_char) -> Result<String, Error> {
    if ptr.is_null() {
        Err(Error::NullPointer)
    } else {
        Ok(owned_cstr(ptr)?)
    }
}
