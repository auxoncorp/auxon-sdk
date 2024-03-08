use crate::{capi_result, Error, NullPtrExt};
use std::ffi::{c_char, c_int, CStr};
use uuid::Uuid;

#[repr(C)]
pub struct timeline_id([u8; 16]);

impl From<&timeline_id> for auxon_sdk::api::TimelineId {
    fn from(tid: &timeline_id) -> Self {
        Uuid::from_bytes(tid.0).into()
    }
}

#[no_mangle]
pub extern "C" fn modality_timeline_id_init(tid: *mut timeline_id) -> c_int {
    capi_result(|| unsafe {
        let tid = tid.as_mut().ok_or(Error::NullPointer)?;
        let new_tid = auxon_sdk::api::TimelineId::allocate();
        tid.0.copy_from_slice(new_tid.get_raw().as_bytes());
        Ok(())
    })
}

#[repr(C)]
pub struct big_int([u8; 16]);

impl big_int {
    pub(crate) fn as_i128(&self) -> i128 {
        i128::from_le_bytes(self.0)
    }

    pub(crate) fn get(&self) -> (u64, u64) {
        let mut lsb = [0_u8; 8];
        let mut msb = [0_u8; 8];
        lsb[0..8].copy_from_slice(&self.0[0..8]);
        msb[0..8].copy_from_slice(&self.0[8..16]);
        (u64::from_le_bytes(lsb), u64::from_le_bytes(msb))
    }

    pub(crate) fn set(&mut self, lower: u64, upper: u64) {
        let lsb = lower.to_le_bytes();
        let msb = upper.to_le_bytes();
        (self.0[..8]).copy_from_slice(&lsb);
        (self.0[8..16]).copy_from_slice(&msb);
    }
}

/// Sets either the lower 64 bits, the upper 64 bits, or both.
/// Either can be set to NULL to be ignored.
#[no_mangle]
pub extern "C" fn modality_big_int_get(
    bi: *const big_int,
    lower: *mut u64,
    upper: *mut u64,
) -> c_int {
    capi_result(|| unsafe {
        let bi = bi.as_ref().ok_or(Error::NullPointer)?;
        let (lo, hi) = bi.get();
        if let Some(lsb) = lower.as_mut() {
            *lsb = lo;
        };
        if let Some(msb) = upper.as_mut() {
            *msb = hi;
        };
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_big_int_set(bi: *mut big_int, lower: u64, upper: u64) -> c_int {
    capi_result(|| unsafe {
        let bi = bi.as_mut().ok_or(Error::NullPointer)?;
        bi.set(lower, upper);
        Ok(())
    })
}

#[repr(C)]
pub struct logical_time([u64; 4]);

impl logical_time {
    pub(crate) fn set_unary(&mut self, a: u64) {
        self.0 = [0, 0, 0, a];
    }

    pub(crate) fn set_binary(&mut self, a: u64, b: u64) {
        self.0 = [0, 0, a, b];
    }

    pub(crate) fn set_trinary(&mut self, a: u64, b: u64, c: u64) {
        self.0 = [0, a, b, c];
    }

    pub(crate) fn set_quaternary(&mut self, a: u64, b: u64, c: u64, d: u64) {
        self.0 = [a, b, c, d];
    }
}

impl From<&logical_time> for auxon_sdk::api::LogicalTime {
    fn from(lt: &logical_time) -> Self {
        auxon_sdk::api::LogicalTime::quaternary(lt.0[0], lt.0[1], lt.0[2], lt.0[3])
    }
}

#[no_mangle]
pub extern "C" fn modality_logical_time_set_unary(lt: *mut logical_time, a: u64) -> c_int {
    capi_result(|| unsafe {
        let lt = lt.as_mut().ok_or(Error::NullPointer)?;
        lt.set_unary(a);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_logical_time_set_binary(lt: *mut logical_time, a: u64, b: u64) -> c_int {
    capi_result(|| unsafe {
        let lt = lt.as_mut().ok_or(Error::NullPointer)?;
        lt.set_binary(a, b);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_logical_time_set_trinary(
    lt: *mut logical_time,
    a: u64,
    b: u64,
    c: u64,
) -> c_int {
    capi_result(|| unsafe {
        let lt = lt.as_mut().ok_or(Error::NullPointer)?;
        lt.set_trinary(a, b, c);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_logical_time_set_quaternary(
    lt: *mut logical_time,
    a: u64,
    b: u64,
    c: u64,
    d: u64,
) -> c_int {
    capi_result(|| unsafe {
        let lt = lt.as_mut().ok_or(Error::NullPointer)?;
        lt.set_quaternary(a, b, c, d);
        Ok(())
    })
}

#[repr(C)]
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum attr_val {
    TimelineId(*const timeline_id),
    String(*const c_char),
    Integer(i64),
    BigInt(*const big_int),
    Float(f64),
    Bool(bool),
    Timestamp(u64),
    LogicalTime(*const logical_time),
}

impl From<&attr_val> for auxon_sdk::api::AttrVal {
    fn from(val: &attr_val) -> Self {
        use attr_val::*;
        unsafe {
            match val {
                TimelineId(tid) => {
                    let tid = &*(*tid);
                    auxon_sdk::api::AttrVal::TimelineId(Box::new(tid.into()))
                }
                String(s) => CStr::from_ptr(*s).to_string_lossy().to_string().into(),
                Integer(i) => (*i).into(),
                BigInt(bi) => {
                    let bi = &*(*bi);
                    auxon_sdk::api::BigInt::new_attr_val(bi.as_i128())
                }
                Float(f) => (*f).into(),
                Bool(b) => (*b).into(),
                Timestamp(t) => auxon_sdk::api::Nanoseconds::from(*t).into(),
                LogicalTime(lt) => auxon_sdk::api::LogicalTime::from(&*(*lt)).into(),
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn modality_attr_val_set_timeline_id(
    attr: *mut attr_val,
    val: *const timeline_id,
) -> c_int {
    capi_result(|| unsafe {
        let attr = attr.as_mut().ok_or(Error::NullPointer)?;
        val.null_check()?;
        *attr = attr_val::TimelineId(val);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_attr_val_set_string(attr: *mut attr_val, val: *const c_char) -> c_int {
    capi_result(|| unsafe {
        let attr = attr.as_mut().ok_or(Error::NullPointer)?;
        val.null_check()?;
        let _valid_str = CStr::from_ptr(val)
            .to_str()
            .map_err(|_| Error::InvalidUtf8)?;
        *attr = attr_val::String(val);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_attr_val_set_integer(attr: *mut attr_val, val: i64) -> c_int {
    capi_result(|| unsafe {
        let attr = attr.as_mut().ok_or(Error::NullPointer)?;
        *attr = attr_val::Integer(val);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_attr_val_set_big_int(attr: *mut attr_val, val: *const big_int) -> c_int {
    capi_result(|| unsafe {
        let attr = attr.as_mut().ok_or(Error::NullPointer)?;
        val.null_check()?;
        *attr = attr_val::BigInt(val);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_attr_val_set_float(attr: *mut attr_val, val: f64) -> c_int {
    capi_result(|| unsafe {
        let attr = attr.as_mut().ok_or(Error::NullPointer)?;
        *attr = attr_val::Float(val);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_attr_val_set_bool(attr: *mut attr_val, val: bool) -> c_int {
    capi_result(|| unsafe {
        let attr = attr.as_mut().ok_or(Error::NullPointer)?;
        *attr = attr_val::Bool(val);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_attr_val_set_timestamp(attr: *mut attr_val, val: u64) -> c_int {
    capi_result(|| unsafe {
        let attr = attr.as_mut().ok_or(Error::NullPointer)?;
        *attr = attr_val::Timestamp(val);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_attr_val_set_logical_time(
    attr: *mut attr_val,
    val: *const logical_time,
) -> c_int {
    capi_result(|| unsafe {
        let attr = attr.as_mut().ok_or(Error::NullPointer)?;
        val.null_check()?;
        *attr = attr_val::LogicalTime(val);
        Ok(())
    })
}
