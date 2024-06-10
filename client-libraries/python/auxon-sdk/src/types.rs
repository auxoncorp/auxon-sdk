use auxon_sdk::api::{AttrVal, Nanoseconds, TimelineId};
use pyo3::{exceptions, prelude::*};

#[pyclass]
#[derive(Clone)]
pub struct EventCoordinate(auxon_sdk::api::EventCoordinate);

#[pymethods]
impl EventCoordinate {}

#[pyclass]
#[derive(Clone)]
pub struct LogicalTime(auxon_sdk::api::LogicalTime);

#[pymethods]
impl LogicalTime {}

pub(crate) fn py_any_to_attr_val(py: Bound<PyAny>) -> Result<AttrVal, PyErr> {
    // Check the most common types first
    if let Ok(i) = py.extract::<i64>() {
        return Ok(i.into());
    }

    if let Ok(f) = py.extract::<f64>() {
        return Ok(f.into());
    }

    if let Ok(s) = py.extract::<String>() {
        return Ok(s.into());
    }

    if let Ok(b) = py.extract::<bool>() {
        return Ok(b.into());
    }

    if let Ok(ts) = py.extract::<std::time::SystemTime>() {
        match ts.duration_since(std::time::SystemTime::UNIX_EPOCH) {
            Ok(dur) => {
                let ns = dur.as_nanos();
                if ns > u64::MAX as u128 {
                    return Err(exceptions::PyValueError::new_err(
                        "Timestamp value too large",
                    ));
                } else {
                    return Ok(Nanoseconds::from(ns as u64).into());
                }
            }
            Err(_) => {
                return Err(exceptions::PyValueError::new_err(
                    "Timestamp before UNIX epoch",
                ));
            }
        }
    }

    // Less common types are checked later
    if let Ok(tl_id) = py.extract::<TimelineId>() {
        return Ok(tl_id.into());
    }

    if let Ok(lt) = py.extract::<LogicalTime>() {
        return Ok(lt.0.into());
    }

    if let Ok(ec) = py.extract::<EventCoordinate>() {
        return Ok(ec.0.into());
    }

    if let Ok(i) = py.extract::<i128>() {
        return Ok(i.into());
    }

    Err(exceptions::PyValueError::new_err(
        "Cannot represent value as AttrVal",
    ))
}
