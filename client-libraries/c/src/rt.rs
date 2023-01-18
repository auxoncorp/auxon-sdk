use crate::{capi_result, Error, NullPtrExt};
use std::ffi::c_int;
use tokio::runtime::{Builder, Runtime};

pub struct runtime(pub(crate) Runtime);

#[no_mangle]
pub extern "C" fn modality_runtime_new(out: *mut *mut runtime) -> c_int {
    capi_result(|| unsafe {
        out.null_check()?;
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Error::AsyncRuntime)?;
        *out = Box::into_raw(Box::new(runtime(rt)));
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_runtime_free(rt: *mut runtime) {
    if !rt.is_null() {
        let _ = unsafe { Box::from_raw(rt) };
    }
}
