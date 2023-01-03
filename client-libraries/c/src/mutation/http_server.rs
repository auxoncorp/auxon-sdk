use crate::{
    capi_result,
    mutation::{attr_kv, mutator},
    runtime, util, Error,
};
use async_trait::async_trait;
use modality_mutator_protocol::{
    actuator::MutatorActuator,
    attrs::{AttrKey, AttrVal},
    mutator::{ActuatorDescriptor, CombinedMutator},
};
use std::collections::BTreeMap;
use std::ffi::{c_char, c_int};
use std::{net, slice};
use uuid::Uuid;

#[repr(C)]
#[derive(Clone)]
pub struct http_mutator {
    pub mutator_correlation_id: *const c_char,
    pub mutator: mutator,
}

struct CApiActuatorDescriptor {
    mutator_correlation_id: String,
    m: mutator,
    capi_params_storage: Vec<attr_kv>,
}

// NOTE: we're using a current thread runtime only atm, so it's ok for now
unsafe impl Send for CApiActuatorDescriptor {}
unsafe impl Sync for CApiActuatorDescriptor {}

impl CApiActuatorDescriptor {
    fn new(m: &http_mutator) -> Result<Self, Error> {
        if !m.mutator.has_get_description() {
            return Err(Error::NullMutatorInterfaceFunction);
        }
        if !m.mutator.has_inject() {
            return Err(Error::NullMutatorInterfaceFunction);
        }
        if !m.mutator.has_reset() {
            return Err(Error::NullMutatorInterfaceFunction);
        }

        let mutator_correlation_id = util::require_owned_cstr(m.mutator_correlation_id)?;
        Ok(Self {
            mutator_correlation_id,
            m: m.mutator.clone(),
            capi_params_storage: Vec::new(),
        })
    }

    fn into_actuator_descriptor(self) -> Result<Box<dyn ActuatorDescriptor + Send>, Error> {
        let desc = self.m.get_description()?;
        Ok(Box::new(CombinedMutator::new(self, desc)))
    }
}

#[async_trait]
impl MutatorActuator for CApiActuatorDescriptor {
    async fn inject(
        &mut self,
        mutation_id: Uuid,
        params: BTreeMap<AttrKey, AttrVal>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.m
            .inject(mutation_id, params, &mut self.capi_params_storage)?;
        Ok(())
    }

    async fn reset(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.m.reset()?;
        Ok(())
    }
}

#[no_mangle]
pub extern "C" fn modality_mutator_http_server_run(
    rt: *const runtime,
    addr: *const c_char,
    port: u16,
    mutators: *const http_mutator,
    mutators_length: usize,
) -> c_int {
    capi_result(|| unsafe {
        let rt = rt.as_ref().ok_or(Error::NullPointer)?;

        let capi_mutators = if mutators.is_null() || mutators_length == 0 {
            &[]
        } else {
            slice::from_raw_parts(mutators, mutators_length)
        };

        let addr: net::IpAddr = if let Some(s) = util::opt_owned_cstr(addr)? {
            s.parse().map_err(|_| Error::InvalidIpAddress)?
        } else {
            net::Ipv4Addr::UNSPECIFIED.into()
        };

        let mut mutators = BTreeMap::new();
        for capi_m in capi_mutators.iter() {
            let m = CApiActuatorDescriptor::new(capi_m)?;
            mutators.insert(
                m.mutator_correlation_id.clone(),
                m.into_actuator_descriptor()?,
            );
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        rt.0.block_on(modality_mutator_server::server::serve_mutators(
            mutators,
            None,
            (addr, port),
            async {
                let _ = shutdown_rx.await.map_err(|_recv_err| {
                    tracing::warn!("Shutdown signal channel unexpectedly closed early");
                });
            },
        ));

        // TODO - consider setting up tokio signal handling
        let _ = shutdown_tx;
        Ok(())
    })
}
