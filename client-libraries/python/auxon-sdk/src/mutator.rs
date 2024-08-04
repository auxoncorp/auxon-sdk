use std::collections::{BTreeMap, HashMap};

use auxon_sdk::{
    api::{AttrType, AttrVal},
    mutation_plane::types::{MutationId, MutatorId},
    mutator_protocol::descriptor::owned::{
        MutatorLayer, MutatorOperation, MutatorStatefulness, OrganizationCustomMetadata,
        OwnedMutatorDescriptor, OwnedMutatorParamDescriptor, ValueDistributionKind,
        ValueDistributionScaling,
    },
    plugin_utils::mutation::Mutator,
};
use pyo3::{
    intern,
    prelude::*,
    types::{PyDict, PyType},
};

use crate::SdkError;

/// A python 'descriptor' used to declare mutator paramters.
///
/// It is meant to be used like this:
///
/// class MyParams:
///     p = MutatorParam("p", int)
///
/// Then, when the mutation is injected, the user will get an instance
/// of MyParams, where they can look at `my_params.p` and see the
/// injected parameter value. This is made possible via the magic
/// `__get__` method; when a mutation is injected, the actual paramter
/// values are stored in a dictionary, which is attached to the
/// instance as `_mutator_parameters`. `__get__` looks up the actual
/// value out of this dictionary.
#[pyclass]
#[derive(Debug, Clone)]
pub struct MutatorParam {
    descriptor: OwnedMutatorParamDescriptor,
}

#[pymethods]
impl MutatorParam {
    #[new]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: &str,
        value_type: Bound<PyType>,
        description: Option<String>,
        value_min: Option<AttrVal>,
        value_max: Option<AttrVal>,
        default_value: Option<AttrVal>,
        least_effect_value: Option<AttrVal>,
        value_distribution_kind: Option<ValueDistributionKind>,
        value_distribution_scaling: Option<ValueDistributionScaling>,
        value_distribution_option_set: Option<BTreeMap<String, AttrVal>>,
        organization_name_segment: Option<String>,
        organization_custom_metadata: Option<HashMap<String, AttrVal>>,
    ) -> Result<Self, PyErr> {
        let organization_custom_metadata = match (
            organization_name_segment,
            organization_custom_metadata,
        ) {
            (Some(n), Some(m)) => {
                validate_organization_name_segment(&n)?;
                OrganizationCustomMetadata::new(n, m)
            }
            (None, None) => None,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "organization_name_segment and organization_custom_metdata must be specificed together ",
            ));
            }
        };

        Ok(Self {
            descriptor: OwnedMutatorParamDescriptor {
                name: name.to_owned(),
                value_type: py_type_to_attr_type(value_type)?,
                description,
                value_min,
                value_max,
                default_value,
                least_effect_value,
                value_distribution_kind,
                value_distribution_scaling,
                value_distribution_option_set,
                organization_custom_metadata,
            },
        })
    }

    pub fn __get__(
        &self,
        py: Python<'_>,
        obj: PyObject,
        _obj_type: Option<PyObject>,
    ) -> PyResult<Option<PyObject>> {
        let Ok(param_dict_obj) = obj.getattr(py, intern!(py, "_mutator_parameters")) else {
            return self.default_value(py);
        };

        // if the value is there, but of a different type, we want to throw an exception.
        let param_dict = param_dict_obj.downcast_bound::<PyDict>(py)?;

        match param_dict.get_item(&self.descriptor.name)? {
            Some(item) => Ok(Some(item.to_object(py))),
            None => self.default_value(py),
        }
    }

    fn default_value(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        Ok(self
            .descriptor
            .default_value
            .clone()
            .map(|val| val.into_py(py)))
    }
}

fn py_type_to_attr_type(py_type: Bound<PyType>) -> PyResult<AttrType> {
    let type_name = py_type.name()?;
    let mut type_name_ref = type_name.as_ref();
    if let Some(s) = type_name_ref.strip_prefix("builtins.") {
        type_name_ref = s;
    }

    match type_name_ref {
        "str" => Ok(AttrType::String),
        "int" => Ok(AttrType::Integer),
        "float" => Ok(AttrType::Float),
        "bool" => Ok(AttrType::Bool),
        _ => Err(pyo3::exceptions::PyValueError::new_err(
            "Supported mutator param types are str, int, float, and bool ",
        )),
    }
}

#[pyclass]
pub struct MutatorHost {
    #[allow(unused)]
    rt: tokio::runtime::Runtime,
    #[allow(unused)]
    worker_join_handle: tokio::task::JoinHandle<Result<(), SdkError>>,
    worker_tx: tokio::sync::mpsc::Sender<WorkerMsg>,
}

enum WorkerMsg {
    RegisterMutator {
        m: Box<dyn Mutator + Send>,
        res_tx: tokio::sync::oneshot::Sender<Result<(), SdkError>>,
    },
}

impl MutatorHost {
    pub fn new(
        mut inner: auxon_sdk::plugin_utils::mutation::MutatorHost,
        rt: tokio::runtime::Runtime,
    ) -> Result<Self, SdkError> {
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel(1);

        let worker_join_handle = rt.spawn(async move {
            loop {
                tokio::select! {
                    control_msg = worker_rx.recv() => {
                        match control_msg {
                            Some(WorkerMsg::RegisterMutator { m, res_tx }) => {
                                let res = inner.register_mutator(m).await;
                                res_tx.send(res.map_err(|e| e.into())).map_err(|_| "Failed to send register mutator response")?;
                            },
                            None => break,
                        }
                    },
                    protocol_msg_res = inner.mutation_conn.read_msg() => {
                        match protocol_msg_res {
                            Ok(m) => {
                                inner.handle_message(m).await;
                            },
                            Err(e) => {
                                tracing::error!(e = &e as &dyn std::error::Error, "Received error from mutation connection");
                                return Err(e.into());
                            }
                        }
                    }
                }
            }
            Result::<(), SdkError>::Ok(())
        });

        Ok(Self {
            rt,
            worker_join_handle,
            worker_tx,
        })
    }
}

#[pymethods]
impl MutatorHost {
    fn register(&mut self, py: Python<'_>, mutator: PyObject) -> PyResult<()> {
        let m = Box::new(PythonMutatorProxy::new(py, mutator)?);
        let (res_tx, res_rx) = tokio::sync::oneshot::channel();
        self.worker_tx
            .blocking_send(WorkerMsg::RegisterMutator { m, res_tx })
            .unwrap();
        if let Err(e) = res_rx.blocking_recv() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unable to register mutator: {e}"
            )));
        }
        Ok(())
    }
}

struct PythonMutatorProxy {
    id: MutatorId,
    descriptor: OwnedMutatorDescriptor,
    obj: PyObject,
    params_class: Py<PyAny>,
}

impl PythonMutatorProxy {
    fn new(py: Python<'_>, mutator: PyObject) -> Result<Self, PyErr> {
        let mutator_class = mutator.getattr(py, "__class__")?;
        let mutator_descriptor = mutator_class
            .getattr(py, "_mutator_descriptor")?
            .extract::<PyMutatorDescriptor>(py)?;
        Ok(Self {
            id: MutatorId::allocate(),
            descriptor: mutator_descriptor.descriptor,
            params_class: mutator_descriptor.params_class,
            obj: mutator,
        })
    }
}

impl Mutator for PythonMutatorProxy {
    fn id(&self) -> MutatorId {
        self.id
    }

    fn descriptor(&self) -> OwnedMutatorDescriptor {
        self.descriptor.clone()
    }

    fn inject(&mut self, mutation_id: MutationId, params: BTreeMap<String, AttrVal>) -> bool {
        Python::with_gil(|py| {
            let params_obj = self.params_class.call0(py).unwrap();
            params_obj
                .setattr(py, intern!(py, "_mutator_parameters"), params)
                .unwrap();

            if let Err(e) =
                self.obj
                    .call_method1(py, intern!(py, "inject"), (mutation_id, params_obj))
            {
                tracing::warn!(
                    err = &e as &dyn std::error::Error,
                    "Failed to inject mutation for Python mutator"
                );

                false
            } else {
                true
            }
        })
    }

    fn clear_mutation(&mut self, mutation_id: &MutationId) {
        Python::with_gil(|py| {
            if let Err(e) =
                self.obj
                    .call_method1(py, intern!(py, "clear_mutation"), (*mutation_id,))
            {
                tracing::warn!(
                    err = &e as &dyn std::error::Error,
                    "Failed to clear mutation for Python mutator"
                );
            }
        })
    }

    fn reset(&mut self) {
        Python::with_gil(|py| {
            if let Err(e) = self.obj.call_method0(py, intern!(py, "reset")) {
                tracing::warn!(
                    err = &e as &dyn std::error::Error,
                    "Failed to reset Python mutator"
                );
            }
        })
    }
}

#[pyclass(name = "MutatorDescriptor")]
#[derive(Debug, Clone)]
pub struct PyMutatorDescriptor {
    descriptor: OwnedMutatorDescriptor,
    params_class: PyObject,
}

#[pymethods]
impl PyMutatorDescriptor {
    #[new]
    #[allow(clippy::too_many_arguments)]
    fn py_new(
        params_class: PyObject,
        params: Vec<MutatorParam>,
        name: Option<String>,
        description: Option<String>,
        layer: Option<MutatorLayer>,
        group: Option<String>,
        operation: Option<MutatorOperation>,
        statefulness: Option<MutatorStatefulness>,
        organization_name_segment: Option<String>,
        organization_custom_metadata: Option<HashMap<String, AttrVal>>,
    ) -> Result<Self, PyErr> {
        let organization_custom_metadata = match (
            organization_name_segment.as_ref(),
            organization_custom_metadata.as_ref(),
        ) {
            (Some(n), Some(m)) => {
                validate_organization_name_segment(&n)?;
                OrganizationCustomMetadata::new(n.clone(), m.clone())
            }
            (None, None) => None,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "organization_name_segment and organization_custom_metdata must be specificed together",
                ));
            }
        };

        let rs_params = params.into_iter().map(|p| p.descriptor).collect();

        let descriptor = OwnedMutatorDescriptor {
            name,
            description,
            layer,
            group,
            operation,
            statefulness,
            organization_custom_metadata,
            params: rs_params,
        };

        Ok(Self {
            descriptor,
            params_class,
        })
    }
}

fn validate_organization_name_segment(s: &str) -> Result<(), PyErr> {
    if !s.is_ascii() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "organization_name_segment must contain ASCII characters only ",
        ));
    }

    if s.contains('.') {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "organization_name_segment may not contain '.' (period) characters.",
        ));
    }

    Ok(())
}
