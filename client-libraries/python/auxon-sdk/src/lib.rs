pub mod types;

use auxon_sdk::{api::AttrVal, reflector_config::TomlValue};
use pyo3::prelude::*;

#[pymodule]
fn _auxon_sdk(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<types::TimelineId>()?;
    m.add_class::<types::EventCoordinate>()?;
    m.add_class::<IngestPluginConfig>()?;
    m.add_class::<IngestClient>()?;

    Ok(())
}

#[pyclass]
pub struct IngestPluginConfig {
    config: auxon_sdk::plugin_utils::ingest::Config<toml::value::Table>,
    plugin_config: PyObject,
}

#[derive(Debug)]
struct ConfigField {
    /// What's the python attr name for this field?
    python_attr_name: String,

    /// What type does python epxected this to have? Extracted from the dataclasses.fields(...) call.
    ty: ConfigFieldType,

    /// How do you configure this from the environment? The form is
    /// `PYTHON_NAME` (the prefix is identified and removed before we
    /// do a comparison).
    env_var_name: String,

    /// How do you configure this from toml? The form is `python-name`
    toml_key: String,
}

impl ConfigField {
    fn new(python_attr_name: String, ty: ConfigFieldType, env_prefix: &str) -> Self {
        Self {
            toml_key: python_attr_name.to_lowercase().replace("_", "-"),
            env_var_name: python_attr_name.to_uppercase(),
            python_attr_name,
            ty,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
enum ConfigFieldType {
    Int,
    Float,
    Str,
    Bool,
}

impl ConfigFieldType {
    fn parse_env(&self, env_val: &str) -> Option<TomlValue> {
        match self {
            ConfigFieldType::Int => Some(TomlValue::Integer(env_val.parse().ok()?)),
            ConfigFieldType::Float => Some(TomlValue::Float(env_val.parse().ok()?)),
            ConfigFieldType::Str => Some(TomlValue::String(env_val.to_owned())),
            ConfigFieldType::Bool => Some(TomlValue::Boolean(env_val.to_lowercase().parse().ok()?)),
        }
    }
}

impl TryFrom<&str> for ConfigFieldType {
    type Error = PyErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "int" => Ok(ConfigFieldType::Int),
            "float" => Ok(ConfigFieldType::Float),
            "str" => Ok(ConfigFieldType::Str),
            "bool" => Ok(ConfigFieldType::Bool),
            _ => Err(pyo3::exceptions::PyValueError::new_err(
                "Unsupported type for config field (must be int, float, str, or bool)",
            )),
        }
    }
}

#[pymethods]
impl IngestPluginConfig {
    #[new]
    pub fn new(config_dataclass: &Bound<PyAny>, env_prefix: &str) -> Result<Self, PyErr> {
        let config_fields_name_and_type = Python::with_gil(|py| {
            let dataclasses = PyModule::import_bound(py, "dataclasses")?;
            let dataclass_fields: Vec<Bound<PyAny>> = dataclasses
                .getattr("fields")?
                .call1((config_dataclass,))?
                .extract()?;
            let mut field_vec = vec![];
            for field in dataclass_fields {
                let name: String = field.getattr("name")?.extract()?;
                let ty: String = field.getattr("type")?.getattr("__name__")?.extract()?;
                field_vec.push((name, ty));
            }

            Result::<_, PyErr>::Ok(field_vec)
        })?;

        let config_fields: Vec<ConfigField> = config_fields_name_and_type
            .into_iter()
            .map(|(name, ty)| Ok(ConfigField::new(name, ty.as_str().try_into()?, env_prefix)))
            .collect::<Result<_, PyErr>>()?;

        dbg!(&config_fields);

        let config = auxon_sdk::plugin_utils::ingest::Config::<toml::value::Table>::load_custom(
            env_prefix,
            |env_key, env_val| {
                dbg!(&env_key, &env_val);
                if let Some(field) = config_fields.iter().find(|f| f.env_var_name == env_key) {
                    let parsed_val = field.ty.parse_env(env_val)?;
                    Some(dbg!((field.toml_key.clone(),parsed_val)))
                } else {
                    None
                }
            },
        )
        .map_err(SdkError::from)?;
        dbg!(&config);

        // build an instance of config_dataclass from the toml table in config.plugin
        let plugin_config = config_dataclass.call0()?; // dataclass constructor
        Python::with_gil(|py| {
            for (toml_key, toml_value) in config.plugin.iter() {
                if let Some(field) = config_fields.iter().find(|f| &f.toml_key == toml_key) {
                    plugin_config.setattr(
                        field.python_attr_name.as_str(),
                        toml_value_to_py(py, toml_value)?,
                    )?;
                }
            }
            Result::<(), PyErr>::Ok(())
        })?;

        dbg!(&plugin_config);

        Ok(Self {
            config,
            plugin_config: plugin_config.into(),
        })
    }

    pub fn connect_and_authenticate(&self) -> Result<IngestClient, SdkError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let client = rt.block_on(self.config.connect_and_authenticate())?;
        Ok(IngestClient { rt, client })
    }

    #[getter]
    pub fn plugin(&self) -> PyObject {
        self.plugin_config.clone()
    }
}

fn toml_value_to_py(py: Python, v: &TomlValue) -> PyResult<PyObject> {
    match v {
        TomlValue::String(s) => Ok(s.to_object(py)),
        TomlValue::Integer(i) => Ok(i.to_object(py)),
        TomlValue::Float(f) => Ok(f.to_object(py)),
        TomlValue::Boolean(b) => Ok(b.to_object(py)),
        TomlValue::Datetime(_) | TomlValue::Array(_) | TomlValue::Table(_) => {
            Err(pyo3::exceptions::PyValueError::new_err(
                "Unsupported type for config field (must be int, float, str, or bool)",
            ))
        }
    }
}

#[pyclass]
pub struct IngestClient {
    rt: tokio::runtime::Runtime,
    client: auxon_sdk::plugin_utils::ingest::Client,
}

#[pymethods]
impl IngestClient {
    pub fn disable_auto_timestamp(&mut self) {
        self.client.disable_auto_timestamp();
    }

    pub fn switch_timeline(&mut self, id: &types::TimelineId) -> Result<(), SdkError> {
        Ok(self.rt.block_on(self.client.switch_timeline(id.0))?)
    }

    pub fn send_timeline_attrs(
        &mut self,
        name: &str,
        timeline_attrs: &Bound<pyo3::types::PyDict>,
    ) -> Result<(), PyErr> {
        let attrs = py_dict_to_attr_vec(timeline_attrs)?;
        self.rt
            .block_on(
                self.client
                    .send_timeline_attrs(name, attrs.iter().map(|(k, v)| (k.as_str(), v.clone()))),
            )
            .map_err(SdkError::from)?;

        Ok(())
    }

    pub fn send_event(
        &mut self,
        name: &str,
        ordering: u128,
        event_attrs: &Bound<pyo3::types::PyDict>,
    ) -> Result<(), PyErr> {
        let attrs = py_dict_to_attr_vec(event_attrs)?;

        self.rt
            .block_on(self.client.send_event(
                name,
                ordering,
                attrs.iter().map(|(k, v)| (k.as_str(), v.clone())),
            ))
            .map_err(SdkError::from)?;

        Ok(())
    }
}

fn py_dict_to_attr_vec(dict: &Bound<pyo3::types::PyDict>) -> Result<Vec<(String, AttrVal)>, PyErr> {
    let mut attrs = Vec::with_capacity(dict.len());
    for (k, v) in dict.iter() {
        let k = k.extract::<String>()?;
        let v = types::py_any_to_attr_val(v)?;
        attrs.push((k, v));
    }
    Ok(attrs)
}

pub struct SdkError(Box<dyn std::error::Error>);

impl From<Box<dyn std::error::Error>> for SdkError {
    fn from(value: Box<dyn std::error::Error>) -> Self {
        SdkError(value)
    }
}

impl From<std::io::Error> for SdkError {
    fn from(value: std::io::Error) -> Self {
        SdkError(Box::new(value))
    }
}

impl From<SdkError> for PyErr {
    fn from(value: SdkError) -> Self {
        // TODO
        pyo3::exceptions::PyValueError::new_err(value.0.to_string())
    }
}
