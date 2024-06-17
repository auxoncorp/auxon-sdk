use auxon_sdk::reflector_config::TomlValue;
use pyo3::prelude::*;

use crate::{ingest::IngestClient, mutator::MutatorHost, SdkError};

#[pyclass]
pub struct PluginConfig {
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
    fn new(python_attr_name: String, ty: ConfigFieldType) -> Self {
        Self {
            toml_key: python_attr_name.to_lowercase().replace('_', "-"),
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
    fn parse_env(
        &self,
        env_val: &str,
    ) -> Result<TomlValue, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            ConfigFieldType::Int => Ok(TomlValue::Integer(env_val.parse()?)),
            ConfigFieldType::Float => Ok(TomlValue::Float(env_val.parse()?)),
            ConfigFieldType::Str => Ok(TomlValue::String(env_val.to_owned())),
            ConfigFieldType::Bool => Ok(TomlValue::Boolean(env_val.to_lowercase().parse()?)),
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
impl PluginConfig {
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
            .map(|(name, ty)| Ok(ConfigField::new(name, ty.as_str().try_into()?)))
            .collect::<Result<_, PyErr>>()?;

        let config = auxon_sdk::plugin_utils::ingest::Config::<toml::value::Table>::load_custom(
            env_prefix,
            |env_key, env_val| {
                if let Some(field) = config_fields.iter().find(|f| f.env_var_name == env_key) {
                    let parsed_val = field.ty.parse_env(env_val)?;
                    Ok(Some((field.toml_key.clone(), parsed_val)))
                } else {
                    Ok(None)
                }
            },
        )
        .map_err(SdkError::from)?;

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

        Ok(Self {
            config,
            plugin_config: plugin_config.into(),
        })
    }

    pub fn connect_and_authenticate_ingest(&self) -> Result<IngestClient, SdkError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let client = rt.block_on(self.config.connect_and_authenticate_ingest())?;
        Ok(IngestClient::new(rt, client))
    }

    pub fn connect_and_authenticate_mutation(&self) -> Result<MutatorHost, SdkError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let mh = rt
            .block_on(self.config.connect_and_authenticate_mutation())
            .unwrap();
        MutatorHost::new(mh, rt)
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
