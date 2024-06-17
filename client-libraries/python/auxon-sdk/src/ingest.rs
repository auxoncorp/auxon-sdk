use crate::{types, SdkError};
use auxon_sdk::{
    api::{AttrVal, TimelineId},
    ingest_client::IngestStatus,
};
use pyo3::prelude::*;

#[pyclass]
pub struct IngestClient {
    rt: tokio::runtime::Runtime,
    client: auxon_sdk::plugin_utils::ingest::Client,
}

impl IngestClient {
    pub fn new(
        rt: tokio::runtime::Runtime,
        client: auxon_sdk::plugin_utils::ingest::Client,
    ) -> Self {
        Self { rt, client }
    }
}

#[pymethods]
impl IngestClient {
    pub fn disable_auto_timestamp(&mut self) {
        self.client.disable_auto_timestamp();
    }

    pub fn switch_timeline(&mut self, id: TimelineId) -> Result<(), SdkError> {
        Ok(self.rt.block_on(self.client.switch_timeline(id))?)
    }

    pub fn send_timeline_attrs(
        &mut self,
        name: &str,
        timeline_attrs: &Bound<pyo3::types::PyDict>,
    ) -> Result<(), PyErr> {
        let attrs = py_dict_to_attr_vec(timeline_attrs)?;
        self.rt.block_on(
            self.client
                .send_timeline_attrs(name, attrs.iter().map(|(k, v)| (k.as_str(), v.clone()))),
        )?;

        Ok(())
    }

    pub fn send_event(
        &mut self,
        name: &str,
        ordering: u128,
        event_attrs: &Bound<pyo3::types::PyDict>,
    ) -> Result<(), PyErr> {
        let attrs = py_dict_to_attr_vec(event_attrs)?;

        self.rt.block_on(self.client.send_event(
            name,
            ordering,
            attrs.iter().map(|(k, v)| (k.as_str(), v.clone())),
        ))?;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), PyErr> {
        self.rt.block_on(self.client.flush())?;
        Ok(())
    }

    pub fn status(&mut self) -> Result<IngestStatus, PyErr> {
        let status = self.rt.block_on(self.client.status())?;
        Ok(status)
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
