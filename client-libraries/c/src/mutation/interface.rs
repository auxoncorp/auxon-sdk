use crate::{attr_val, capi_result, util, Error, NullPtrExt};
use auxon_sdk::api::{AttrKey, AttrType, AttrVal};
use auxon_sdk::mutator_protocol::descriptor::owned as mmi;
use std::ffi::{c_char, c_int, c_void};
use std::{
    collections::{BTreeMap, HashMap},
    option, ptr, slice,
};
use uuid::Uuid;

#[repr(C)]
#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, PartialOrd, Ord)]
pub enum attr_type {
    TimelineId,
    String,
    Integer,
    BigInt,
    Float,
    Bool,
    Timestamp,
    LogicalTime,
    Any,
}

impl From<attr_type> for AttrType {
    fn from(value: attr_type) -> Self {
        use attr_type::*;
        match value {
            TimelineId => AttrType::TimelineId,
            String => AttrType::String,
            Integer => AttrType::Integer,
            BigInt => AttrType::BigInt,
            Float => AttrType::Float,
            Bool => AttrType::Bool,
            Timestamp => AttrType::Nanoseconds,
            LogicalTime => AttrType::LogicalTime,
            Any => AttrType::Any,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct attr_kv {
    pub key: *const c_char,
    pub val: attr_val,
}

#[repr(transparent)]
struct opt_attr_val_ptr(*const attr_val);

impl From<opt_attr_val_ptr> for Option<AttrVal> {
    fn from(value: opt_attr_val_ptr) -> Self {
        if value.0.is_null() {
            None
        } else {
            Some(unsafe { &*value.0 }.into())
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct mutator_descriptor {
    pub name: *const c_char,
    pub description: *const c_char,
    pub layer: mutator_layer,
    pub group: *const c_char,
    pub operation: mutator_operation,
    pub statefulness: mutator_statefulness,
    pub organization_custom_metadata: *const organization_custom_metadata,
    pub params: *const mutator_param_descriptor,
    pub params_length: usize,
}

#[no_mangle]
pub extern "C" fn modality_mutator_descriptor_init(value: *mut mutator_descriptor) -> c_int {
    capi_result(|| unsafe {
        let m = value.as_mut().ok_or(Error::NullPointer)?;
        m.name = ptr::null();
        m.description = ptr::null();
        m.layer = mutator_layer::None;
        m.group = ptr::null();
        m.operation = mutator_operation::None;
        m.statefulness = mutator_statefulness::None;
        m.organization_custom_metadata = ptr::null();
        m.params = ptr::null();
        m.params_length = 0;
        Ok(())
    })
}

impl TryFrom<&mutator_descriptor> for mmi::OwnedMutatorDescriptor {
    type Error = Error;

    fn try_from(value: &mutator_descriptor) -> Result<Self, Self::Error> {
        let organization_custom_metadata = if value.organization_custom_metadata.is_null() {
            None
        } else {
            Some(unsafe { &*value.organization_custom_metadata }.try_into()?)
        };

        let params = if value.params.is_null() || value.params_length == 0 {
            Default::default()
        } else {
            let mpds = unsafe { slice::from_raw_parts(value.params, value.params_length) };
            let mut params = Vec::new();
            for mpd in mpds {
                params.push(mpd.try_into()?);
            }
            params
        };

        Ok(mmi::OwnedMutatorDescriptor {
            name: util::opt_owned_cstr(value.name)?,
            description: util::opt_owned_cstr(value.description)?,
            layer: value.layer.into(),
            group: util::opt_owned_cstr(value.group)?,
            operation: value.operation.into(),
            statefulness: value.statefulness.into(),
            organization_custom_metadata,
            params,
        })
    }
}

#[repr(C)]
#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, PartialOrd, Ord)]
pub enum mutator_layer {
    None,
    Implementational,
    Operational,
    Environmental,
}

impl From<mutator_layer> for Option<mmi::MutatorLayer> {
    fn from(value: mutator_layer) -> Self {
        use mutator_layer::*;
        Some(match value {
            None => return option::Option::None,
            Implementational => mmi::MutatorLayer::Implementational,
            Operational => mmi::MutatorLayer::Operational,
            Environmental => mmi::MutatorLayer::Environmental,
        })
    }
}

#[repr(C)]
#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, PartialOrd, Ord)]
pub enum mutator_operation {
    None,
    Delay,
    Duplicate,
    DropFraction,
    DropPositional,
    Disable,
    Enable,
    Corrupt,
    SetToValue,
    SubstituteNextValue,
    Reorder,
    Stimulate,
}

impl From<mutator_operation> for Option<mmi::MutatorOperation> {
    fn from(value: mutator_operation) -> Self {
        use mutator_operation::*;
        Some(match value {
            None => return option::Option::None,
            Delay => mmi::MutatorOperation::Delay,
            Duplicate => mmi::MutatorOperation::Duplicate,
            DropFraction => mmi::MutatorOperation::DropFraction,
            DropPositional => mmi::MutatorOperation::DropPositional,
            Disable => mmi::MutatorOperation::Disable,
            Enable => mmi::MutatorOperation::Enable,
            Corrupt => mmi::MutatorOperation::Corrupt,
            SetToValue => mmi::MutatorOperation::SetToValue,
            SubstituteNextValue => mmi::MutatorOperation::SubstituteNextValue,
            Reorder => mmi::MutatorOperation::Reorder,
            Stimulate => mmi::MutatorOperation::Stimulate,
        })
    }
}

#[repr(C)]
#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, PartialOrd, Ord)]
pub enum mutator_statefulness {
    None,
    Permanent,
    Intermittent,
    Transient,
}

impl From<mutator_statefulness> for Option<mmi::MutatorStatefulness> {
    fn from(value: mutator_statefulness) -> Self {
        use mutator_statefulness::*;
        Some(match value {
            None => return option::Option::None,
            Permanent => mmi::MutatorStatefulness::Permanent,
            Intermittent => mmi::MutatorStatefulness::Intermittent,
            Transient => mmi::MutatorStatefulness::Transient,
        })
    }
}

#[repr(C)]
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct organization_custom_metadata {
    pub organization_name_segment: *const c_char,
    pub attributes: *const attr_kv,
    pub attributes_length: usize,
}

#[no_mangle]
pub extern "C" fn modality_organization_custom_metadata_init(
    value: *mut organization_custom_metadata,
) -> c_int {
    capi_result(|| unsafe {
        let m = value.as_mut().ok_or(Error::NullPointer)?;
        m.organization_name_segment = ptr::null();
        m.attributes = ptr::null();
        m.attributes_length = 0;
        Ok(())
    })
}

impl TryFrom<&organization_custom_metadata> for mmi::OrganizationCustomMetadata {
    type Error = Error;

    fn try_from(value: &organization_custom_metadata) -> Result<Self, Self::Error> {
        let ons = util::default_owned_cstr(value.organization_name_segment)?;
        let attr_kvs = if value.attributes.is_null() || value.attributes_length == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(value.attributes, value.attributes_length) }
        };
        let mut attributes = HashMap::new();
        for kv in attr_kvs.iter() {
            attributes.insert(util::default_owned_cstr(kv.key)?, (&kv.val).into());
        }
        mmi::OrganizationCustomMetadata::new(ons, attributes).ok_or(Error::InvalidNameSegment)
    }
}

#[repr(C)]
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct mutator_param_descriptor {
    pub value_type: attr_type,
    pub name: *const c_char,
    pub description: *const c_char,
    pub value_min: *const attr_val,
    pub value_max: *const attr_val,
    pub default_value: *const attr_val,
    pub least_effect_value: *const attr_val,
    pub value_distribution_kind: value_distribution_kind,
    pub value_distribution_scaling: value_distribution_scaling,
    pub value_distribution_option_set: *const attr_kv,
    pub value_distribution_option_set_length: usize,
    pub organization_custom_metadata: *const organization_custom_metadata,
}

#[no_mangle]
pub extern "C" fn modality_mutator_param_descriptor_init(
    value: *mut mutator_param_descriptor,
) -> c_int {
    capi_result(|| unsafe {
        let m = value.as_mut().ok_or(Error::NullPointer)?;
        m.value_type = attr_type::Any;
        m.name = ptr::null();
        m.description = ptr::null();
        m.value_min = ptr::null();
        m.value_max = ptr::null();
        m.default_value = ptr::null();
        m.least_effect_value = ptr::null();
        m.value_distribution_kind = value_distribution_kind::None;
        m.value_distribution_scaling = value_distribution_scaling::None;
        m.value_distribution_option_set = ptr::null();
        m.value_distribution_option_set_length = 0;
        m.organization_custom_metadata = ptr::null();
        Ok(())
    })
}

impl TryFrom<&mutator_param_descriptor> for mmi::OwnedMutatorParamDescriptor {
    type Error = Error;

    fn try_from(value: &mutator_param_descriptor) -> Result<Self, Self::Error> {
        let value_distribution_option_set = if value.value_distribution_option_set.is_null()
            || value.value_distribution_option_set_length == 0
        {
            None
        } else {
            let set = unsafe {
                slice::from_raw_parts(
                    value.value_distribution_option_set,
                    value.value_distribution_option_set_length,
                )
            };
            let mut vdos = BTreeMap::new();
            for kv in set.iter() {
                vdos.insert(util::default_owned_cstr(kv.key)?, (&kv.val).into());
            }
            Some(vdos)
        };

        let organization_custom_metadata = if value.organization_custom_metadata.is_null() {
            None
        } else {
            Some(unsafe { &*value.organization_custom_metadata }.try_into()?)
        };

        Ok(mmi::OwnedMutatorParamDescriptor {
            value_type: value.value_type.into(),
            name: util::default_owned_cstr(value.name)?,
            description: util::opt_owned_cstr(value.description)?,

            value_min: opt_attr_val_ptr(value.value_min).into(),
            value_max: opt_attr_val_ptr(value.value_max).into(),
            default_value: opt_attr_val_ptr(value.default_value).into(),
            least_effect_value: opt_attr_val_ptr(value.least_effect_value).into(),
            value_distribution_kind: value.value_distribution_kind.into(),
            value_distribution_scaling: value.value_distribution_scaling.into(),
            value_distribution_option_set,
            organization_custom_metadata,
        })
    }
}

#[repr(C)]
#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, PartialOrd, Ord)]
pub enum value_distribution_kind {
    None,
    Continuous,
    Discrete,
}

impl From<value_distribution_kind> for Option<mmi::ValueDistributionKind> {
    fn from(value: value_distribution_kind) -> Self {
        use value_distribution_kind::*;
        Some(match value {
            None => return option::Option::None,
            Continuous => mmi::ValueDistributionKind::Continuous,
            Discrete => mmi::ValueDistributionKind::Discrete,
        })
    }
}

#[repr(C)]
#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, PartialOrd, Ord)]
pub enum value_distribution_scaling {
    None,
    Linear,
    Complex,
    Circular,
}

impl From<value_distribution_scaling> for Option<mmi::ValueDistributionScaling> {
    fn from(value: value_distribution_scaling) -> Self {
        use value_distribution_scaling::*;
        Some(match value {
            None => return option::Option::None,
            Linear => mmi::ValueDistributionScaling::Linear,
            Complex => mmi::ValueDistributionScaling::Complex,
            Circular => mmi::ValueDistributionScaling::Circular,
        })
    }
}

#[repr(C)]
pub struct mutation_id([u8; 16]);

impl From<&mutation_id> for Uuid {
    fn from(mid: &mutation_id) -> Self {
        Uuid::from_bytes(mid.0)
    }
}

impl From<Uuid> for mutation_id {
    fn from(value: Uuid) -> Self {
        mutation_id(value.into_bytes())
    }
}

#[no_mangle]
pub extern "C" fn modality_mutation_id_init(value: *mut mutation_id) -> c_int {
    capi_result(|| unsafe {
        let mid = value.as_mut().ok_or(Error::NullPointer)?;
        let new_mid = Uuid::new_v4();
        mid.0.copy_from_slice(new_mid.as_bytes());
        Ok(())
    })
}

pub type mutator_get_description_fn =
    Option<extern "C" fn(*mut c_void, *mut *const mutator_descriptor)>;
pub type mutator_actuator_inject_fn =
    Option<extern "C" fn(*mut c_void, *const mutation_id, *const attr_kv, usize) -> c_int>;
pub type mutator_actuator_reset_fn = Option<extern "C" fn(*mut c_void) -> c_int>;

#[repr(C)]
#[derive(Clone)]
pub struct mutator {
    pub state: *mut c_void,
    pub get_description: mutator_get_description_fn,
    pub inject: mutator_actuator_inject_fn,
    pub reset: mutator_actuator_reset_fn,
}

impl mutator {
    pub(crate) fn has_get_description(&self) -> bool {
        self.get_description.is_some()
    }

    pub(crate) fn has_inject(&self) -> bool {
        self.inject.is_some()
    }

    pub(crate) fn has_reset(&self) -> bool {
        self.reset.is_some()
    }

    pub(crate) fn get_description(&self) -> Result<mmi::OwnedMutatorDescriptor, Error> {
        let cb = self
            .get_description
            .ok_or(Error::NullMutatorInterfaceFunction)?;
        let mut md_ptr: *const mutator_descriptor = ptr::null();
        (cb)(self.state, &mut md_ptr as *mut _);
        md_ptr.null_check()?;
        let md = unsafe { &*md_ptr };
        mmi::OwnedMutatorDescriptor::try_from(md)
    }

    pub(crate) fn inject(
        &self,
        mut_id: Uuid,
        params: BTreeMap<AttrKey, AttrVal>,
        capi_params_storage: &mut Vec<attr_kv>,
    ) -> Result<(), Error> {
        let cb = self.inject.ok_or(Error::NullMutatorInterfaceFunction)?;
        let m_id = mutation_id::from(mut_id);

        capi_params_storage.clear();
        for (k, v) in params.iter() {
            let capi_v = match v {
                AttrVal::String(v) => attr_val::String(v.as_ptr() as *const _),
                AttrVal::Integer(v) => attr_val::Integer(*v),
                AttrVal::Float(v) => attr_val::Float(**v),
                AttrVal::Bool(v) => attr_val::Bool(*v),
                AttrVal::Timestamp(v) => attr_val::Timestamp(v.get_raw()),
                // TODO - pass through all the available variants once we unify the
                // ingest and mutation AttrVal types. Note that some of the variants
                // will require heap/stack allocation to hold the converted C API type
                // while a reference is given to the callback
                _ => {
                    tracing::warn!(attr_type = %v.attr_type(), "Dropping unsupported attr type");
                    continue;
                }
            };
            capi_params_storage.push(attr_kv {
                key: k.as_ref().as_ptr() as *const c_char,
                val: capi_v,
            });
        }

        let c_ret = (cb)(
            self.state,
            &m_id as *const _,
            capi_params_storage.as_ptr(),
            capi_params_storage.len(),
        );

        // Don't drop the param data referenced by the capi_params until inject returns
        let _ = params;

        if c_ret != Error::Ok as c_int {
            Err(Error::MutatorInterfaceError)
        } else {
            Ok(())
        }
    }

    pub(crate) fn reset(&self) -> Result<(), Error> {
        let cb = self.reset.ok_or(Error::NullMutatorInterfaceFunction)?;
        let c_ret = (cb)(self.state);
        if c_ret != Error::Ok as c_int {
            Err(Error::MutatorInterfaceError)
        } else {
            Ok(())
        }
    }
}

#[no_mangle]
pub extern "C" fn modality_mutator_init(value: *mut mutator) -> c_int {
    capi_result(|| unsafe {
        let m = value.as_mut().ok_or(Error::NullPointer)?;
        m.state = ptr::null_mut();
        m.get_description = None;
        m.inject = None;
        m.reset = None;
        Ok(())
    })
}
