use crate::api::{AttrKey, AttrVal};

/// Flat, infallible view on a mutator descriptor
pub trait MutatorDescriptor {
    /// Returned attribute iterator should not contain duplicate keys.
    /// It is effectively a map of key-value pairs.
    fn get_description_attributes(&self) -> Box<dyn Iterator<Item = (AttrKey, AttrVal)> + '_>;
}

pub mod owned {
    use crate::api::AttrType;

    use super::*;
    use crate::mutator_protocol::params_attributes::{
        is_valid_param_key, MUTATOR_PARAMS_DEFAULT_VALUE_SUFFIX, MUTATOR_PARAMS_DESCRIPTION_SUFFIX,
        MUTATOR_PARAMS_LEAST_EFFECT_VALUE_SUFFIX, MUTATOR_PARAMS_NAME_SUFFIX,
        MUTATOR_PARAMS_PREFIX, MUTATOR_PARAMS_VALUE_DISTRIBUTION_KIND_SUFFIX,
        MUTATOR_PARAMS_VALUE_DISTRIBUTION_OPTION_SET_INTERFIX,
        MUTATOR_PARAMS_VALUE_DISTRIBUTION_SCALING_SUFFIX, MUTATOR_PARAMS_VALUE_MAX_SUFFIX,
        MUTATOR_PARAMS_VALUE_MIN_SUFFIX, MUTATOR_PARAMS_VALUE_TYPE_SUFFIX,
    };
    use crate::mutator_protocol::{attrs, params_attributes::is_valid_single_key_segment_contents};
    use std::collections::{BTreeMap, HashMap};

    #[derive(Debug, Clone, Default)]
    pub struct OwnedMutatorDescriptor {
        pub name: Option<String>,
        pub description: Option<String>,
        pub layer: Option<MutatorLayer>,
        pub group: Option<String>,
        pub operation: Option<MutatorOperation>,
        pub statefulness: Option<MutatorStatefulness>,
        pub organization_custom_metadata: Option<OrganizationCustomMetadata>,
        /// The parameters for mutations injected with this mutator
        pub params: Vec<OwnedMutatorParamDescriptor>,
    }

    impl OwnedMutatorDescriptor {
        pub fn into_description_attributes(
            self,
        ) -> Box<dyn Iterator<Item = (AttrKey, AttrVal)> + 'static> {
            let mut all_mutator_attrs: Vec<(AttrKey, AttrVal)> = vec![];
            if let Some(mutator_name) = self.name.as_ref() {
                all_mutator_attrs.push((attrs::mutator::NAME, mutator_name.into()))
            }
            if let Some(mutator_description) = self.description.as_ref() {
                all_mutator_attrs.push((attrs::mutator::DESCRIPTION, mutator_description.into()))
            }
            if let Some(mutator_layer) = self.layer.as_ref() {
                all_mutator_attrs.push((attrs::mutator::LAYER, mutator_layer.name().into()))
            }
            if let Some(mutator_group) = self.group.as_ref() {
                all_mutator_attrs.push((attrs::mutator::GROUP, mutator_group.into()))
            }
            if let Some(mutator_operation) = self.operation.as_ref() {
                all_mutator_attrs.push((attrs::mutator::OPERATION, mutator_operation.name().into()))
            }
            if let Some(organization_custom_metadata) = self.organization_custom_metadata.as_ref() {
                let mut mutator_level_custom_metadata_prefix = "mutator.".to_string();
                mutator_level_custom_metadata_prefix.push_str(
                    organization_custom_metadata
                        .organization_name_segment
                        .as_str(),
                );
                mutator_level_custom_metadata_prefix.push('.');
                for (k, v) in organization_custom_metadata.attributes.iter() {
                    all_mutator_attrs.push((
                        AttrKey::from(format!("{mutator_level_custom_metadata_prefix}{k}")),
                        v.clone(),
                    ));
                }
            }

            for param in &self.params {
                all_mutator_attrs.extend(param.mutator_params_param_key_prefixed_attributes());
            }

            Box::new(all_mutator_attrs.into_iter())
        }

        pub fn try_from_description_attributes(
            i: impl Iterator<Item = (AttrKey, AttrVal)>,
        ) -> Result<Self, ParamDescriptorFromAttrsError> {
            let mut attrs_map: BTreeMap<AttrKey, AttrVal> = i.collect();
            let mut d = OwnedMutatorDescriptor {
                name: None,
                description: None,
                layer: None,
                group: None,
                operation: None,
                statefulness: None,
                organization_custom_metadata: None,
                params: vec![],
            };
            if let Some(AttrVal::String(s)) = attrs_map.remove(&attrs::mutator::NAME) {
                d.name = Some(s.to_string());
            }
            if let Some(AttrVal::String(s)) = attrs_map.remove(&attrs::mutator::DESCRIPTION) {
                d.description = Some(s.to_string());
            }
            if let Some(AttrVal::String(s)) = attrs_map.remove(&attrs::mutator::GROUP) {
                d.group = Some(s.to_string());
            }
            if let Some(AttrVal::String(s)) = attrs_map.remove(&attrs::mutator::LAYER) {
                d.layer = match s.as_ref() {
                    "implementational" => Some(MutatorLayer::Implementational),
                    "operational" => Some(MutatorLayer::Operational),
                    "environmental" => Some(MutatorLayer::Environmental),
                    _ => None,
                };
            }
            if let Some(AttrVal::String(s)) = attrs_map.remove(&attrs::mutator::OPERATION) {
                d.operation = match s.as_ref() {
                    "delay" => Some(MutatorOperation::Delay),
                    "duplicate" => Some(MutatorOperation::Duplicate),
                    "drop_fraction" => Some(MutatorOperation::DropFraction),
                    "drop_positional" => Some(MutatorOperation::DropPositional),
                    "disable" => Some(MutatorOperation::Disable),
                    "enable" => Some(MutatorOperation::Enable),
                    "corrupt" => Some(MutatorOperation::Corrupt),
                    "set_to_value" => Some(MutatorOperation::SetToValue),
                    "substitute_next_value" => Some(MutatorOperation::SubstituteNextValue),
                    "reorder" => Some(MutatorOperation::Reorder),
                    "stimulate" => Some(MutatorOperation::Stimulate),
                    _ => None,
                };
            }
            if let Some(AttrVal::String(s)) = attrs_map.remove(&attrs::mutator::STATEFULNESS) {
                d.statefulness = match s.as_ref() {
                    "permanent" => Some(MutatorStatefulness::Permanent),
                    "intermittent" => Some(MutatorStatefulness::Intermittent),
                    "transient" => Some(MutatorStatefulness::Transient),
                    _ => None,
                };
            }
            let _ = attrs_map.remove(&attrs::mutator::ID);
            // N.B. When the owned descriptor type expands to include
            // these, toss them in here.
            let _ = attrs_map.remove(&attrs::mutator::SOURCE_FILE);
            let _ = attrs_map.remove(&attrs::mutator::SOURCE_LINE);
            let _ = attrs_map.remove(&attrs::mutator::SAFETY);

            let mut custom_bucket: BTreeMap<AttrKey, AttrVal> = Default::default();
            let mut param_key_to_pairs: BTreeMap<String, BTreeMap<AttrKey, AttrVal>> =
                Default::default();
            for (k, v) in attrs_map {
                if let Some(rest) = k.as_ref().strip_prefix(MUTATOR_PARAMS_PREFIX) {
                    if let Some((key, _post_key)) = rest.split_once('.') {
                        let post_key_pairs = param_key_to_pairs.entry(key.to_string()).or_default();
                        post_key_pairs.insert(k, v);
                    } else {
                        // Drop it
                    }
                } else {
                    custom_bucket.insert(k, v);
                }
            }
            for (pk, pairs) in param_key_to_pairs {
                d.params.push(
                    OwnedMutatorParamDescriptor::try_from_param_key_and_attributes(pk, pairs)?,
                )
            }

            // TODO, later, - organization-custom-metadata using `custom_bucket`

            Ok(d)
        }
    }
    #[derive(Debug, thiserror::Error, Eq, PartialEq)]
    pub enum ParamDescriptorFromAttrsError {
        #[error("Missing the `mutator.params.<param-key>.name` attribute")]
        MissingParameterNameAttribute,
        #[error("Missing the `mutator.params.<param-key>.value_type` attribute")]
        MissingValueTypeAttribute,
        #[error("Invalid parameter key. Parameter keys must be ASCII with no periods.")]
        InvalidParameterKey,
    }

    impl MutatorDescriptor for OwnedMutatorDescriptor {
        fn get_description_attributes(&self) -> Box<dyn Iterator<Item = (AttrKey, AttrVal)> + '_> {
            let mut all_mutator_attrs: Vec<(AttrKey, AttrVal)> = vec![];
            if let Some(mutator_name) = self.name.as_ref() {
                all_mutator_attrs.push((attrs::mutator::NAME, mutator_name.into()))
            }
            if let Some(mutator_description) = self.description.as_ref() {
                all_mutator_attrs.push((attrs::mutator::DESCRIPTION, mutator_description.into()))
            }
            if let Some(mutator_layer) = self.layer.as_ref() {
                all_mutator_attrs.push((attrs::mutator::LAYER, mutator_layer.name().into()))
            }
            if let Some(mutator_group) = self.group.as_ref() {
                all_mutator_attrs.push((attrs::mutator::GROUP, mutator_group.into()))
            }
            if let Some(mutator_operation) = self.operation.as_ref() {
                all_mutator_attrs.push((attrs::mutator::OPERATION, mutator_operation.name().into()))
            }
            if let Some(organization_custom_metadata) = self.organization_custom_metadata.as_ref() {
                let mut mutator_level_custom_metadata_prefix = "mutator.".to_string();
                mutator_level_custom_metadata_prefix.push_str(
                    organization_custom_metadata
                        .organization_name_segment
                        .as_str(),
                );
                mutator_level_custom_metadata_prefix.push('.');
                for (k, v) in organization_custom_metadata.attributes.iter() {
                    all_mutator_attrs.push((
                        AttrKey::from(format!("{mutator_level_custom_metadata_prefix}{k}")),
                        v.clone(),
                    ));
                }
            }

            for param in &self.params {
                all_mutator_attrs.extend(param.mutator_params_param_key_prefixed_attributes());
            }

            Box::new(all_mutator_attrs.into_iter())
        }
    }

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub enum MutatorLayer {
        Implementational,
        Operational,
        Environmental,
    }

    impl MutatorLayer {
        pub fn name(&self) -> &'static str {
            match self {
                MutatorLayer::Implementational => "implementational",
                MutatorLayer::Operational => "operational",
                MutatorLayer::Environmental => "environmental",
            }
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> pyo3::FromPyObject<'py> for MutatorLayer {
        fn extract_bound(
            ob: &pyo3::prelude::Bound<'py, pyo3::prelude::PyAny>,
        ) -> pyo3::prelude::PyResult<Self> {
            use pyo3::prelude::*;

            if let Ok(s) = ob.extract::<String>() {
                if s == "implementational" {
                    return Ok(MutatorLayer::Implementational);
                }
                if s == "operational" {
                    return Ok(MutatorLayer::Operational);
                }
                if s == "environmental" {
                    return Ok(MutatorLayer::Environmental);
                }
            }
            Err(pyo3::exceptions::PyValueError::new_err(
                "ValueDistributionKind must be one of \"implementational\", \"operational\", or \"environmental\". ",
            ))
        }
    }

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub enum MutatorStatefulness {
        /// Sticks. Has effect immediately and continuously. Stays until explicitly told to leave.
        Permanent,
        /// Sticks. Has effect zero or more times at some point in the future.
        /// Stays a possibility until explicitly told to leave.
        Intermittent,
        /// Sticks for a bit, probably goes away on its own.
        /// given regular system operations.
        Transient,
    }

    impl MutatorStatefulness {
        pub fn name(&self) -> &'static str {
            match self {
                MutatorStatefulness::Permanent => "permanent",
                MutatorStatefulness::Intermittent => "intermittent",
                MutatorStatefulness::Transient => "transient",
            }
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> pyo3::FromPyObject<'py> for MutatorStatefulness {
        fn extract_bound(
            ob: &pyo3::prelude::Bound<'py, pyo3::prelude::PyAny>,
        ) -> pyo3::prelude::PyResult<Self> {
            use pyo3::prelude::*;

            if let Ok(s) = ob.extract::<String>() {
                if s == "permanent" {
                    return Ok(MutatorStatefulness::Permanent);
                }
                if s == "intermittent" {
                    return Ok(MutatorStatefulness::Intermittent);
                }
                if s == "transient" {
                    return Ok(MutatorStatefulness::Transient);
                }
            }

            Err(pyo3::exceptions::PyValueError::new_err(
                "ValueDistributionKind must be one of \"permanent\", \"intermittent\", or \"transient\". ",
            ))
        }
    }

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub enum MutatorOperation {
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

    impl MutatorOperation {
        pub fn name(&self) -> &'static str {
            match self {
                MutatorOperation::Delay => "delay",
                MutatorOperation::Duplicate => "duplicate",
                MutatorOperation::DropFraction => "drop_fraction",
                MutatorOperation::DropPositional => "drop_positional",
                MutatorOperation::Disable => "disable",
                MutatorOperation::Enable => "enable",
                MutatorOperation::Corrupt => "corrupt",
                MutatorOperation::SetToValue => "set_to_value",
                MutatorOperation::SubstituteNextValue => "substitute_next_value",
                MutatorOperation::Reorder => "reorder",
                MutatorOperation::Stimulate => "stimulate",
            }
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> pyo3::FromPyObject<'py> for MutatorOperation {
        fn extract_bound(
            ob: &pyo3::prelude::Bound<'py, pyo3::prelude::PyAny>,
        ) -> pyo3::prelude::PyResult<Self> {
            use pyo3::prelude::*;

            if let Ok(s) = ob.extract::<String>() {
                if s == "delay" {
                    return Ok(MutatorOperation::Delay);
                }
                if s == "duplicate" {
                    return Ok(MutatorOperation::Duplicate);
                }
                if s == "drop_fraction" {
                    return Ok(MutatorOperation::DropFraction);
                }
                if s == "drop_positional" {
                    return Ok(MutatorOperation::DropPositional);
                }
                if s == "disable" {
                    return Ok(MutatorOperation::Disable);
                }
                if s == "enable" {
                    return Ok(MutatorOperation::Enable);
                }
                if s == "corrupt" {
                    return Ok(MutatorOperation::Corrupt);
                }
                if s == "set_to_value" {
                    return Ok(MutatorOperation::SetToValue);
                }
                if s == "substitute_next_value" {
                    return Ok(MutatorOperation::SubstituteNextValue);
                }
                if s == "reorder" {
                    return Ok(MutatorOperation::Reorder);
                }
                if s == "stimulate" {
                    return Ok(MutatorOperation::Stimulate);
                }
            }

            Err(pyo3::exceptions::PyValueError::new_err(
                "ValueDistributionKind must be one of: \"delay\", \"duplicate\", \"drop_fraction\", \"drop_positional\", \"disable\", \"enable\", \"corrupt\", \"set_to_value\", \"substitute_next_value\", \"reorder\", \"stimulate\"",
            ))
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct OrganizationCustomMetadata {
        /// Expected to be ASCII and not contain any periods.
        organization_name_segment: String,
        /// Note that we do not expect the keys to be prefixed with anything in particular.
        pub attributes: HashMap<String, AttrVal>,
    }

    impl OrganizationCustomMetadata {
        pub fn empty(organization_name_segment: String) -> Option<Self> {
            if is_valid_single_key_segment_contents(organization_name_segment.as_str()) {
                Some(OrganizationCustomMetadata {
                    organization_name_segment,
                    attributes: Default::default(),
                })
            } else {
                None
            }
        }
        pub fn new(
            organization_name_segment: String,
            attributes: HashMap<String, AttrVal>,
        ) -> Option<Self> {
            if is_valid_single_key_segment_contents(organization_name_segment.as_str()) {
                Some(OrganizationCustomMetadata {
                    organization_name_segment,
                    attributes,
                })
            } else {
                None
            }
        }

        pub fn organization_name_segment(&self) -> &str {
            self.organization_name_segment.as_str()
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct OwnedMutatorParamDescriptor {
        pub value_type: AttrType,
        /// This is used as the parameter key interfix for parameter-specific attributes
        /// and as the value associated with the `mutator.params.<param-key>.name attribute`
        pub name: String,
        pub description: Option<String>,
        pub value_min: Option<AttrVal>,
        pub value_max: Option<AttrVal>,
        pub default_value: Option<AttrVal>,
        pub least_effect_value: Option<AttrVal>,
        pub value_distribution_kind: Option<ValueDistributionKind>,
        pub value_distribution_scaling: Option<ValueDistributionScaling>,
        pub value_distribution_option_set: Option<BTreeMap<String, AttrVal>>,
        pub organization_custom_metadata: Option<OrganizationCustomMetadata>,
    }

    impl Default for OwnedMutatorParamDescriptor {
        fn default() -> Self {
            Self {
                value_type: AttrType::Integer,
                name: "".to_string(),
                description: Default::default(),
                value_min: Default::default(),
                value_max: Default::default(),
                default_value: Default::default(),
                least_effect_value: Default::default(),
                value_distribution_kind: Default::default(),
                value_distribution_scaling: Default::default(),
                value_distribution_option_set: Default::default(),
                organization_custom_metadata: Default::default(),
            }
        }
    }

    impl OwnedMutatorParamDescriptor {
        pub(crate) fn mutator_params_param_key_prefixed_attributes(
            &self,
        ) -> impl Iterator<Item = (AttrKey, AttrVal)> {
            let mut param_attrs: Vec<(AttrKey, AttrVal)> = vec![];
            let mut param_prefix =
                crate::mutator_protocol::params_attributes::MUTATOR_PARAMS_PREFIX.to_string();
            param_prefix.push_str(self.name.as_str());
            // The period delimiting the segment is in the various constant suffices, don't add yet
            param_attrs.push((
                AttrKey::from(format!(
                    "{param_prefix}{}",
                    crate::mutator_protocol::params_attributes::MUTATOR_PARAMS_NAME_SUFFIX
                )),
                self.name.as_str().into(),
            ));
            param_attrs.push((
                AttrKey::from(format!(
                    "{param_prefix}{}",
                    crate::mutator_protocol::params_attributes::MUTATOR_PARAMS_VALUE_TYPE_SUFFIX
                )),
                self.value_type.to_string().into(),
            ));

            if let Some(param_description) = self.description.as_ref() {
                param_attrs.push((
                    AttrKey::from(format!(
                        "{param_prefix}{}",
                        crate::mutator_protocol::params_attributes::MUTATOR_PARAMS_DESCRIPTION_SUFFIX
                    )),
                    param_description.into(),
                ));
            }
            if let Some(value_min) = self.value_min.as_ref() {
                param_attrs.push((
                    AttrKey::from(format!(
                        "{param_prefix}{}",
                        crate::mutator_protocol::params_attributes::MUTATOR_PARAMS_VALUE_MIN_SUFFIX
                    )),
                    value_min.clone(),
                ));
            }
            if let Some(value_max) = self.value_max.as_ref() {
                param_attrs.push((
                    AttrKey::from(format!(
                        "{param_prefix}{}",
                        crate::mutator_protocol::params_attributes::MUTATOR_PARAMS_VALUE_MAX_SUFFIX
                    )),
                    value_max.clone(),
                ));
            }
            if let Some(default_value) = self.default_value.as_ref() {
                param_attrs.push((
                    AttrKey::from(format!(
                        "{param_prefix}{}",
                        crate::mutator_protocol::params_attributes::MUTATOR_PARAMS_DEFAULT_VALUE_SUFFIX
                    )),
                    default_value.clone(),
                ));
            }
            if let Some(least_effect_value) = self.least_effect_value.as_ref() {
                param_attrs.push((
                    AttrKey::from(format!(
                        "{param_prefix}{}",
                        crate::mutator_protocol::params_attributes::MUTATOR_PARAMS_LEAST_EFFECT_VALUE_SUFFIX
                    )),
                    least_effect_value.clone(),
                ));
            }

            if let Some(value_distribution_kind) = self.value_distribution_kind.as_ref() {
                param_attrs.push((
                    AttrKey::from(format!(
                        "{param_prefix}{}",
                        crate::mutator_protocol::params_attributes::MUTATOR_PARAMS_VALUE_DISTRIBUTION_KIND_SUFFIX
                    )),
                    value_distribution_kind.name().into(),
                ));
            }
            if let Some(value_distribution_scaling) = self.value_distribution_scaling.as_ref() {
                param_attrs.push((
                    AttrKey::from(format!(
                        "{param_prefix}{}",
                        crate::mutator_protocol::params_attributes::MUTATOR_PARAMS_VALUE_DISTRIBUTION_SCALING_SUFFIX
                    )),
                    value_distribution_scaling.name().into(),
                ));
            }
            if let Some(option_set) = self.value_distribution_option_set.as_ref() {
                // TODO - more correct-by-construction / insertion checking that the option set keys
                // are correctly formed (no leading periods, no duplication of param prefix bits)
                for (option_key_segment, option_val) in option_set {
                    param_attrs.push((AttrKey::from(format!("{param_prefix}{}{}", crate::mutator_protocol::params_attributes::MUTATOR_PARAMS_VALUE_DISTRIBUTION_OPTION_SET_INTERFIX, option_key_segment)), option_val.clone()));
                }
            }

            if let Some(organization_custom_metadata) = self.organization_custom_metadata.as_ref() {
                let mut parameter_level_custom_metadata_prefix = param_prefix.clone();
                parameter_level_custom_metadata_prefix.push_str(
                    organization_custom_metadata
                        .organization_name_segment
                        .as_str(),
                );
                parameter_level_custom_metadata_prefix.push('.');
                for (k, v) in organization_custom_metadata.attributes.iter() {
                    param_attrs.push((
                        AttrKey::from(format!("{parameter_level_custom_metadata_prefix}{k}")),
                        v.clone(),
                    ));
                }
            }

            param_attrs.into_iter()
        }

        /// Assumes that the attributes are of the form `mutator.params.<param-key>.the.rest`
        pub(crate) fn try_from_param_key_and_attributes(
            param_key: String,
            attributes: BTreeMap<AttrKey, AttrVal>,
        ) -> Result<Self, ParamDescriptorFromAttrsError> {
            if !is_valid_param_key(&param_key) {
                return Err(ParamDescriptorFromAttrsError::InvalidParameterKey);
            }
            let mut value_type: Option<AttrType> = None;
            let mut name: Option<String> = None;
            let mut description: Option<String> = None;
            let mut value_min: Option<AttrVal> = None;
            let mut value_max: Option<AttrVal> = None;
            let mut default_value: Option<AttrVal> = None;
            let mut least_effect_value: Option<AttrVal> = None;
            let mut value_distribution_kind: Option<ValueDistributionKind> = None;
            let mut value_distribution_scaling: Option<ValueDistributionScaling> = None;
            let mut value_distribution_option_set: Option<BTreeMap<String, AttrVal>> = None;
            // TODO, later
            let organization_custom_metadata: Option<OrganizationCustomMetadata> = None;

            let params_prefix = format!("mutator.params.{param_key}");
            for (k, v) in attributes {
                if let Some(post_key_with_period) = k.as_ref().strip_prefix(&params_prefix) {
                    if post_key_with_period == MUTATOR_PARAMS_NAME_SUFFIX {
                        if let AttrVal::String(s) = v {
                            name = Some(s.to_string());
                        }
                    } else if post_key_with_period == MUTATOR_PARAMS_VALUE_TYPE_SUFFIX {
                        if let AttrVal::String(s) = v {
                            value_type = match s.as_ref() {
                                "TimelineId" => Some(AttrType::TimelineId),
                                "String" => Some(AttrType::String),
                                "Integer" => Some(AttrType::Integer),
                                "BigInteger" => Some(AttrType::BigInt),
                                "Float" => Some(AttrType::Float),
                                "Bool" => Some(AttrType::Bool),
                                "Nanoseconds" => Some(AttrType::Nanoseconds),
                                "LogicalTime" => Some(AttrType::LogicalTime),
                                "Any" => Some(AttrType::Any),
                                "Coordinate" => Some(AttrType::EventCoordinate),
                                _ => None,
                            }
                        }
                    } else if post_key_with_period == MUTATOR_PARAMS_DESCRIPTION_SUFFIX {
                        if let AttrVal::String(s) = v {
                            description = Some(s.to_string());
                        }
                    } else if post_key_with_period == MUTATOR_PARAMS_VALUE_MIN_SUFFIX {
                        value_min = Some(v);
                    } else if post_key_with_period == MUTATOR_PARAMS_VALUE_MAX_SUFFIX {
                        value_max = Some(v);
                    } else if post_key_with_period == MUTATOR_PARAMS_DEFAULT_VALUE_SUFFIX {
                        default_value = Some(v);
                    } else if post_key_with_period == MUTATOR_PARAMS_LEAST_EFFECT_VALUE_SUFFIX {
                        least_effect_value = Some(v);
                    } else if post_key_with_period == MUTATOR_PARAMS_VALUE_DISTRIBUTION_KIND_SUFFIX
                    {
                        if let AttrVal::String(s) = v {
                            value_distribution_kind = match s.as_ref() {
                                "continuous" => Some(ValueDistributionKind::Continuous),
                                "discrete" => Some(ValueDistributionKind::Discrete),
                                _ => None,
                            };
                        }
                    } else if post_key_with_period
                        == MUTATOR_PARAMS_VALUE_DISTRIBUTION_SCALING_SUFFIX
                    {
                        if let AttrVal::String(s) = v {
                            value_distribution_scaling = match s.as_ref() {
                                "linear" => Some(ValueDistributionScaling::Linear),
                                "complex" => Some(ValueDistributionScaling::Complex),
                                "circular" => Some(ValueDistributionScaling::Circular),
                                _ => None,
                            };
                        }
                    } else if let Some(option_set_member_key) = post_key_with_period
                        .strip_prefix(MUTATOR_PARAMS_VALUE_DISTRIBUTION_OPTION_SET_INTERFIX)
                    {
                        let mut option_set =
                            value_distribution_option_set.take().unwrap_or_default();
                        option_set.insert(option_set_member_key.to_string(), v);
                        value_distribution_option_set = Some(option_set)
                    }
                }
            }
            // TODO, later:
            // units

            let d = OwnedMutatorParamDescriptor {
                value_type: if let Some(vt) = value_type {
                    vt
                } else {
                    return Err(ParamDescriptorFromAttrsError::MissingValueTypeAttribute);
                },
                name: if let Some(n) = name {
                    n
                } else {
                    return Err(ParamDescriptorFromAttrsError::MissingParameterNameAttribute);
                },
                description,
                value_min,
                value_max,
                default_value,
                least_effect_value,
                value_distribution_kind,
                value_distribution_scaling,
                value_distribution_option_set,
                organization_custom_metadata,
            };
            Ok(d)
        }
    }

    impl OwnedMutatorParamDescriptor {
        /// `name` is used as the parameter key interfix for parameter-specific attributes
        /// and as the value associated with the `mutator.params.<param-key>.name attribute`
        /// and thus must be a valid single segment of an attribute key (ASCII, no periods).
        pub fn new(value_type: AttrType, name: String) -> Option<Self> {
            if is_valid_single_key_segment_contents(name.as_str()) {
                Some(OwnedMutatorParamDescriptor {
                    value_type,
                    name,
                    description: None,
                    value_min: None,
                    value_max: None,
                    default_value: None,
                    least_effect_value: None,
                    value_distribution_kind: None,
                    value_distribution_scaling: None,
                    value_distribution_option_set: None,
                    organization_custom_metadata: None,
                })
            } else {
                None
            }
        }

        pub fn with_description(mut self, s: &str) -> Self {
            self.description = Some(s.to_owned());
            self
        }

        pub fn with_value_min(mut self, val: impl Into<AttrVal>) -> Self {
            self.value_min = Some(val.into());
            self
        }

        pub fn with_value_max(mut self, val: impl Into<AttrVal>) -> Self {
            self.value_max = Some(val.into());
            self
        }

        pub fn with_default_value(mut self, val: impl Into<AttrVal>) -> Self {
            self.default_value = Some(val.into());
            self
        }

        pub fn with_least_effect_value(mut self, val: impl Into<AttrVal>) -> Self {
            self.least_effect_value = Some(val.into());
            self
        }

        pub fn with_value_distribution_kind(mut self, kind: ValueDistributionKind) -> Self {
            self.value_distribution_kind = Some(kind);
            self
        }

        pub fn with_value_distribution_scaling(
            mut self,
            scaling: ValueDistributionScaling,
        ) -> Self {
            self.value_distribution_scaling = Some(scaling);
            self
        }

        pub fn with_value_distribution_option(mut self, key: &str, val: AttrVal) -> Self {
            if self.value_distribution_option_set.is_none() {
                self.value_distribution_option_set = Some(Default::default());
            }

            self.value_distribution_option_set
                .as_mut()
                .unwrap()
                .insert(key.to_owned(), val);

            self
        }
    }

    #[derive(Debug, Copy, Clone, Eq, PartialEq)]
    pub enum ValueDistributionKind {
        Continuous,
        Discrete,
    }

    impl ValueDistributionKind {
        pub fn name(&self) -> &'static str {
            match self {
                ValueDistributionKind::Continuous => "continuous",
                ValueDistributionKind::Discrete => "discrete",
            }
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> pyo3::FromPyObject<'py> for ValueDistributionKind {
        fn extract_bound(
            ob: &pyo3::prelude::Bound<'py, pyo3::prelude::PyAny>,
        ) -> pyo3::prelude::PyResult<Self> {
            use pyo3::prelude::*;

            if let Ok(s) = ob.extract::<String>() {
                if s == "continuous" {
                    return Ok(ValueDistributionKind::Continuous);
                }
                if s == "discrete" {
                    return Ok(ValueDistributionKind::Discrete);
                }
            }

            Err(pyo3::exceptions::PyValueError::new_err(
                "ValueDistributionKind must be one of \"continuous\" or \"discrete\". ",
            ))
        }
    }

    #[derive(Debug, Copy, Clone, PartialEq)]
    pub enum ValueDistributionScaling {
        Linear,
        Complex,
        Circular,
    }
    impl ValueDistributionScaling {
        pub fn name(&self) -> &'static str {
            match self {
                ValueDistributionScaling::Linear => "linear",
                ValueDistributionScaling::Complex => "complex",
                ValueDistributionScaling::Circular => "circular",
            }
        }
    }

    #[cfg(feature = "pyo3")]
    impl<'py> pyo3::FromPyObject<'py> for ValueDistributionScaling {
        fn extract_bound(
            ob: &pyo3::prelude::Bound<'py, pyo3::prelude::PyAny>,
        ) -> pyo3::prelude::PyResult<Self> {
            use pyo3::prelude::*;

            if let Ok(s) = ob.extract::<String>() {
                if s == "linear" {
                    return Ok(ValueDistributionScaling::Linear);
                }
                if s == "complex" {
                    return Ok(ValueDistributionScaling::Complex);
                }
                if s == "discrete" {
                    return Ok(ValueDistributionScaling::Circular);
                }
            }

            Err(pyo3::exceptions::PyValueError::new_err(
                "ValueDistributionScaling must be one of \"linear\", \"complex\", or \"circular\". ",
            ))
        }
    }
}
