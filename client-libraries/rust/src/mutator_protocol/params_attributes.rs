pub const MUTATOR_PARAMS_PREFIX: &str = "mutator.params.";

pub const MUTATOR_PARAMS_NAME_SUFFIX: &str = ".name";
pub const MUTATOR_PARAMS_VALUE_TYPE_SUFFIX: &str = ".value_type";

pub const MUTATOR_PARAMS_DESCRIPTION_SUFFIX: &str = ".description";
pub const MUTATOR_PARAMS_VALUE_MIN_SUFFIX: &str = ".value_min";
pub const MUTATOR_PARAMS_VALUE_MAX_SUFFIX: &str = ".value_max";
pub const MUTATOR_PARAMS_UNITS_SUFFIX: &str = ".units";
pub const MUTATOR_PARAMS_DEFAULT_VALUE_SUFFIX: &str = ".default_value";
pub const MUTATOR_PARAMS_LEAST_EFFECT_VALUE_SUFFIX: &str = ".least_effect_value";

pub const MUTATOR_PARAMS_VALUE_DISTRIBUTION_INTERFIX: &str = ".value_distribution.";
pub const MUTATOR_PARAMS_VALUE_DISTRIBUTION_KIND_SUFFIX: &str = ".value_distribution.kind";
pub const MUTATOR_PARAMS_VALUE_DISTRIBUTION_SCALING_SUFFIX: &str = ".value_distribution.scaling";

pub const MUTATOR_PARAMS_VALUE_DISTRIBUTION_OPTION_SET_SUFFIX: &str =
    ".value_distribution.option_set";
pub const MUTATOR_PARAMS_VALUE_DISTRIBUTION_OPTION_SET_INTERFIX: &str =
    ".value_distribution.option_set.";

/// Mutator parameter-specific attributes have keys in the format:
/// `mutator.params.<param-key>.rest.of.key`
/// where `<param-key>` must be ASCII and not contain any periods.
pub fn is_valid_param_key(s: &str) -> bool {
    is_valid_single_key_segment_contents(s)
}

/// Must be ASCII and contain no periods
pub fn is_valid_single_key_segment_contents(s: &str) -> bool {
    if !s.is_ascii() {
        return false;
    }
    !s.contains('.')
}
