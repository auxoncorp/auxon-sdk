/// A serde helper to deserialize string-based values via FromStr.
///
/// This is specifically required due to a flaw in the Envy crate (which is used for
/// loading configuration from environment variables), when using `#[serde(flatten)]`
/// to split out a 'common' configuration struture which is shared between different
/// main binaries (like a collector and an importer).
///
/// This works for primitive bool and numeric types only. It is not required for strings.
///
/// Example usage:
///
/// ```no_run
/// use serde::{Deserialize, Serialize};
/// #[derive(Serialize, Deserialize)]
/// pub struct CollectorConfig {
///     #[serde(flatten)]
///     pub common: CommonConfig
/// }
///
/// #[derive(Serialize, Deserialize)]
/// pub struct CommonConfig {
///     #[serde(default, deserialize_with="auxon_sdk::plugin_utils::serde::from_str")]
///     pub some_val: Option<bool>
/// }
/// ```
///
/// `envy` crate issue: https://github.com/softprops/envy/issues/26
pub fn from_str<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::de::Deserializer<'de>,
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    deserializer.deserialize_any(StringifiedOptionAnyVisitor(std::marker::PhantomData))
}

pub struct StringifiedOptionAnyVisitor<T>(std::marker::PhantomData<T>);

impl<'de, T> serde::de::Visitor<'de> for StringifiedOptionAnyVisitor<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    type Value = Option<T>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string compatible with from_str")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Some(T::from_str(v).map_err(E::custom)?))
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(&v.to_string())
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(&v.to_string())
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(&v.to_string())
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(&v.to_string())
    }
}
