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
/// ```
/// #[derive(Serialize, Deserialize)]
/// pub struct CollectorConfig {
///     #[serde(flatten)]
///     pub common: CommonConfig
/// }
///
/// #[derive(Serialize, Deserialize)]
/// pub struct CommonConfig {
///     #[serde(Default, deserialize_with="auxon_sdk::plugin_utils::serde::from_str")]
///     pub some_val: Option<bool>
/// }
/// ```
///
/// `envy` crate issue: https://github.com/softprops/envy/issues/26
pub fn from_str<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: de::Deserializer<'de>,
    T: FromStr,
    T::Err: Display,
{
    deserializer.deserialize_any(StringifiedOptionAnyVisitor(PhantomData))
}

pub struct StringifiedOptionAnyVisitor<T>(PhantomData<T>);

impl<'de, T> de::Visitor<'de> for StringifiedOptionAnyVisitor<T>
where
    T: FromStr,
    T::Err: Display,
{
    type Value = Option<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string compatible with from_str")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Some(T::from_str(v).map_err(E::custom)?))
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&v.to_string())
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&v.to_string())
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&v.to_string())
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&v.to_string())
    }
}
