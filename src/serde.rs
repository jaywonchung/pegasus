use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use serde::de::{self, MapAccess, Visitor, value::MapAccessDeserializer};
use serde::{Deserialize, Deserializer};
use void::Void;

/// A Visitor implementation that is able to parse either a bare string or a map.
/// https://serde.rs/string-or-struct.html
pub fn string_or_mapping<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + FromStr<Err = Void>,
    D: Deserializer<'de>,
{
    struct StringOrMapping<T>(PhantomData<fn() -> T>);

    impl<'de, T> Visitor<'de> for StringOrMapping<T>
    where
        T: Deserialize<'de> + FromStr<Err = Void>,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or mapping")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(FromStr::from_str(value).unwrap())
        }

        fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            Deserialize::deserialize(MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(StringOrMapping(PhantomData))
}
