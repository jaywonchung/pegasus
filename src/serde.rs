use std::collections::HashMap;
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

/// Parsed host specification with slots as scalar.
#[derive(Debug, Clone)]
pub struct HostSpecParsed {
    pub slots: Option<usize>,
    pub params: HashMap<String, Vec<String>>,
}

impl<'de> Deserialize<'de> for HostSpecParsed {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct HostSpecVisitor;

        impl<'de> Visitor<'de> for HostSpecVisitor {
            type Value = HostSpecParsed;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a mapping with optional slots and params")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut slots: Option<usize> = None;
                let mut params: HashMap<String, Vec<String>> = HashMap::new();

                while let Some(key) = map.next_key::<String>()? {
                    if key == "slots" {
                        let value: serde_yaml::Value = map.next_value()?;
                        match value {
                            serde_yaml::Value::Number(n) => {
                                slots = Some(n.as_u64().ok_or_else(|| {
                                    de::Error::custom("slots must be a positive integer")
                                })? as usize);
                            }
                            serde_yaml::Value::Sequence(seq) if seq.len() == 1 => {
                                // Allow single-element list for backwards compatibility
                                if let Some(serde_yaml::Value::Number(n)) = seq.first() {
                                    slots = Some(n.as_u64().ok_or_else(|| {
                                        de::Error::custom("slots must be a positive integer")
                                    })? as usize);
                                } else {
                                    return Err(de::Error::custom("slots must be a number"));
                                }
                            }
                            serde_yaml::Value::Sequence(_) => {
                                return Err(de::Error::custom(
                                    "slots must be a single number, not a list",
                                ));
                            }
                            _ => {
                                return Err(de::Error::custom("slots must be a number"));
                            }
                        }
                    } else {
                        let value: serde_yaml::Value = map.next_value()?;
                        let vec = match value {
                            serde_yaml::Value::String(s) => vec![s],
                            serde_yaml::Value::Number(n) => vec![n.to_string()],
                            serde_yaml::Value::Sequence(seq) => seq
                                .into_iter()
                                .map(|v| match v {
                                    serde_yaml::Value::String(s) => Ok(s),
                                    serde_yaml::Value::Number(n) => Ok(n.to_string()),
                                    _ => Err(de::Error::custom(
                                        "list elements must be strings or numbers",
                                    )),
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                            _ => {
                                return Err(de::Error::custom(
                                    "parameter values must be strings, numbers, or lists",
                                ));
                            }
                        };
                        params.insert(key, vec);
                    }
                }

                Ok(HostSpecParsed { slots, params })
            }
        }

        deserializer.deserialize_map(HostSpecVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_slots_scalar() {
        let yaml = "hostname:\n  - server1\nslots: 8";
        let spec: HostSpecParsed = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.slots, Some(8));
    }

    #[test]
    fn test_host_slots_single_element_list() {
        let yaml = "hostname:\n  - server1\nslots:\n  - 8";
        let spec: HostSpecParsed = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.slots, Some(8));
    }

    #[test]
    fn test_host_slots_multi_element_list_errors() {
        let yaml = "hostname:\n  - server1\nslots:\n  - 8\n  - 4";
        let result: Result<HostSpecParsed, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("single number"));
    }

    #[test]
    fn test_host_slots_omitted_defaults_to_none() {
        let yaml = "hostname:\n  - server1";
        let spec: HostSpecParsed = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.slots, None);
    }
}
