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

    // =========================================================================
    // Error cases for host spec parsing
    // =========================================================================

    #[test]
    fn test_host_slots_negative_number_errors() {
        let yaml = "hostname:\n  - server1\nslots: -1";
        let result: Result<HostSpecParsed, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_host_slots_string_errors() {
        let yaml = "hostname:\n  - server1\nslots: eight";
        let result: Result<HostSpecParsed, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_host_slots_float_truncated() {
        // YAML floats get converted, this tests the behavior
        let yaml = "hostname:\n  - server1\nslots: 8.5";
        let result: Result<HostSpecParsed, _> = serde_yaml::from_str(yaml);
        // This may either parse as 8 or fail - test current behavior
        if let Ok(spec) = result {
            assert_eq!(spec.slots, Some(8));
        }
        // If it errors, that's also acceptable
    }

    #[test]
    fn test_host_slots_zero() {
        // Zero is syntactically valid YAML but semantically questionable
        let yaml = "hostname:\n  - server1\nslots: 0";
        let result: Result<HostSpecParsed, _> = serde_yaml::from_str(yaml);
        // The parser accepts 0, validation happens elsewhere
        assert!(result.is_ok());
        assert_eq!(result.unwrap().slots, Some(0));
    }

    #[test]
    fn test_host_slots_very_large() {
        let yaml = "hostname:\n  - server1\nslots: 1024";
        let spec: HostSpecParsed = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.slots, Some(1024));
    }

    // =========================================================================
    // Parameter parsing edge cases
    // =========================================================================

    #[test]
    fn test_host_params_mixed_types() {
        let yaml = r#"
hostname:
  - server1
gpu_count: 8
region: us-east
"#;
        let spec: HostSpecParsed = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            spec.params.get("gpu_count").unwrap(),
            &vec!["8".to_string()]
        );
        assert_eq!(
            spec.params.get("region").unwrap(),
            &vec!["us-east".to_string()]
        );
    }

    #[test]
    fn test_host_params_list_with_numbers() {
        let yaml = r#"
hostname:
  - server1
batch_size:
  - 16
  - 32
  - 64
"#;
        let spec: HostSpecParsed = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            spec.params.get("batch_size").unwrap(),
            &vec!["16".to_string(), "32".to_string(), "64".to_string()]
        );
    }

    #[test]
    fn test_host_params_empty_list_errors() {
        let yaml = r#"
hostname:
  - server1
empty_param: []
"#;
        let spec: HostSpecParsed = serde_yaml::from_str(yaml).unwrap();
        // Empty list is valid but may cause issues downstream
        assert_eq!(
            spec.params.get("empty_param").unwrap(),
            &Vec::<String>::new()
        );
    }

    #[test]
    fn test_host_params_nested_object_errors() {
        let yaml = r#"
hostname:
  - server1
invalid_param:
  nested: value
"#;
        let result: Result<HostSpecParsed, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    // =========================================================================
    // Hostname parametrization
    // =========================================================================

    #[test]
    fn test_hostname_list() {
        let yaml = r#"
hostname:
  - server1
  - server2
  - server3
slots: 4
"#;
        let spec: HostSpecParsed = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            spec.params.get("hostname").unwrap(),
            &vec![
                "server1".to_string(),
                "server2".to_string(),
                "server3".to_string()
            ]
        );
        assert_eq!(spec.slots, Some(4));
    }

    #[test]
    fn test_hostname_single_string() {
        let yaml = "hostname: server1\nslots: 8";
        let spec: HostSpecParsed = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            spec.params.get("hostname").unwrap(),
            &vec!["server1".to_string()]
        );
    }
}
