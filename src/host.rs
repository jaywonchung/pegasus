//! SSH Hosts.
//!
//! One SSH connection is created for one `Host`. Each connection will run commands in parallel with
//! other connections in its own tokio task.

use colourado::Color;
use itertools::sorted;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::str::FromStr;

use colored::*;
use serde::Deserialize;
use void::Void;

use crate::serde::string_or_mapping;

#[derive(Debug, Clone)]
pub struct Host {
    /// SSH hostname to connect to.
    pub hostname: String,
    /// Sub-host parameters used to fill in templates.
    pub params: HashMap<String, String>,
}

impl Host {
    fn new(hostname: String) -> Self {
        Self {
            hostname,
            params: HashMap::new(),
        }
    }

    /// For pretty-printing the host name.
    /// Surrounds with brackets and colors it with a random color.
    pub fn prettify(&self, color: Color) -> ColoredString {
        let r = (color.red * 256.0) as u8;
        let g = (color.green * 256.0) as u8;
        let b = (color.blue * 256.0) as u8;
        format!("{}", self).truecolor(r, g, b)
    }
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.params.is_empty() {
            write!(f, "[{}]", self.hostname)?;
        } else {
            write!(f, "[{} (", self.hostname)?;
            for (i, (key, value)) in sorted(self.params.iter()).enumerate() {
                if i == 0 {
                    write!(f, "{}={}", key, value)?;
                } else {
                    write!(f, ",{}={}", key, value)?;
                }
            }
            write!(f, ")]")?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct HostSpec(#[serde(deserialize_with = "string_or_mapping")] HostSpecInner);

#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct HostSpecInner(HashMap<String, Vec<String>>);

impl FromStr for HostSpecInner {
    type Err = Void;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut map = HashMap::new();
        map.insert("hostname".to_string(), vec![s.to_string()]);
        Ok(Self(map))
    }
}

pub fn get_hosts(hosts_file: &str) -> Vec<Host> {
    // Read and parse the host file to a vector of HostSpec objects.
    let hosts_fd =
        File::open(hosts_file).unwrap_or_else(|_| panic!("Failed to open {}", hosts_file));
    let host_specs: Vec<HostSpec> = serde_yaml::from_reader(hosts_fd)
        .unwrap_or_else(|_| panic!("Failed to parse {}", hosts_file));

    // All entries that are parametrized (i.e., not bare strings)
    // should have the key 'hostname'.
    if !host_specs
        .iter()
        .all(|spec| spec.0 .0.contains_key("hostname"))
    {
        panic!(
            "A parametrized entry in {} is missing the 'hostname' key.",
            hosts_file
        );
    }

    // Cartesian product.
    let mut hosts = Vec::with_capacity(host_specs.len());
    // Each spec is completely independent. Each spec will expand to `expanded` and they'll
    // simply concatenate to form the final `hosts` vector.
    for HostSpec(HostSpecInner(mut spec)) in host_specs {
        let mut expanded = vec![];
        // We checked before that every `spec` has the key 'hostname'.
        for hostname in spec.remove("hostname").unwrap() {
            expanded.push(Host::new(hostname));
        }
        // For each parameter (String -> Vec<String>), the size of `expanded` will increase
        // by the number of possible values.
        for (key, values) in spec {
            let mut part_expanded = Vec::with_capacity(values.len());
            for host in expanded {
                for value in values.iter() {
                    let mut host = host.clone();
                    host.params.insert(key.clone(), value.clone());
                    part_expanded.push(host);
                }
            }
            expanded = part_expanded;
        }
        // Fill in parameters with Handlebars.
        let mut registry = handlebars::Handlebars::new();
        handlebars_misc_helpers::register(&mut registry);
        for host in expanded.iter_mut() {
            if !registry.has_template(&host.hostname) {
                registry
                    .register_template_string(&host.hostname, &host.hostname)
                    .unwrap();
            }
            host.hostname = registry.render(&host.hostname, &host.params).unwrap();
        }
        hosts.push(expanded);
    }

    let hosts = hosts.into_iter().flatten().collect();
    eprintln!("[Pegasus] Hosts detected:\n{:#?}", &hosts);
    hosts
}
