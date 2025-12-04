//! SSH Hosts.
//!
//! One SSH connection is created for one `Host`. Each connection will run commands in parallel with
//! other connections in its own tokio task.

use itertools::sorted;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::str::FromStr;

use colored::*;
use colourado::Color;
use openssh::{KnownHosts, Session as SSHSession};
use serde::Deserialize;
use void::Void;

use crate::error::PegasusError;
use crate::serde::{HostSpecParsed, string_or_mapping};
use crate::session::{LocalSession, RemoteSession, Session};

#[derive(Debug, Clone)]
pub struct Host {
    /// SSH hostname to connect to.
    pub hostname: String,
    /// Sub-host parameters used to fill in templates.
    pub params: HashMap<String, String>,
    /// Number of slots (e.g., GPUs) this host provides. Defaults to 1.
    pub slots: usize,
}

impl Host {
    fn new(hostname: String) -> Self {
        Self {
            hostname,
            params: HashMap::new(),
            slots: 1,
        }
    }

    /// Connects to the host over SSH.
    pub async fn connect(
        &self,
        color: Color,
    ) -> Result<Box<dyn Session + Send + Sync>, PegasusError> {
        let colorhost = self.prettify(color);
        // Localhost does not need an SSH connection.
        if self.is_localhost() {
            eprintln!("{} Just spawning subprocess for localhost.", colorhost);
            Ok(Box::new(LocalSession::new(colorhost)))
        } else {
            let session = match SSHSession::connect_mux(&self.hostname, KnownHosts::Add).await {
                Ok(session) => session,
                Err(e) => {
                    eprintln!("{} Failed to connect to host: {:?}", colorhost, e);
                    return Err(PegasusError::SshError(e));
                }
            };
            eprintln!("{} Connected to host.", colorhost);
            Ok(Box::new(RemoteSession::new(colorhost, session)))
        }
    }

    /// Returns true if the host is localhost.
    pub fn is_localhost(&self) -> bool {
        let hostname = self.hostname.trim().to_lowercase();
        hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1"
    }

    /// For pretty-printing the host name.
    /// Surrounds with brackets and colors it with a random color.
    pub fn prettify(&self, color: Color) -> ColoredString {
        let r = (color.red * 255.0) as u8;
        let g = (color.green * 255.0) as u8;
        let b = (color.blue * 255.0) as u8;
        format!("{}", self).truecolor(r, g, b)
    }
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let has_params = !self.params.is_empty();
        let has_slots = self.slots != 1;

        if !has_params && !has_slots {
            write!(f, "[{}]", self.hostname)?;
        } else {
            write!(f, "[{} (", self.hostname)?;
            let mut first = true;
            if has_slots {
                write!(f, "slots={}", self.slots)?;
                first = false;
            }
            for (key, value) in sorted(self.params.iter()) {
                if first {
                    write!(f, "{}={}", key, value)?;
                    first = false;
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
struct HostSpecInner(HostSpecParsed);

impl FromStr for HostSpecInner {
    type Err = Void;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut params = HashMap::new();
        params.insert("hostname".to_string(), vec![s.to_string()]);
        Ok(Self(HostSpecParsed {
            slots: None,
            params,
        }))
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
        .all(|spec| spec.0.0.params.contains_key("hostname"))
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
    for HostSpec(HostSpecInner(spec)) in host_specs {
        let slots = spec.slots.unwrap_or(1);
        let mut params = spec.params;

        let mut expanded = vec![];
        // We checked before that every `spec` has the key 'hostname'.
        for hostname in params.remove("hostname").unwrap() {
            let mut host = Host::new(hostname);
            host.slots = slots;
            expanded.push(host);
        }
        // For each parameter (String -> Vec<String>), the size of `expanded` will increase
        // by the number of possible values.
        for (key, values) in params {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_new_defaults_to_one_slot() {
        let host = Host::new("test-host".to_string());
        assert_eq!(host.slots, 1);
        assert_eq!(host.hostname, "test-host");
        assert!(host.params.is_empty());
    }

    #[test]
    fn test_host_display_without_slots() {
        let host = Host::new("test-host".to_string());
        assert_eq!(format!("{}", host), "[test-host]");
    }

    #[test]
    fn test_host_display_with_slots() {
        let mut host = Host::new("test-host".to_string());
        host.slots = 8;
        assert_eq!(format!("{}", host), "[test-host (slots=8)]");
    }

    #[test]
    fn test_host_display_with_slots_and_params() {
        let mut host = Host::new("test-host".to_string());
        host.slots = 4;
        host.params.insert("gpu".to_string(), "nvidia".to_string());
        let display = format!("{}", host);
        assert!(display.contains("slots=4"));
        assert!(display.contains("gpu=nvidia"));
    }
}
