use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::str::FromStr;
use std::sync::Arc;

use handlebars::Handlebars;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use tokio::sync::Mutex;
use tokio::time;
use void::Void;

use crate::host::Host;
use crate::serde::string_or_mapping;
use crate::sync::LockedFile;

#[derive(Debug, Clone)]
pub struct Cmd {
    /// Command (template).
    command: String,
    /// Command parameters used to fill in the command template.
    params: HashMap<String, String>,
    /// Number of slots this command requires. Defaults to 1.
    pub slots_required: usize,
}

impl Cmd {
    fn new(command: String) -> Self {
        Self {
            command,
            params: HashMap::new(),
            slots_required: 1,
        }
    }

    /// Test-only constructor that's public.
    pub fn new_for_test(command: String) -> Self {
        Self::new(command)
    }

    fn into_map(mut self) -> HashMap<String, Vec<String>> {
        self.params.insert("command".to_string(), self.command);
        if self.slots_required != 1 {
            self.params
                .insert("slots".to_string(), self.slots_required.to_string());
        }
        self.params.into_iter().map(|(k, v)| (k, vec![v])).collect()
    }

    /// Insert a parameter into the command's params.
    pub fn insert_param(&mut self, key: String, value: String) {
        self.params.insert(key, value);
    }

    pub fn fill_template(mut self, register: &mut Handlebars, host: &Host) -> String {
        if !register.has_template(&self.command) {
            register
                .register_template_string(&self.command, &self.command)
                .expect("Failed to register template string.");
        }
        let host = host.clone();
        self.params.extend(host.params);
        self.params.insert("hostname".to_string(), host.hostname);
        register
            .render(&self.command, &self.params)
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to render command template '{}' with params '{:#?}'. Error: {:?}",
                    &self.command, &self.params, e
                )
            })
    }
}

pub struct FailedCmd {
    host: String,
    cmd: String,
    error: String,
}

impl FailedCmd {
    pub fn new(host: String, cmd: String, error: String) -> Self {
        Self { host, cmd, error }
    }
}

impl Debug for FailedCmd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} ({})", self.host, self.cmd, self.error)
    }
}

/// Validates that a queue file is properly parsable.
/// Returns Ok(job_count) if valid, Err with message if invalid.
pub fn validate_queue_file(path: &str) -> Result<usize, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("Failed to open file: {}", e))?;
    let specs: Vec<JobSpec> =
        serde_yaml::from_reader(file).map_err(|e| format!("Failed to parse YAML: {}", e))?;

    // Validate each job spec has a "command" key
    for (i, spec) in specs.iter().enumerate() {
        if !spec.0.0.contains_key("command") {
            return Err(format!("Job {} is missing 'command' key", i));
        }
    }

    Ok(specs.len())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobSpec(#[serde(deserialize_with = "string_or_mapping")] JobSpecInner);

#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
struct JobSpecInner(HashMap<String, Vec<String>>);

impl FromStr for JobSpecInner {
    type Err = Void;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut map = HashMap::new();
        map.insert("command".to_string(), vec![s.to_string()]);
        Ok(Self(map))
    }
}

impl Serialize for JobSpecInner {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let JobSpecInner(ref map) = *self;
        let commands = &map["command"];
        if map.len() == 1 && commands.len() == 1 {
            // Check if there is only one key, and if so, it should be "command".
            // In that case, serialize the value of "command" directly as a string.
            commands[0].serialize(serializer)
        } else {
            let mut map_ser = serializer.serialize_map(Some(map.len()))?;
            map_ser.serialize_entry("command", &commands)?;
            for (key, values) in map {
                if key != "command" {
                    map_ser.serialize_entry(key, values)?;
                }
            }
            map_ser.end()
        }
    }
}

pub struct JobQueue {
    queue_file: String,
    cancelled: Arc<Mutex<bool>>,
}

impl JobQueue {
    pub fn new(queue_file: &str, cancelled: Arc<Mutex<bool>>) -> Self {
        Self {
            queue_file: queue_file.to_owned(),
            cancelled,
        }
    }

    pub async fn next(&mut self) -> Option<Cmd> {
        loop {
            let queue_file = LockedFile::acquire(&self.queue_file).await;
            // This handles the case where the user killed Pegasus while having
            // the queue file open in lock mode.
            if *self.cancelled.lock().await {
                eprintln!("[pegasus] Ctrl-c detected. Not fetching another job.");
                return None;
            }
            let file = queue_file.read_handle();
            let job_specs: Result<Vec<JobSpec>, _> = serde_yaml::from_reader(file);
            if let Ok(mut job_specs) = job_specs {
                if job_specs.is_empty() {
                    return None;
                }
                let job_spec = job_specs.remove(0);

                // Job spec must contain the key "command".
                if !job_spec.0.0.contains_key("command") {
                    eprintln!("[Pegasus] Job at the head of the queue has no 'command' key.");
                    eprintln!("[Pegasus] Wait 5 seconds for fix...");
                    time::sleep(time::Duration::from_secs(5)).await;
                    continue;
                }

                // Job spec looks good. Perform cartesian product.
                let JobSpec(JobSpecInner(mut spec)) = job_spec;

                // Extract slots BEFORE cartesian product - it's not a parameter to expand.
                let slots_required: usize = spec
                    .remove("slots")
                    .and_then(|v| v.into_iter().next())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1);

                let mut job = vec![];
                for command in spec.remove("command").unwrap() {
                    let mut cmd = Cmd::new(command);
                    cmd.slots_required = slots_required;
                    job.push(cmd);
                }
                for (key, values) in spec {
                    let mut expanded = Vec::with_capacity(values.len());
                    for command in job {
                        for value in values.iter() {
                            let mut command = command.clone();
                            command.params.insert(key.clone(), value.clone());
                            expanded.push(command);
                        }
                    }
                    job = expanded;
                }

                // Take the first command and put the rest back to the beginning of job specs.
                let next_command = job.remove(0);
                let remaining: Vec<_> = job
                    .into_iter()
                    .map(|cmd| JobSpec(JobSpecInner(cmd.into_map())))
                    .collect();
                job_specs = [remaining, job_specs].concat();

                // Job spec looks good. Remove it from the queue file.
                let writer = queue_file.write_handle();
                serde_yaml::to_writer(writer, &job_specs).expect("Failed to update the queue file");

                // Move the job to consumed.yaml.
                let write_handle = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("consumed.yaml")
                    .expect("Failed to open consumed.yaml.");
                serde_yaml::to_writer(
                    write_handle,
                    &vec![JobSpec(JobSpecInner(next_command.clone().into_map()))],
                )
                .expect("Failed to update consumed.yaml");

                return Some(next_command);
            } else {
                drop(queue_file);
                eprintln!(
                    "[Pegasus] Failed to parse {}: {}",
                    &self.queue_file,
                    job_specs.unwrap_err()
                );
                eprintln!("[Pegasus] Wait 5 seconds for fix...");
                time::sleep(time::Duration::from_secs(5)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cmd_new_defaults_to_one_slot() {
        let cmd = Cmd::new("echo hello".to_string());
        assert_eq!(cmd.slots_required, 1);
    }

    #[test]
    fn test_cmd_into_map_without_slots() {
        let cmd = Cmd::new("echo hello".to_string());
        let map = cmd.into_map();
        // slots=1 is the default, so it should NOT be in the map
        assert!(!map.contains_key("slots"));
        assert!(map.contains_key("command"));
    }

    #[test]
    fn test_cmd_into_map_with_slots() {
        let mut cmd = Cmd::new("echo hello".to_string());
        cmd.slots_required = 4;
        let map = cmd.into_map();
        // slots=4 is not the default, so it SHOULD be in the map
        assert!(map.contains_key("slots"));
        assert_eq!(map.get("slots").unwrap(), &vec!["4".to_string()]);
    }

    #[test]
    fn test_cmd_insert_param() {
        let mut cmd = Cmd::new("echo {{msg}}".to_string());
        cmd.insert_param("msg".to_string(), "hello".to_string());
        let map = cmd.into_map();
        assert_eq!(map.get("msg").unwrap(), &vec!["hello".to_string()]);
    }
}
