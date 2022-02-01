use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::str::FromStr;

use handlebars::Handlebars;
use serde::{Deserialize, Serialize};
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
}

impl Cmd {
    fn new(command: String) -> Self {
        Self {
            command,
            params: HashMap::new(),
        }
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
            .expect(&format!(
                "Failed to render command template '{}' with params '{:#?}'",
                &self.command, &self.params
            ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobSpec(#[serde(deserialize_with = "string_or_mapping")] JobSpecInner);

#[derive(Debug, Clone, Serialize, Deserialize)]
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

pub async fn get_one_job() -> Option<Vec<Cmd>> {
    let mut error_count = 0;
    loop {
        let queue_file = LockedFile::acquire("lock", "queue.yaml").await;
        let read_handle = queue_file.read_handle();
        let job_specs: Result<Vec<JobSpec>, _> = serde_yaml::from_reader(&read_handle);
        match job_specs {
            Ok(mut job_specs) => {
                if job_specs.is_empty() {
                    return None;
                }

                // Read the first job spec from queue.yaml.
                let job_spec = job_specs.remove(0);

                // Check if it has the key 'command'.
                if !job_spec.0 .0.contains_key("command") {
                    job_specs.insert(0, job_spec);
                    eprintln!("Job at the head of the queue has no 'command' key.");
                    serde_yaml::to_writer(queue_file.write_handle(), &job_specs)
                        .expect("Failed to update queue.yaml");
                    eprintln!("Waiting 5 seconds before retry...");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    continue;
                }

                // Job spec looks good. Remove it from queue.yaml.
                // NOTE: Might consider removing only one command from the command value list
                //       when there are multiple command templates in a single queue.yaml entry
                serde_yaml::to_writer(queue_file.write_handle(), &job_specs)
                    .expect("Failed to update queue.yaml");

                // Move the job to consumed.yaml.
                let (mut consumed, write_handle): (Vec<JobSpec>, _) =
                    if std::fs::metadata("consumed.yaml").is_ok() {
                        // consumed.yaml exists. So read it and deserialize it.
                        let handle =
                            File::open("consumed.yaml").expect("Failed to open consumed.yaml");
                        let consumed = serde_yaml::from_reader(handle)
                            .expect("Failed to parse consumed.yaml.");
                        let write_handle =
                            File::create("consumed.yaml").expect("Failed to create consumed.yaml.");
                        (consumed, write_handle)
                    } else {
                        // consumed.yaml does not exist. Create one and return an empty vector.
                        let write_handle =
                            File::create("consumed.yaml").expect("Failed to create consumed.yaml");
                        (Vec::with_capacity(1), write_handle)
                    };
                consumed.push(job_spec.clone());
                serde_yaml::to_writer(write_handle, &consumed)
                    .expect("Failed to update consumed.yaml");

                // Cartesian product.
                let JobSpec(JobSpecInner(mut spec)) = job_spec;
                let mut job = vec![];
                for command in spec.remove("command").unwrap() {
                    job.push(Cmd::new(command));
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

                return Some(job);
            }
            Err(err) => {
                error_count += 1;
                if error_count > 10 {
                    eprintln!("Failed to parse queue.yaml too many times. Assuming empty queue.");
                    return None;
                }
                eprintln!("Failed to parse queue.yaml: {}", err);
                eprintln!("Waiting 5 seconds before retry...");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }
}
