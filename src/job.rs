use std::collections::HashMap;
use std::fs::File;
use std::str::FromStr;

use handlebars::Handlebars;
use serde::{Deserialize, Serialize};
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
            .unwrap_or_else(|_| {
                panic!(
                    "Failed to render command template '{}' with params '{:#?}'",
                    &self.command, &self.params
                )
            })
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

pub struct JobQueue {
    fetched: Vec<Cmd>,
}

impl JobQueue {
    pub fn new() -> Self {
        Self {
            fetched: Vec::new(),
        }
    }

    pub async fn next(&mut self) -> Option<Cmd> {
        if self.fetched.is_empty() {
            self.fetch().await;
        }
        // When queue.yaml is empty, calling `fetch` will leave
        // `self.fetched` an empty vector. Thus, `None` is returned.
        self.fetched.pop()
    }

    async fn fetch(&mut self) {
        loop {
            let queue_file = LockedFile::acquire(".lock", "queue.yaml").await;
            let file = queue_file.read_handle();
            let job_specs: Result<Vec<JobSpec>, _> = serde_yaml::from_reader(file);
            if let Ok(mut job_specs) = job_specs {
                if job_specs.is_empty() {
                    return;
                }
                let job_spec = job_specs.remove(0);

                // Job spec must contain the key "command".
                if !job_spec.0 .0.contains_key("command") {
                    eprintln!("[Pegasus] Job at the head of the queue has no 'command' key.");
                    eprintln!("[Pegasus] Wait 5 seconds for fix...");
                    time::sleep(time::Duration::from_secs(5)).await;
                    continue;
                }

                // Job spec looks good. Remove it from queue.yaml.
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

                // Reverse the command vector so that we can `pop` in `next`.
                job.reverse();
                self.fetched = job;
                return;
            } else {
                eprintln!(
                    "[Pegasus] Failed to parse queue.yaml: {}",
                    job_specs.unwrap_err()
                );
                eprintln!("[Pegasus] Wait 5 seconds for fix...");
                time::sleep(time::Duration::from_secs(5)).await;
            }
        }
    }
}
