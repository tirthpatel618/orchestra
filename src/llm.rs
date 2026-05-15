use crate::{task::TaskFuture, LlmUsage, OrchestraError, RuntimeEvent, Task, TaskInput};
use serde::{Deserialize, Serialize};
use std::{env, fmt};
use tokio::sync::mpsc;

const GROQ_API_BASE: &str = "https://api.groq.com/openai/v1";
const GROQ_DEFAULT_MODEL: &str = "llama-3.1-8b-instant";
const DEFAULT_SYSTEM_PROMPT: &str = "Answer with only the integer.";

#[derive(Clone)]
pub struct LlmConfig {
    provider: String,
    api_base: String,
    api_key: String,
    model: String,
    temperature: f32,
    max_tokens: u16,
}

impl LlmConfig {
    pub fn openai_compatible(
        provider: impl Into<String>,
        api_base: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            api_base: api_base.into(),
            api_key: api_key.into(),
            model: model.into(),
            temperature: 0.0,
            max_tokens: 8,
        }
    }

    pub fn groq(api_key: impl Into<String>) -> Self {
        Self::openai_compatible("groq", GROQ_API_BASE, api_key, GROQ_DEFAULT_MODEL)
    }

    pub fn groq_from_env() -> Result<Self, OrchestraError> {
        let api_key = env::var("GROQ_API_KEY").map_err(|_| OrchestraError::NodeFailed {
            node: "llm_config".to_string(),
            message: "missing GROQ_API_KEY environment variable".to_string(),
        })?;

        Ok(Self::groq(api_key))
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    pub fn with_api_base(mut self, api_base: impl Into<String>) -> Self {
        self.api_base = api_base.into();
        self
    }

    pub fn with_temperature(mut self, temperature: f32) -> Self {
        self.temperature = temperature;
        self
    }

    pub fn with_max_tokens(mut self, max_tokens: u16) -> Self {
        self.max_tokens = max_tokens.max(1);
        self
    }

    pub fn provider(&self) -> &str {
        &self.provider
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    fn chat_completions_url(&self) -> String {
        format!("{}/chat/completions", self.api_base.trim_end_matches('/'))
    }
}

impl fmt::Debug for LlmConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LlmConfig")
            .field("provider", &self.provider)
            .field("api_base", &self.api_base)
            .field("api_key", &"<redacted>")
            .field("model", &self.model)
            .field("temperature", &self.temperature)
            .field("max_tokens", &self.max_tokens)
            .finish()
    }
}

#[derive(Clone)]
pub struct LlmTask {
    config: LlmConfig,
    system_prompt: String,
    user_prompt: String,
    include_dependency_outputs: bool,
    normalize_integer_output: bool,
    substitute_dependency_outputs: bool,
    client: reqwest::Client,
}

impl LlmTask {
    pub fn new(config: LlmConfig, user_prompt: impl Into<String>) -> Self {
        Self {
            config,
            system_prompt: DEFAULT_SYSTEM_PROMPT.to_string(),
            user_prompt: user_prompt.into(),
            include_dependency_outputs: false,
            normalize_integer_output: false,
            substitute_dependency_outputs: false,
            client: reqwest::Client::new(),
        }
    }

    pub fn arithmetic(config: LlmConfig, expression: impl Into<String>) -> Self {
        Self::new(config, expression).normalize_integer_output()
    }

    pub fn with_system_prompt(mut self, system_prompt: impl Into<String>) -> Self {
        self.system_prompt = system_prompt.into();
        self
    }

    pub fn include_dependency_outputs(mut self) -> Self {
        self.include_dependency_outputs = true;
        self
    }

    pub fn normalize_integer_output(mut self) -> Self {
        self.normalize_integer_output = true;
        self
    }

    pub fn substitute_dependency_outputs(mut self) -> Self {
        self.substitute_dependency_outputs = true;
        self
    }

    fn render_user_prompt(&self, input: &TaskInput) -> String {
        if self.substitute_dependency_outputs {
            let mut prompt = self.user_prompt.clone();
            for (node, output) in &input.dependency_outputs {
                prompt = prompt.replace(&format!("{{{node}}}"), output);
            }
            return prompt;
        }

        if !self.include_dependency_outputs {
            return self.user_prompt.clone();
        }

        let mut dependencies = input
            .dependency_outputs
            .iter()
            .map(|(node, output)| format!("{node} = {output}"))
            .collect::<Vec<_>>();
        dependencies.sort();

        if dependencies.is_empty() {
            self.user_prompt.clone()
        } else {
            format!(
                "{}\n\nDependency outputs:\n{}\n\nReturn only the final answer.",
                self.user_prompt,
                dependencies.join("\n")
            )
        }
    }
}

impl fmt::Debug for LlmTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("LlmTask")
                .field("config", &self.config)
                .field("system_prompt", &self.system_prompt)
                .field("user_prompt", &self.user_prompt)
                .field(
                "include_dependency_outputs",
                &self.include_dependency_outputs,
            )
            .field("normalize_integer_output", &self.normalize_integer_output)
            .field(
                "substitute_dependency_outputs",
                &self.substitute_dependency_outputs,
            )
            .finish_non_exhaustive()
    }
}

impl Task for LlmTask {
    fn execute<'a>(
        &'a self,
        input: TaskInput,
        events: Option<mpsc::Sender<RuntimeEvent>>,
    ) -> TaskFuture<'a> {
        Box::pin(async move {
            let request = ChatCompletionRequest {
                model: self.config.model.clone(),
                messages: vec![
                    ChatMessage {
                        role: "system".to_string(),
                        content: self.system_prompt.clone(),
                    },
                    ChatMessage {
                        role: "user".to_string(),
                        content: self.render_user_prompt(&input),
                    },
                ],
                temperature: self.config.temperature,
                max_tokens: self.config.max_tokens,
            };

            let response = self
                .client
                .post(self.config.chat_completions_url())
                .bearer_auth(&self.config.api_key)
                .json(&request)
                .send()
                .await
                .map_err(|error| {
                    node_failed(&input.node, format!("LLM request failed: {error}"))
                })?;

            let status = response.status();
            let body = response.text().await.map_err(|error| {
                node_failed(&input.node, format!("LLM response read failed: {error}"))
            })?;

            if !status.is_success() {
                return Err(node_failed(
                    &input.node,
                    format!(
                        "LLM request returned HTTP {status}: {}",
                        truncate(&body, 500)
                    ),
                ));
            }

            let completion =
                serde_json::from_str::<ChatCompletionResponse>(&body).map_err(|error| {
                    node_failed(&input.node, format!("LLM response parse failed: {error}"))
                })?;
            let output = completion
                .choices
                .first()
                .and_then(|choice| choice.message.content.as_deref())
                .map(str::trim)
                .filter(|content| !content.is_empty())
                .ok_or_else(|| node_failed(&input.node, "LLM response did not contain content"))?
                .to_string();
            let output = if self.normalize_integer_output {
                last_integer(&output).ok_or_else(|| {
                    node_failed(
                        &input.node,
                        format!("LLM response did not contain an integer: {output}"),
                    )
                })?
            } else {
                output
            };

            if let Some(usage) = completion.usage {
                send_usage(
                    &events,
                    &input.node,
                    LlmUsage {
                        provider: self.config.provider.clone(),
                        model: self.config.model.clone(),
                        prompt_tokens: usage.prompt_tokens.unwrap_or(0),
                        completion_tokens: usage.completion_tokens.unwrap_or(0),
                        total_tokens: usage.total_tokens.unwrap_or(0),
                    },
                )
                .await;
            }

            Ok(output)
        })
    }
}

async fn send_usage(events: &Option<mpsc::Sender<RuntimeEvent>>, node: &str, usage: LlmUsage) {
    if let Some(events) = events {
        let _ = events
            .send(RuntimeEvent::NodeLlmUsage {
                node: node.to_string(),
                usage,
            })
            .await;
    }
}

fn node_failed(node: &str, message: impl Into<String>) -> OrchestraError {
    OrchestraError::NodeFailed {
        node: node.to_string(),
        message: message.into(),
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn last_integer(value: &str) -> Option<String> {
    let mut integers = Vec::new();
    let mut current = String::new();

    for character in value.chars() {
        if character.is_ascii_digit() || (character == '-' && current.is_empty()) {
            current.push(character);
        } else if current.chars().any(|character| character.is_ascii_digit()) {
            integers.push(std::mem::take(&mut current));
        } else {
            current.clear();
        }
    }

    if current.chars().any(|character| character.is_ascii_digit()) {
        integers.push(current);
    }

    integers.pop()
}

#[derive(Debug, Clone, Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    temperature: f32,
    max_tokens: u16,
}

#[derive(Debug, Clone, Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
    usage: Option<ChatUsage>,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatChoice {
    message: ChatResponseMessage,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatResponseMessage {
    content: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatUsage {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn groq_config_uses_openai_compatible_defaults() {
        let config = LlmConfig::groq("test-key");

        assert_eq!(config.provider(), "groq");
        assert_eq!(config.model(), GROQ_DEFAULT_MODEL);
        assert_eq!(
            config.chat_completions_url(),
            "https://api.groq.com/openai/v1/chat/completions"
        );
    }

    #[test]
    fn dependency_prompt_is_stable_and_sorted() {
        let task = LlmTask::arithmetic(LlmConfig::groq("test-key"), "Compute a + b.")
            .include_dependency_outputs();
        let input = TaskInput {
            node: "sum".to_string(),
            dependency_outputs: HashMap::from([
                ("b".to_string(), "7".to_string()),
                ("a".to_string(), "3".to_string()),
            ]),
        };

        let prompt = task.render_user_prompt(&input);

        assert!(prompt.contains("a = 3\nb = 7"));
        assert!(prompt.ends_with("Return only the final answer."));
    }

    #[test]
    fn dependency_outputs_can_be_substituted_into_prompt() {
        let task = LlmTask::arithmetic(LlmConfig::groq("test-key"), "{a} * {b}")
            .substitute_dependency_outputs();
        let input = TaskInput {
            node: "multiply".to_string(),
            dependency_outputs: HashMap::from([
                ("a".to_string(), "36".to_string()),
                ("b".to_string(), "15".to_string()),
            ]),
        };

        assert_eq!(task.render_user_prompt(&input), "36 * 15");
    }

    #[test]
    fn arithmetic_output_uses_last_integer() {
        assert_eq!(last_integer("36 * 15 = 540"), Some("540".to_string()));
        assert_eq!(last_integer("answer: -12"), Some("-12".to_string()));
        assert_eq!(last_integer("no integer"), None);
    }
}
