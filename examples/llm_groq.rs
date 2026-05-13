use orchestra::{Flow, LlmConfig, LlmTask, Pipeline};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = match LlmConfig::groq_from_env() {
        Ok(config) => config.with_max_tokens(8),
        Err(error) => {
            eprintln!("{error}");
            eprintln!("Set GROQ_API_KEY to run this example.");
            return Ok(());
        }
    };

    let mut flow = Flow::new();
    flow.add_node("add", LlmTask::arithmetic(config, "3 + 5"))?;

    let result = Pipeline::new(flow).execute_with_trace().await?;
    let add_trace = &result.trace.nodes["add"];

    println!("answer: {}", result.outputs["add"]);
    println!(
        "provider: {:?}",
        add_trace.llm_usage.as_ref().map(|usage| &usage.provider)
    );
    println!(
        "model: {:?}",
        add_trace.llm_usage.as_ref().map(|usage| &usage.model)
    );
    println!("prompt_tokens: {}", result.trace.prompt_tokens);
    println!("completion_tokens: {}", result.trace.completion_tokens);
    println!("total_tokens: {}", result.trace.total_tokens);
    println!("trace_json: {}", result.trace_json_pretty()?);

    Ok(())
}
