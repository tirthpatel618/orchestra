use crate::{
    CompiledPipeline, FakeTask, Flow, LlmConfig, LlmTask, OrchestraError, Pipeline, RunOutput,
    RunResult, RuntimeEvent, Task, TaskFuture, TaskInput,
};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    types::{PyDict, PyModule},
};
use std::{sync::Arc, time::Duration};
use tokio::{runtime::Runtime, sync::mpsc, time::sleep};

#[pyclass(name = "Flow", skip_from_py_object)]
#[derive(Debug, Clone, Default)]
pub struct PyFlow {
    flow: Flow,
}

#[pymethods]
impl PyFlow {
    #[new]
    pub fn new() -> Self {
        Self { flow: Flow::new() }
    }

    #[pyo3(signature = (id, output, *, delay_ms = 0, chunks = None, fail_with = None, include_dependency_outputs = false))]
    pub fn add_fake_node(
        &mut self,
        id: String,
        output: String,
        delay_ms: u64,
        chunks: Option<Vec<String>>,
        fail_with: Option<String>,
        include_dependency_outputs: bool,
    ) -> PyResult<()> {
        let mut task = FakeTask::new(output).delay(Duration::from_millis(delay_ms));

        if let Some(chunks) = chunks {
            task = task.chunks(chunks);
        }

        if let Some(message) = fail_with {
            task = task.fail_with(message);
        }

        if include_dependency_outputs {
            task = task.include_dependency_outputs();
        }

        self.flow.add_node(id, task).map_err(map_error)
    }

    pub fn add_dependency(&mut self, node: String, dependency: String) -> PyResult<()> {
        self.flow.add_dependency(node, dependency).map_err(map_error)
    }

    #[pyo3(signature = (id, operation, *, operands = None, delay_ms = 0, modulus = None))]
    pub fn add_arithmetic_node(
        &mut self,
        id: String,
        operation: String,
        operands: Option<Vec<i64>>,
        delay_ms: u64,
        modulus: Option<i64>,
    ) -> PyResult<()> {
        let operation = ArithmeticOperation::parse(&operation)?;
        let operands = operands
            .unwrap_or_default()
            .into_iter()
            .map(i128::from)
            .collect::<Vec<_>>();
        let modulus = modulus
            .map(i128::from)
            .filter(|value| *value > 0);

        if matches!(operation, ArithmeticOperation::Constant) && operands.len() != 1 {
            return Err(PyValueError::new_err(
                "constant arithmetic nodes require exactly one operand",
            ));
        }

        self.flow
            .add_node(
                id,
                LocalArithmeticTask {
                    operation,
                    operands,
                    delay: Duration::from_millis(delay_ms),
                    modulus,
                },
            )
            .map_err(map_error)
    }

    #[pyo3(signature = (id, prompt, *, api_key = None, model = None, max_tokens = 8, temperature = 0.0, include_dependency_outputs = false, substitute_dependency_outputs = false))]
    pub fn add_groq_llm_node(
        &mut self,
        id: String,
        prompt: String,
        api_key: Option<String>,
        model: Option<String>,
        max_tokens: u16,
        temperature: f32,
        include_dependency_outputs: bool,
        substitute_dependency_outputs: bool,
    ) -> PyResult<()> {
        let mut config = match api_key {
            Some(api_key) => LlmConfig::groq(api_key),
            None => LlmConfig::groq_from_env().map_err(map_error)?,
        }
        .with_max_tokens(max_tokens)
        .with_temperature(temperature);

        if let Some(model) = model {
            config = config.with_model(model);
        }

        let mut task = LlmTask::arithmetic(config, prompt);
        if include_dependency_outputs {
            task = task.include_dependency_outputs();
        }
        if substitute_dependency_outputs {
            task = task.substitute_dependency_outputs();
        }

        self.flow.add_node(id, task).map_err(map_error)
    }

    #[pyo3(signature = (*, event_buffer = 64, max_concurrency = None))]
    pub fn compile(
        &self,
        event_buffer: usize,
        max_concurrency: Option<usize>,
    ) -> PyResult<PyCompiledPipeline> {
        let mut pipeline = Pipeline::new(self.flow.clone()).with_event_buffer(event_buffer);
        if let Some(max_concurrency) = max_concurrency {
            pipeline = pipeline.with_max_concurrency(max_concurrency);
        }

        Ok(PyCompiledPipeline {
            pipeline: pipeline.compile().map_err(map_error)?,
            runtime: Arc::new(new_runtime()?),
        })
    }

    pub fn node_count(&self) -> usize {
        self.flow.nodes().len()
    }
}

#[pyclass(name = "CompiledPipeline", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyCompiledPipeline {
    pipeline: CompiledPipeline,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl PyCompiledPipeline {
    pub fn execute(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let outputs = py
            .detach(|| self.runtime.block_on(self.pipeline.execute()))
            .map_err(map_error)?;
        outputs_to_dict(py, outputs)
    }

    pub fn execute_with_trace(&self, py: Python<'_>) -> PyResult<PyRunResult> {
        let result = py
            .detach(|| self.runtime.block_on(self.pipeline.execute_with_trace()))
            .map_err(map_error)?;
        Ok(PyRunResult { result })
    }

    pub fn execute_report(&self, py: Python<'_>) -> PyRunResult {
        let result = py.detach(|| self.runtime.block_on(self.pipeline.execute_report()));
        PyRunResult { result }
    }
}

#[pyclass(name = "RunResult", skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyRunResult {
    result: RunResult,
}

#[pymethods]
impl PyRunResult {
    #[getter]
    pub fn outputs(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        outputs_to_dict(py, self.result.outputs.clone())
    }

    #[getter]
    pub fn status(&self) -> String {
        format!("{:?}", self.result.trace.status)
    }

    #[getter]
    pub fn error(&self) -> Option<String> {
        self.result.trace.error.clone()
    }

    #[getter]
    pub fn duration_ms(&self) -> u128 {
        self.result.trace.duration_ms
    }

    #[getter]
    pub fn event_count(&self) -> u64 {
        self.result.trace.event_count
    }

    #[getter]
    pub fn streamed_chunk_count(&self) -> u64 {
        self.result.trace.streamed_chunk_count
    }

    pub fn trace_json(&self) -> PyResult<String> {
        self.result.trace_json().map_err(json_error)
    }

    pub fn trace_json_pretty(&self) -> PyResult<String> {
        self.result.trace_json_pretty().map_err(json_error)
    }
}

#[pymodule]
pub fn orchestra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyFlow>()?;
    m.add_class::<PyCompiledPipeline>()?;
    m.add_class::<PyRunResult>()?;
    Ok(())
}

fn new_runtime() -> PyResult<Runtime> {
    Runtime::new().map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

fn outputs_to_dict(py: Python<'_>, outputs: RunOutput) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    for (node, output) in outputs {
        dict.set_item(node, output)?;
    }
    Ok(dict.unbind())
}

fn map_error(error: OrchestraError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn json_error(error: serde_json::Error) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[derive(Debug, Clone)]
struct LocalArithmeticTask {
    operation: ArithmeticOperation,
    operands: Vec<i128>,
    delay: Duration,
    modulus: Option<i128>,
}

impl Task for LocalArithmeticTask {
    fn execute<'a>(
        &'a self,
        input: TaskInput,
        _events: Option<mpsc::Sender<RuntimeEvent>>,
    ) -> TaskFuture<'a> {
        Box::pin(async move {
            if !self.delay.is_zero() {
                sleep(self.delay).await;
            }

            let dependency_values = input
                .dependency_outputs
                .iter()
                .map(|(node, output)| parse_dependency_output(node, output))
                .collect::<Result<Vec<_>, _>>()?;

            let values = self
                .operands
                .iter()
                .chain(dependency_values.iter())
                .copied()
                .collect::<Vec<_>>();

            let value = match self.operation {
                ArithmeticOperation::Constant => apply_modulus(self.operands[0], self.modulus),
                ArithmeticOperation::Add => values.into_iter().fold(0, |sum, value| {
                    apply_modulus(sum + apply_modulus(value, self.modulus), self.modulus)
                }),
                ArithmeticOperation::Multiply => values.into_iter().fold(1, |product, value| {
                    apply_modulus(
                        product * apply_modulus(value, self.modulus),
                        self.modulus,
                    )
                }),
            };

            Ok(value.to_string())
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArithmeticOperation {
    Constant,
    Add,
    Multiply,
}

impl ArithmeticOperation {
    fn parse(operation: &str) -> PyResult<Self> {
        match operation {
            "const" | "constant" => Ok(Self::Constant),
            "add" | "sum" => Ok(Self::Add),
            "mul" | "multiply" | "product" => Ok(Self::Multiply),
            _ => Err(PyValueError::new_err(
                "operation must be one of: const, add, sum, mul, multiply, product",
            )),
        }
    }
}

fn parse_dependency_output(node: &str, output: &str) -> Result<i128, OrchestraError> {
    output
        .trim()
        .parse::<i128>()
        .map_err(|error| OrchestraError::NodeFailed {
            node: node.to_string(),
            message: format!("dependency output is not an integer: {error}"),
        })
}

fn apply_modulus(value: i128, modulus: Option<i128>) -> i128 {
    match modulus {
        Some(modulus) => value.rem_euclid(modulus),
        None => value,
    }
}
