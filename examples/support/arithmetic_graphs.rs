use orchestra::{
    Flow, NodeId, OrchestraError, RunOutput, RuntimeEvent, Task, TaskFuture, TaskInput,
};
use std::{collections::HashMap, time::Duration};
use tokio::{sync::mpsc, time::sleep};

pub const MODULUS: u64 = 1_000_000_007;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayeredSpec {
    pub width: usize,
    pub depth: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeSpec {
    pub depth: usize,
    pub branching: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnswerReduction {
    Single,
    Sum,
}

#[derive(Debug, Clone)]
pub struct ArithmeticGraph {
    pub flow: Flow,
    pub expected_answer: u64,
    pub answer_nodes: Vec<NodeId>,
    pub reduction: AnswerReduction,
}

impl ArithmeticGraph {
    pub fn answer_from_outputs(&self, outputs: &RunOutput) -> Result<u64, ArithmeticGraphError> {
        match self.reduction {
            AnswerReduction::Single => {
                let Some(node) = self.answer_nodes.first() else {
                    return Err(ArithmeticGraphError::InvalidSpec(
                        "single-answer graph has no answer node".to_string(),
                    ));
                };
                let Some(output) = outputs.get(node) else {
                    return Err(ArithmeticGraphError::MissingOutput(node.clone()));
                };
                parse_value(node, output)
            }
            AnswerReduction::Sum => self.answer_nodes.iter().try_fold(0, |sum, node| {
                let Some(output) = outputs.get(node) else {
                    return Err(ArithmeticGraphError::MissingOutput(node.clone()));
                };
                Ok(add_mod(sum, parse_value(node, output)?))
            }),
        }
    }
}

pub fn chain(length: usize, delay: Duration) -> Result<ArithmeticGraph, ArithmeticGraphError> {
    require_positive(length, "chain length")?;

    let mut flow = Flow::new();
    let mut expected = 1;

    flow.add_node("chain_0", ArithmeticTask::constant(expected, delay))?;
    for index in 1..length {
        let node = chain_node(index);
        let dependency = chain_node(index - 1);
        let add = index as u64;

        flow.add_node(node.clone(), ArithmeticTask::affine(31, add, delay))?;
        flow.add_dependency(node, dependency)?;
        expected = add_mod(mul_mod(expected, 31), add);
    }

    Ok(ArithmeticGraph {
        flow,
        expected_answer: expected,
        answer_nodes: vec![chain_node(length - 1)],
        reduction: AnswerReduction::Single,
    })
}

pub fn wide(width: usize, delay: Duration) -> Result<ArithmeticGraph, ArithmeticGraphError> {
    require_positive(width, "wide width")?;

    let mut flow = Flow::new();
    let mut expected = 0;
    let mut answer_nodes = Vec::with_capacity(width);

    for index in 0..width {
        let node = wide_node(index);
        let value = leaf_value(index);
        flow.add_node(node.clone(), ArithmeticTask::constant(value, delay))?;
        expected = add_mod(expected, value);
        answer_nodes.push(node);
    }

    Ok(ArithmeticGraph {
        flow,
        expected_answer: expected,
        answer_nodes,
        reduction: AnswerReduction::Sum,
    })
}

pub fn fan_in(width: usize, delay: Duration) -> Result<ArithmeticGraph, ArithmeticGraphError> {
    require_positive(width, "fan-in width")?;

    let mut flow = Flow::new();
    let mut expected = 0;

    for index in 0..width {
        let node = source_node(index);
        let value = leaf_value(index);
        flow.add_node(node.clone(), ArithmeticTask::constant(value, delay))?;
        expected = add_mod(expected, value);
    }

    flow.add_node("reducer", ArithmeticTask::sum_dependencies(0, delay))?;
    for index in 0..width {
        flow.add_dependency("reducer", source_node(index))?;
    }

    Ok(ArithmeticGraph {
        flow,
        expected_answer: expected,
        answer_nodes: vec!["reducer".to_string()],
        reduction: AnswerReduction::Single,
    })
}

pub fn layered(
    spec: LayeredSpec,
    delay: Duration,
) -> Result<ArithmeticGraph, ArithmeticGraphError> {
    require_positive(spec.width, "layered width")?;
    require_positive(spec.depth, "layered depth")?;

    let mut flow = Flow::new();
    let mut previous_values = Vec::with_capacity(spec.width);

    for index in 0..spec.width {
        let node = layered_node(0, index);
        let value = leaf_value(index);
        flow.add_node(node, ArithmeticTask::constant(value, delay))?;
        previous_values.push(value);
    }

    for layer in 1..spec.depth {
        let previous_sum = previous_values.iter().copied().fold(0, add_mod);
        let mut current_values = Vec::with_capacity(spec.width);

        for index in 0..spec.width {
            let node = layered_node(layer, index);
            let add = (layer + index) as u64;
            flow.add_node(node.clone(), ArithmeticTask::sum_dependencies(add, delay))?;

            for previous in 0..spec.width {
                flow.add_dependency(node.clone(), layered_node(layer - 1, previous))?;
            }

            current_values.push(add_mod(previous_sum, add));
        }

        previous_values = current_values;
    }

    let answer_nodes = (0..spec.width)
        .map(|index| layered_node(spec.depth - 1, index))
        .collect::<Vec<_>>();
    let expected_answer = previous_values.into_iter().fold(0, add_mod);

    Ok(ArithmeticGraph {
        flow,
        expected_answer,
        answer_nodes,
        reduction: AnswerReduction::Sum,
    })
}

pub fn tree(spec: TreeSpec, delay: Duration) -> Result<ArithmeticGraph, ArithmeticGraphError> {
    require_positive(spec.depth, "tree depth")?;
    require_positive(spec.branching, "tree branching")?;

    let mut flow = Flow::new();
    let leaf_level = spec.depth - 1;
    let leaf_count = node_count_at_depth(leaf_level, spec.branching)?;
    let mut level_values = HashMap::new();

    for index in 0..leaf_count {
        let node = tree_node(leaf_level, index);
        let value = leaf_value(index);
        flow.add_node(node, ArithmeticTask::constant(value, delay))?;
        level_values.insert((leaf_level, index), value);
    }

    for level in (0..leaf_level).rev() {
        let count = node_count_at_depth(level, spec.branching)?;
        for index in 0..count {
            let node = tree_node(level, index);
            let add = (level + index) as u64;
            flow.add_node(node.clone(), ArithmeticTask::sum_dependencies(add, delay))?;

            let mut sum = add;
            for child in 0..spec.branching {
                let child_index = index * spec.branching + child;
                flow.add_dependency(node.clone(), tree_node(level + 1, child_index))?;
                let value = level_values[&(level + 1, child_index)];
                sum = add_mod(sum, value);
            }

            level_values.insert((level, index), sum);
        }
    }

    Ok(ArithmeticGraph {
        flow,
        expected_answer: level_values[&(0, 0)],
        answer_nodes: vec![tree_node(0, 0)],
        reduction: AnswerReduction::Single,
    })
}

pub fn chain_node(index: usize) -> String {
    format!("chain_{index}")
}

pub fn wide_node(index: usize) -> String {
    format!("wide_{index}")
}

pub fn source_node(index: usize) -> String {
    format!("source_{index}")
}

pub fn layered_node(layer: usize, index: usize) -> String {
    format!("layer_{layer}_{index}")
}

pub fn tree_node(level: usize, index: usize) -> String {
    format!("tree_{level}_{index}")
}

#[derive(Debug, Clone)]
struct ArithmeticTask {
    operation: Operation,
    delay: Duration,
}

impl ArithmeticTask {
    fn constant(value: u64, delay: Duration) -> Self {
        Self {
            operation: Operation::Constant(value % MODULUS),
            delay,
        }
    }

    fn affine(multiplier: u64, add: u64, delay: Duration) -> Self {
        Self {
            operation: Operation::Affine {
                multiplier: multiplier % MODULUS,
                add: add % MODULUS,
            },
            delay,
        }
    }

    fn sum_dependencies(add: u64, delay: Duration) -> Self {
        Self {
            operation: Operation::SumDependencies { add: add % MODULUS },
            delay,
        }
    }
}

impl Task for ArithmeticTask {
    fn execute<'a>(
        &'a self,
        input: TaskInput,
        _events: Option<mpsc::Sender<RuntimeEvent>>,
    ) -> TaskFuture<'a> {
        Box::pin(async move {
            if !self.delay.is_zero() {
                sleep(self.delay).await;
            }

            let value = match self.operation {
                Operation::Constant(value) => value,
                Operation::Affine { multiplier, add } => {
                    let dependency = single_dependency_value(&input)?;
                    add_mod(mul_mod(dependency, multiplier), add)
                }
                Operation::SumDependencies { add } => input.dependency_outputs.iter().try_fold(
                    add,
                    |sum, (node, output)| -> Result<u64, OrchestraError> {
                        Ok(add_mod(sum, parse_task_value(&input.node, node, output)?))
                    },
                )?,
            };

            Ok(value.to_string())
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum Operation {
    Constant(u64),
    Affine { multiplier: u64, add: u64 },
    SumDependencies { add: u64 },
}

fn single_dependency_value(input: &TaskInput) -> Result<u64, OrchestraError> {
    if input.dependency_outputs.len() != 1 {
        return Err(OrchestraError::NodeFailed {
            node: input.node.clone(),
            message: format!(
                "expected exactly one dependency output, got {}",
                input.dependency_outputs.len()
            ),
        });
    }

    let (node, output) = input.dependency_outputs.iter().next().unwrap();
    parse_task_value(&input.node, node, output)
}

fn parse_value(node: &str, output: &str) -> Result<u64, ArithmeticGraphError> {
    output
        .parse::<u64>()
        .map(|value| value % MODULUS)
        .map_err(|error| ArithmeticGraphError::InvalidOutput {
            node: node.to_string(),
            output: output.to_string(),
            message: error.to_string(),
        })
}

fn parse_task_value(
    current_node: &str,
    dependency_node: &str,
    output: &str,
) -> Result<u64, OrchestraError> {
    parse_value(dependency_node, output).map_err(|error| OrchestraError::NodeFailed {
        node: current_node.to_string(),
        message: error.to_string(),
    })
}

fn leaf_value(index: usize) -> u64 {
    ((index as u64) + 1) % MODULUS
}

fn add_mod(left: u64, right: u64) -> u64 {
    (left + right) % MODULUS
}

fn mul_mod(left: u64, right: u64) -> u64 {
    ((left as u128 * right as u128) % MODULUS as u128) as u64
}

fn require_positive(value: usize, name: &str) -> Result<(), ArithmeticGraphError> {
    if value == 0 {
        return Err(ArithmeticGraphError::InvalidSpec(format!(
            "{name} must be greater than zero"
        )));
    }

    Ok(())
}

fn node_count_at_depth(depth: usize, branching: usize) -> Result<usize, ArithmeticGraphError> {
    let exponent = u32::try_from(depth).map_err(|_| {
        ArithmeticGraphError::InvalidSpec("tree depth is too large to calculate".to_string())
    })?;

    branching.checked_pow(exponent).ok_or_else(|| {
        ArithmeticGraphError::InvalidSpec("tree node count overflowed usize".to_string())
    })
}

#[derive(Debug, thiserror::Error)]
pub enum ArithmeticGraphError {
    #[error("{0}")]
    InvalidSpec(String),

    #[error("missing output for answer node: {0}")]
    MissingOutput(NodeId),

    #[error("node '{node}' returned invalid arithmetic output '{output}': {message}")]
    InvalidOutput {
        node: NodeId,
        output: String,
        message: String,
    },

    #[error(transparent)]
    Orchestra(#[from] OrchestraError),
}
