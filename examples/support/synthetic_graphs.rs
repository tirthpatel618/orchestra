use orchestra::{FakeTask, Flow, OrchestraError};
use std::time::Duration;

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

pub fn chain(length: usize, delay: Duration) -> Result<Flow, GraphSpecError> {
    require_positive(length, "chain length")?;

    let mut flow = Flow::new();
    for index in 0..length {
        let node = format!("chain_{index}");
        flow.add_node(node.clone(), FakeTask::new(node.clone()).delay(delay))?;

        if index > 0 {
            flow.add_dependency(node, format!("chain_{}", index - 1))?;
        }
    }

    Ok(flow)
}

pub fn wide(width: usize, delay: Duration) -> Result<Flow, GraphSpecError> {
    require_positive(width, "wide width")?;

    let mut flow = Flow::new();
    for index in 0..width {
        let node = format!("wide_{index}");
        flow.add_node(node.clone(), FakeTask::new(node).delay(delay))?;
    }

    Ok(flow)
}

pub fn fan_in(width: usize, delay: Duration) -> Result<Flow, GraphSpecError> {
    require_positive(width, "fan-in width")?;

    let mut flow = Flow::new();
    for index in 0..width {
        let node = format!("source_{index}");
        flow.add_node(node.clone(), FakeTask::new(node).delay(delay))?;
    }

    flow.add_node("reducer", FakeTask::new("reduced").delay(delay))?;

    for index in 0..width {
        flow.add_dependency("reducer", format!("source_{index}"))?;
    }

    Ok(flow)
}

pub fn layered(spec: LayeredSpec, delay: Duration) -> Result<Flow, GraphSpecError> {
    require_positive(spec.width, "layered width")?;
    require_positive(spec.depth, "layered depth")?;

    let mut flow = Flow::new();

    for layer in 0..spec.depth {
        for index in 0..spec.width {
            let node = layered_node(layer, index);
            flow.add_node(node.clone(), FakeTask::new(node.clone()).delay(delay))?;

            if layer > 0 {
                for previous in 0..spec.width {
                    flow.add_dependency(node.clone(), layered_node(layer - 1, previous))?;
                }
            }
        }
    }

    Ok(flow)
}

pub fn tree(spec: TreeSpec, delay: Duration) -> Result<Flow, GraphSpecError> {
    require_positive(spec.depth, "tree depth")?;
    require_positive(spec.branching, "tree branching")?;

    let mut flow = Flow::new();
    let leaf_start = spec.depth - 1;
    let leaf_count = node_count_at_depth(leaf_start, spec.branching)?;

    for index in 0..leaf_count {
        let node = tree_node(leaf_start, index);
        flow.add_node(node.clone(), FakeTask::new(node).delay(delay))?;
    }

    for level in (0..leaf_start).rev() {
        let count = node_count_at_depth(level, spec.branching)?;
        for index in 0..count {
            let node = tree_node(level, index);
            flow.add_node(node.clone(), FakeTask::new(node.clone()).delay(delay))?;

            for child in 0..spec.branching {
                let child_index = index * spec.branching + child;
                flow.add_dependency(node.clone(), tree_node(level + 1, child_index))?;
            }
        }
    }

    Ok(flow)
}

pub fn layered_node(layer: usize, index: usize) -> String {
    format!("layer_{layer}_{index}")
}

pub fn tree_node(level: usize, index: usize) -> String {
    format!("tree_{level}_{index}")
}

fn require_positive(value: usize, name: &str) -> Result<(), GraphSpecError> {
    if value == 0 {
        return Err(GraphSpecError::InvalidSpec(format!(
            "{name} must be greater than zero"
        )));
    }

    Ok(())
}

fn node_count_at_depth(depth: usize, branching: usize) -> Result<usize, GraphSpecError> {
    let exponent = u32::try_from(depth).map_err(|_| {
        GraphSpecError::InvalidSpec("tree depth is too large to calculate".to_string())
    })?;

    branching
        .checked_pow(exponent)
        .ok_or_else(|| GraphSpecError::InvalidSpec("tree node count overflowed usize".to_string()))
}

#[derive(Debug, thiserror::Error)]
pub enum GraphSpecError {
    #[error("{0}")]
    InvalidSpec(String),

    #[error(transparent)]
    Orchestra(#[from] OrchestraError),
}
