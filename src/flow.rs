use crate::{OrchestraError, Task};
use std::{collections::HashMap, sync::Arc};

pub type NodeId = String;

#[derive(Clone)]
pub struct Node {
    pub id: NodeId,
    pub task: Arc<dyn Task>,
    pub dependencies: Vec<NodeId>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("id", &self.id)
            .field("dependencies", &self.dependencies)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Default)]
pub struct Flow {
    nodes: HashMap<NodeId, Node>,
}

impl Flow {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node<T>(&mut self, id: impl Into<NodeId>, task: T) -> Result<(), OrchestraError>
    where
        T: Task + 'static,
    {
        let id = id.into();
        if self.nodes.contains_key(&id) {
            return Err(OrchestraError::DuplicateNode(id));
        }

        self.nodes.insert(
            id.clone(),
            Node {
                id,
                task: Arc::new(task),
                dependencies: Vec::new(),
            },
        );
        Ok(())
    }

    pub fn add_dependency(
        &mut self,
        node: impl Into<NodeId>,
        dependency: impl Into<NodeId>,
    ) -> Result<(), OrchestraError> {
        let node = node.into();
        let dependency = dependency.into();

        if !self.nodes.contains_key(&dependency) {
            return Err(OrchestraError::MissingDependency { node, dependency });
        }

        let Some(target) = self.nodes.get_mut(&node) else {
            return Err(OrchestraError::MissingNode(node));
        };

        if !target.dependencies.contains(&dependency) {
            target.dependencies.push(dependency);
        }

        Ok(())
    }

    pub fn validate(&self) -> Result<(), OrchestraError> {
        let mut visiting = HashMap::new();

        for id in self.nodes.keys() {
            self.visit(id, &mut visiting)?;
        }

        Ok(())
    }

    pub fn nodes(&self) -> &HashMap<NodeId, Node> {
        &self.nodes
    }

    fn visit(
        &self,
        id: &NodeId,
        visiting: &mut HashMap<NodeId, VisitState>,
    ) -> Result<(), OrchestraError> {
        match visiting.get(id) {
            Some(VisitState::Visiting) => return Err(OrchestraError::CycleDetected),
            Some(VisitState::Visited) => return Ok(()),
            None => {}
        }

        let Some(node) = self.nodes.get(id) else {
            return Err(OrchestraError::MissingNode(id.clone()));
        };

        visiting.insert(id.clone(), VisitState::Visiting);
        for dependency in &node.dependencies {
            if !self.nodes.contains_key(dependency) {
                return Err(OrchestraError::MissingDependency {
                    node: id.clone(),
                    dependency: dependency.clone(),
                });
            }
            self.visit(dependency, visiting)?;
        }
        visiting.insert(id.clone(), VisitState::Visited);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VisitState {
    Visiting,
    Visited,
}
