#[path = "../examples/support/synthetic_graphs.rs"]
mod synthetic_graphs;

use std::time::Duration;
use synthetic_graphs::{layered_node, tree_node, LayeredSpec, TreeSpec};

#[test]
fn builds_chain_graph() {
    let flow = synthetic_graphs::chain(4, Duration::ZERO).unwrap();

    assert_eq!(flow.nodes().len(), 4);
    assert_eq!(flow.nodes()["chain_0"].dependencies, Vec::<String>::new());
    assert_eq!(
        flow.nodes()["chain_3"].dependencies,
        vec!["chain_2".to_string()]
    );
    flow.validate().unwrap();
}

#[test]
fn builds_wide_graph() {
    let flow = synthetic_graphs::wide(5, Duration::ZERO).unwrap();

    assert_eq!(flow.nodes().len(), 5);
    assert!(flow
        .nodes()
        .values()
        .all(|node| node.dependencies.is_empty()));
    flow.validate().unwrap();
}

#[test]
fn builds_fan_in_graph() {
    let flow = synthetic_graphs::fan_in(3, Duration::ZERO).unwrap();

    assert_eq!(flow.nodes().len(), 4);
    assert_eq!(flow.nodes()["reducer"].dependencies.len(), 3);
    flow.validate().unwrap();
}

#[test]
fn builds_layered_graph() {
    let flow =
        synthetic_graphs::layered(LayeredSpec { width: 3, depth: 4 }, Duration::ZERO).unwrap();

    assert_eq!(flow.nodes().len(), 12);
    assert_eq!(flow.nodes()[&layered_node(0, 0)].dependencies.len(), 0);
    assert_eq!(flow.nodes()[&layered_node(3, 2)].dependencies.len(), 3);
    flow.validate().unwrap();
}

#[test]
fn builds_tree_graph() {
    let flow = synthetic_graphs::tree(
        TreeSpec {
            depth: 4,
            branching: 2,
        },
        Duration::ZERO,
    )
    .unwrap();

    assert_eq!(flow.nodes().len(), 15);
    assert_eq!(flow.nodes()[&tree_node(0, 0)].dependencies.len(), 2);
    assert_eq!(flow.nodes()[&tree_node(3, 0)].dependencies.len(), 0);
    flow.validate().unwrap();
}
