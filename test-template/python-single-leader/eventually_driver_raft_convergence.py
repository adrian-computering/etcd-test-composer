#!/usr/bin/env -S python3 -u

"""
Eventually driver verifying post-quiescence Raft convergence.
Checks that every node agrees on cluster membership,
and that the cluster has a single, consistent, known leader among its members.
Catches membership disagreement, leader disagreement (split-brain), stuck elections, and reported non-member leaders.
Ref: https://antithesis.com/docs/test_templates/test_composer_reference/#eventually-command
"""

# Antithesis SDK
from antithesis.assertions import (
    always,
)

# etcd
import etcd3

cluster_nodes = ("etcd0", "etcd1", "etcd2")


def observe_members_and_leaders(nodes):
    node_member_views = []
    node_leaders = []

    for node in nodes:
        # no try/except: during Eventually fault injection is off, so if we fail to reach a node we should announce it.
        client = etcd3.client(host=node, port=2379)
        status = client.status()
        leader_id = status.leader.id
        node_member_views.append((node, {m.id for m in client.members}))
        node_leaders.append((node, leader_id))
    print(f"Client: Queried cluster for members and leaders across {len(nodes)} nodes.")
    return node_member_views, node_leaders


def validate_member_agreement(node_member_views):
    all_member_sets = [view for _, view in node_member_views]

    # Antithesis assertion that all our cluster nodes always agree on the same set of member IDs
    always(
        all(s == all_member_sets[0] for s in all_member_sets),
        "All etcd nodes agree on cluster membership",
        {"node_member_views": [(n, sorted(v)) for n, v in node_member_views]}
    )
    print(f"Client: Checked for cluster membership agreement from {len(all_member_sets)} nodes.")



def validate_leaders(observed_node_leaders, observed_member_views):
    leader_ids = [leader_id for _, leader_id in observed_node_leaders]
    # Antithesis Assertion for the entire cluster:
    # All nodes in the cluster agree on the same elected leader.
    always(
        len(set(leader_ids)) == 1,
        "All etcd nodes agree on a single leader",
        {"observed_node_leaders": observed_node_leaders},
    )

    # Node-specific Antithesis assertions:
    node_members_by_name = dict(observed_member_views)
    for node, leader_id in observed_node_leaders:
        # Assertion: Each node reports a leader.
        always(
            leader_id != 0,
            "Each node reports an elected leader after quiescence",
            {"node": node, "leader_id": leader_id},
        )

        node_members = node_members_by_name[node]
        # Assertion: Each node's leader is member of its own view of the cluster
        always(
            leader_id in node_members,
            "Each node's leader is a known member of its own view of the cluster",
            {"node": node, "leader_id": leader_id, "node_members": sorted(node_members)}
        )
    print(f"Client: Validated leaders on {len(node_members_by_name)} nodes.")


if __name__ == "__main__":
    member_views, leaders = observe_members_and_leaders(cluster_nodes)

    # We test for both leadership and membership convergence in a single driver,
    # in case a bug triggers both
    validate_member_agreement(member_views)
    validate_leaders(leaders, member_views)
    print(f"Client: Tested for Raft convergence!")
