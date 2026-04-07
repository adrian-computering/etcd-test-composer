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
    observations = []
    for node in nodes:
        try:
            client = etcd3.client(host=node, port=2379)
            status = client.status()
            members = {m.id for m in client.members}
            observations.append({
                "node": node,
                "reachable": True,
                "leader_id": status.leader.id,
                "members": members,
                "error": None,
            })
            print(f"Client: connected to {node}")
        except Exception as e:
            observations.append({
                "node": node,
                "reachable": False,
                "leader_id": None,
                "members": None,
                "error": repr(e),
            })
            print(f"Client: Failed to reach node! Node ID: {node}, error: {repr(e)}")

    print(f"Client: Successfully queried for members and leaders across {len(observations)} nodes.")

    return observations


def validate_convergence(observations):
    # Per-node: each node must be reachable after quiescence.
    for obs in observations:
        always(
            obs["reachable"],
            "Each node is reachable after quiescence",
            {"node": obs["node"], "error": obs["error"]},
        )
    print(f"Client: Checked all nodes are reachable for {len(observations)} nodes.")

    reachable = [o for o in observations if o["reachable"]]
    # If nothing is reachable, there is nothing else meaningful to check. Other assertions fail automatically.
    if not reachable:
        return

    # Full cluster: Every reachable node agrees on the same list of cluster members.
    member_sets = [o["members"] for o in reachable]
    always(
        all(s == member_sets[0] for s in member_sets),
        "All reachable etcd nodes agree on cluster membership",
        {"node_member_views": [(o["node"], sorted(o["members"])) for o in reachable]}
    )
    print(f"Client: Checked for cluster membership agreement from {len(member_sets)} nodes.")

    # Full cluster: Every reachable node agrees on the same leader.
    leader_ids = [o["leader_id"] for o in reachable]
    always(
        len(set(leader_ids)) == 1,
        "All reachable etcd nodes agree on a single leader",
        {"observed_node_leaders": [(o["node"], o["leader_id"]) for o in reachable]},
    )

    # Per node: Every reachable node reports a leader and that leader is a cluster member.
    for o in reachable:
        always(
            o["leader_id"] != 0,
            "Each reachable node reports an elected leader after quiescence",
            {"node": o["node"], "leader_id": o["leader_id"]},
        )
        always(
            o["leader_id"] in o["members"],
            "Each reachable node's leader is a member of its own view of the cluster",
            {"node": o["node"], "leader_id": o["leader_id"], "node_members": sorted(o["members"])},
        )

    print(f"Client: Validated cluster membership and leadership for {len(reachable)} nodes.")


if __name__ == "__main__":
    cluster_observations = observe_members_and_leaders(cluster_nodes)
    # We test for 100% availability, then both leadership and membership convergence in a single driver,
    # in case a bug triggers a mixed fault
    validate_convergence(cluster_observations)

    print(f"Client: Tested for Raft convergence!")
