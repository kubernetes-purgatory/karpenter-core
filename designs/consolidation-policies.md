# Consolidation Policies

## Problem Statement

Karpenter provides 3 forms of consolidation: single-node, multi-node, and emptiness consolidation. Single-node replaces expensive nodes with cheaper nodes that satisfy the workloads requirements. Multi-node replaces multiple nodes with a single, larger node that can satisfy workload requirements. Emptiness removes nodes that have no workloads that need to be relocated to reduce costs.

Customers want to have control over the types of consolidation that can occur within a nodepool. Some nodepools may only be used for job type workloads (crons, jobs, spark, etc) where single-node consolidation is dangerous and wastes execution when it disrupts pods. Other nodepools may be used mostly by long-running daemons, where daemonset costs are more significant and multi-node consolidation to binpack into larger nodes would be preferred.

Karpenter's consolidation estimates the price of instances when deciding to consolidate, but does not consider the cost of disruption. This can harm cluster stability, if the price improvement of the node is small, or the workloads disrupted are very costly to rebuild to their previous running state (e.g. long-running job that must restart from scratch).

Karpenter's consolidation is an all or nothing offering today. Nodepools can disable consolidation and let only empty nodes be removed, or customers can opt into all forms of consolidation.

## Design

Split off to a separate PR: https://github.com/kubernetes-sigs/karpenter/pull/1433