# Multi-node Consolidation Partitioning

## Background

Multi-node consolidation actions currently attempt to replace N nodes with a single node that can satisfy all the tenant workloads. This is desirable because there is a daemonset cost and baseline kubelet overhead associated with each node in a cluster, so reducing the node count by merging smaller nodes can be cheaper even if node costs are equal.

## Problem Statement

In a cluster that has many non-homogeneous nodepools (typically due to multi-tenancy), or where nodepools support multiple cpu architectures, multi-node consolidation fails because tenant workloads are unlikely to be successfully migrated to different nodepool or different cpu architecture.

While tenant workloads may support multiple cpu architectures, in practice we have rarely observed workloads be configured to attempt to spread their workloads fairly across multiple cpu architectures. Tenant workloads typically prefer the cpu architecture they were most optimized for, and customers rarely pursue having a multi-architecture deployment strategy. Instead one cpu architecture is typically favored, for performance, cost, availability, or other reasons. When a cluster has multiple such tenants, with different cpu architecture choices, this results in inconsolidatable groups of nodes, which could still be consolidated within their group.

A similar problem occurs with non-homogeneous nodepools. In multi-tenant clusters, nodepools for each customer often have requirements that differ. Each tenant workload will target nodes which match their nodepool requirements. The more multi-tenancy in a cluster, the more likely many nodes with incompatible requirements will exist.  

As a result, multi-node consolidation suffers when the cluster is not homogeneous. In a non-homogeneous clusters, candidate nodes will contain tenant workloads that have few or no valid destinations due to the cpu architecture or nodepool mismatching. Ultimately, this results in extra daemonsets costs and kubelet overhead on the controlplane, as non-homogeneous clusters will accumulate many tiny nodes that fail to be consolidated.

## Proposal

Multi-node consolidation should be partitioned into multiple consolidation actions, each of which is responsible for consolidating nodes that are homogenous in terms of cpu architecture and nodepool. This will allow for more successful consolidation actions, and will reduce the number of nodes in the cluster more effectively.

A naive, simple approach would be to have Karpenter create M bins for multi-node consolidation, based on cpu architecture and the nodepool of the node. Multi-node consolidation would sort the bins by the number of nodes contained, and attempt multi-node consolidation on the nodes within each bin, starting with the largest, descending. After any solution is found, return the solution.

## Drawbacks

### Homogeneous Nodepools
Partitioning multi-node consolidation may result in fewer successful consolidations if a cluster has many nodepools which are homogeneous (e.g weighted fallback nodepools). The naive proposal will result in potentially consolidatable nodes in two or more different nodepools not being consolidated together. 

One possible way to mitigate this would be to make Karpenter smart enough to know if two nodepools requirements are similar, and then treat these nodepools as one bin. The simplest approach would be to look for equality in the nodepool requirements. 
