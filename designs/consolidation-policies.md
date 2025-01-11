# Consolidation Policies

## Problem Statement

Karpenter provides 3 forms of consolidation: single-node, multi-node, and emptiness consolidation. Single-node replaces expensive nodes with cheaper nodes that satisfy the workloads requirements. Multi-node replaces multiple nodes with a single, larger node that can satisfy workload requirements. Emptiness removes nodes that have no workloads that need to be relocated to reduce costs.

Customers want to have control over the types of consolidation that can occur within a nodepool. Some nodepools may only be used for job type workloads (crons, jobs, spark, etc) where single-node consolidation is dangerous and wastes execution when it disrupts pods. Other nodepools may be used mostly by long-running daemons, where daemonset costs are more significant and multi-node consolidation to binpack into larger nodes would be preferred.

Karpenter's consolidation considers the price of instances when deciding to consolidate, but does not consider the cost of disruption. This can harm cluster stability, if the price improvement of the node is small, or the workloads disrupted are very costly to rebuild to their previous running state (e.g. long-running job that must restart from scratch).

Karpenter's consolidation is an all or nothing offering today. Nodepools can disable consolidation and let only empty nodes be removed, or customers can opt into all forms of consolidation.

## Design

### Option 1: Add price thresholds, merge consolidation controllers
The motivation for the additional controls is the high cost of disruption of certain workloads for small or negative cost savings. So instead of offering controls, the proposal is to add price improvement thresholds for consolidation, to avoid this scenario.

The proposal is to find a price improvement threshold which accurate represents the "cost" of disrupting the workloads. This threshold could be an arbitrary-fixed value (e.g. 20% price improvement), or heuristically computed by Karpenter based on the cluster shape (e.g. nodepool ttl's might be a heuristic input).

The separation of the single-node and multi-node consolidation is arbitrary. The single-node consolidation finds the "least costly" eligible node to disrupt, and uses that as a solution, even if it's a poor solution. However, the least-costly eligible node to disrupt is not necessarily the most cost-savings node to disrupt, but the single-node consolidation stops after it has any solution.

In contrast, the multi-node consolidation spends as much time as possible to find a solution. So while price thresholds could be implemented into each consolidation controller, it is simpler to consider merging their responsabilities into one shared controller based on multi-node consolidation. This combined consolidation controller could search as long as possible for the most effective consolidation outcome, from emptiness, multi-node or single-node replacements.

#### Pros/Cons
* 👍👍 No new configuration for the `NodePool` crd
* ~ More complex implementation for consolidation, but easier to understand (only 1 controller for consolidation).
* 👎 It's possible a one-size fits all price threshold will satisfy some customers and not others
* 👎 High amount of engineering effort to implement.

### Option 2: Refactor and Expand Consolidation Policies
Another approach would be to not make significant changes to consolidation, but expose controls to allow enabling or disabling various consolidation controllers. Karpenter already presents the binary consolidation options. The `NodePool` resource has a disruption field that exposes the `consolidationPolicy` field. This field is an enum with two supported values: `WhenEmpty` and `WhenUnderutilized`.

The proposal is to add two new consolidation policy enum values:

* `WhenCheaper`
* `WhenUnderutilizedOrCheaper`

And change the semantics of the existing consolidation policy enum value:

* `WhenUnderutilized`

The semantics of each of the enum values matches their names, making it straightforward to explain to customers or users. `WhenUnderutilizedOrCheaper` becomes the new default. `WhenUnderutilizedOrCheaper` is the same as `WhenUnderutilized` previously. `WhenUnderutilized` semantics changes to only allow emptiness or multi-node consolidation to occur for a nodepool. `WhenCheaper` only allows emptiness or single-node consolidation to occur for a nodepool.

#### Pros/Cons
* 👍 Simple configuration for the `NodePool` crd
* 👍 Simple implementation for the various consolidation controllers.
* 👎 If Karpenter adds consolidation modes with new purposes, more `consolidationPolicy` enum values will be needed to make them toggleable.

### Option 3: Add Consolidation Configuration to NodePools
Karpenter's `NodePool` resource could expose fine-grained consolidation controls to allow configuring each controller, as well as controller-specific subfields (e.g multi-node consolidation's `maxParallel` setting).

A possible `NodePool` partial resource might look like this:
```yaml
spec:
 disruption:
   consolidation:
     singleNode:
       enabled: true
     multiNode:
       enabled: true
       maxParallel: 100
     emptiness:
       enabled: true
       emptyThreshold: "30%"
```

#### Pros/Cons
* 👍 Fine-grained controls for the various consolidation controllers.
* 👎 Complex configuration for the `NodePool` crd
* 👎 Complex implementation for the various consolidation controllers.
* 👎 Limits Karpenter's ability to change / remove existing consolidation controllers, as config will be tightly coupled.

