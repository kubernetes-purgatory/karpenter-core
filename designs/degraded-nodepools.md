# RFC: Degraded NodePool Status Condition

## Overview

Karpenter can launch nodes with a NodePool that will never join a cluster when a NodeClass is misconfigured.

One example is that when a network path does not exist due to a misconfigured VPC (network access control lists, subnets, route tables), Karpenter will not be able to provision compute with that NodeClass that joins the cluster until the error is fixed. Crucially, this will continue to charge users for compute that can never be used in a cluster.

To improve visibility of these failure modes, this RFC proposes adding a `Degraded` status condition on the Nodepool that indicate to cluster users there may be a problem with a NodePool/NodeClass combination that needs to be investigated and corrected. 

**Note:** The issues that we want to address in the launch path with this status condition are the ones that happen during Nodeclaim launch and registration. Nodeclaim initialization never fails as of today, it can only transition from Unknown to True unless we plan on changing that in the future.   

## 🔑 Introduce a Degraded Status Condition on the NodePool

```
// ConditionTypeDegraded = "Degraded" condition indicates that a misconfiguration exists that prevents the normal, successful use of a Karpenter resource
ConditionTypeDegraded = "Degraded"
```
We want to introduce a `Degraded` status condition on the Nodepool. This condition can be set to either of Unknown, True or False. The state transition is not unidirectional meaning it can go from True to False and back to True or Unknown. `Degraded` status condition will not be a pre-condition for Nodepool readiness. A degraded Nodepool will continue to be considered for provisioning. We will only deprioritize it during provisioning among the nodepools with similar weight.

### Option 1: In-memory Buffer to store history - Recommended

This option will have an in-memory FIFO buffer, which will grow to a max size of 10 (this can be changed later). This buffer will store data about the success or failure in the launch path and is evaluated by a controller to determine the relative health of the NodePool. This will be an int buffer and a positive means `Degraded: False`, negative means `Degraded: True` and 0 means `Degraded: Unknown`. 

Evaluation conditions -

1. We start with an empty buffer with `Degraded: Unknown`.
2. There have to be 2 minimum failures in the buffer for `Degraded` to transition to `True` or basically the threshold would be 80%. 
3. If the buffer starts with a success then `Degraded: False`. 
4. If Karpenter restarts then we flush the buffer but don't change the existing state of `Degraded` status condition.
5. If there is an update to a Nodepool/Nodeclass, flush the buffer and set `Degraded: Unknown`.
6. Since the buffer is FIFO, we remove the oldest launch result when the max buffer size is reached.

See below for example evaluations:

```
Successful Launch: 1
Default: 0
Unsuccessful Launch: -1

[] = 'Degraded: Unknown'
[-1] = 'Degraded: Unknown'
[+1] = 'Degraded: False'
[-1, +1] = 'Degraded: False'
[-1, -1] = 'Degraded: True'
[-1, +1, -1] = 'Degraded: True'
[-1, +1, +1, +1, +1, +1, +1, +1, +1, +1] = 'Degraded: False'
```

#### Considerations

1. 👎 Heuristics can be wrong and mask failures
2. 👍 Keeps track of recent launch history.
3. 👍 Can be easily expanded if we want to update the buffer size depending on the cluster size.
4. 👍 Observability improvements so that users can begin triaging misconfigurations

### Option 2: `FailedLaunches` Status Field on the NodePool

Introduce a field called `FailedLaunches` on the NodePool status which stores the number of failed launches.

```
// NodePoolStatus defines the observed state of NodePool
type NodePoolStatus struct {
	// Resources is the list of resources that have been provisioned.
	// +optional
	Resources v1.ResourceList `json:"resources,omitempty"`
	// FailedLaunches tracks the number of times a nodepool failed before being marked degraded
	// +optional
	FailedLaunches int `json:"failedLaunches,omitempty"`
	// Conditions contains signals for health and readiness
	// +optional
	Conditions []status.Condition `json:"conditions,omitempty"`
}
```
Evaluation conditions -

1. If 3 or more NodeClaims fail to launch with a NodePool then the NodePool will be marked as degraded. The retries are included to account for transient errors.
2. Reset `FailedLaunches` to zero following an edit to the NodePool or when a sufficient amount time has passed. 
3. Once a NodePool is `Degraded`, it recovers with `Degraded: false` after an update to the NodePool or when the NodeClaim registration expiration TTL (currently 15 minutes) passes since the `lastTransitionTime` for the status condition on the NodePool, whichever comes first. 
4. A successful provisioning could also remove the status condition but this may cause more apiserver and metric churn than is necessary.

As additional misconfigurations are handled, they can be added to the `Degraded` status condition and the `Degraded` controller expanded to handle automated recovery efforts. This is probably most simply achieved by changing the Status Condition metrics to use comma-delimiting for `Reason`s with the most recent change present in the `Message`.

```
  - lastTransitionTime: "2024-12-16T12:34:56Z"
    message: "FizzBuzz component was misconfigured"
    observedGeneration: 1
    reason: FizzBuzzFailure,FooBarFailure
    status: "True"
    type: Degraded
```

This introduces challenges when determining when to evaluate contributors to the status condition but since the `Degraded` status condition only has a single contributor this decision can be punted. When the time comes to implement the multiple contributors to this status condition, this probably looks like a `Degraded` controller which acts as a "heartbeat" and evaluates each of the contributors.

#### Considerations

1. 👎 Three retries can still be a long time to wait on compute that never provisions correctly
2. 👎 Heuristics can be wrong and mask failures
3. 👍 Observability improvements so that users can begin triaging misconfigurations

### How Does this Affect Metrics and Improve Observability?
To improve observability, a new label can be added to metrics that tracks if the pod was expected to succeed for the NodePool configuration. Taking pod `provisioning_unbound_time_seconds` as an example, if a NodePool has never launched a node because a NACL blocks the network connectivity of the only zone for which it is configured, then this metric would be artificially higher than expected. Since Karpenter adds a label for if the NodePool has successfully launched a node before, users can view both the raw and filtered version of the pod unbound metric. From Karpenter's point-of-view, the pod should have bound successfully if the NodePool/NodeClass configuration had previously been used to launch a node.

Furthering the pod `provisioning_unbound_time_seconds` example:

```
PodProvisioningUnboundTimeSeconds = opmetrics.NewPrometheusGauge(
	crmetrics.Registry,
	prometheus.GaugeOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.PodSubsystem,
		Name:      "provisioning_unbound_time_seconds",
		Help:      "The time from when Karpenter first thinks the pod can schedule until it binds. Note: this calculated from a point in memory, not by the pod creation timestamp.",
	},
	[]string{podName, podNamespace, nodePoolDegraded},
)
```

`nodePoolDegraded` can then be used as an additional label filter. There is still usefulness in the unfiltered metric and users should be able to compare the two metrics.
