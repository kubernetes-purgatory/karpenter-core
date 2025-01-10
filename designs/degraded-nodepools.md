# RFC: Misconfigured NodePool Observability

## Overview

Karpenter can launch nodes with a NodePool that will never join a cluster when a NodeClass is misconfigured.

One example is that when a network path does not exist due to a misconfigured VPC (network access control lists, subnets, route tables), Karpenter will not be able to provision compute with that NodeClass that joins the cluster until the error is fixed. Crucially, this will continue to charge users for compute that can never be used in a cluster.

To improve visibility of these failure modes, this RFC proposes mechanisms that indicate to cluster users there may be a problem with a NodePool/NodeClass combination that needs to be investigated and corrected.

## Options

### Option 1: Introduce a `Degraded` Status Condition on the NodePool

```
// ConditionTypeDegraded = "Degraded" condition indicates that a misconfiguration exists that prevents the normal, successful use of a Karpenter resource
ConditionTypeDegraded = "Degraded"
```

This option would set `Degraded: true` on a NodePool whenever Karpenter suspects something is wrong with the launch path but isn't sure. In this case, if 3 or more NodeClaims fail to launch with a NodePool then the NodePool will be marked as degraded. The retries are included to account for transient errors. The number of failed launches is stored as a status on the NodePool and then reset to zero following an edit to the NodePool or a sufficient amount time has passed.

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

Once a NodePool is `Degraded`, it recovers with `Degraded: false` after an update to the NodePool or when the NodeClaim registration expiration TTL (currently 15 minutes) passes since the `lastTransitionTime` for the status condition on the NodePool, whichever comes first. A `Degraded` NodePool is not passed over when provisioning and may continue to be chosen during scheduling. A successful provisioning could also remove the status condition but this may cause more apiserver and metric churn than is necessary.

As additional misconfigurations are handled, they can be added to the `Degraded` status condition and the `Degraded` controller expanded to handle automated recovery efforts. This is probably most simpoly achieved by changing the Status Condition metrics to use comma-delimiting for `Reason`s with the most recent change present in the `Message`.

```
  - lastTransitionTime: "2024-12-16T12:34:56Z"
    message: "FizzBuzz component was misconfigured"
    observedGeneration: 1
    reason: FizzBuzzFailure,FooBarFailure
    status: "True"
    type: Degraded
```

This introduces challenges when determining when to evaluate contributors to the status condition but since the `Degraded` status condition only has a single contributor this decision can be punted. When the time comes to implement the multiple contributors to this status condition, this probably looks like a `Degraded` controller which acts as a "heartbeat" and evaluates each of the contributors.

Finally, this status condition would not be a precondition for NodePool `Readiness` because the NodePool should still be considered for scheduling purposes.

#### Considerations

1. 👎 Three retries can still be a long time to wait on compute that never provisions correctly
2. 👎 Heuristics can be wrong and mask failures
3. 👍 Observability improvements so that users can begin triaging misconfigurations
4. 👍 `Degraded` is not a pre-condition for NodePool readiness

### Option 2: Expand `Validated` Status Condition and Use Reasons

The implementation is roughly the same except that validation is a pre-condition for `Readiness`. This has impact in a larger portion of the code because `Validated` would no longer block provisioning or `Readiness`. However, it is still an option that Karpenter could expand the `Valdiated` status condition so that any time a misconfiguration is encountered, the NodePool is treated as having failed validation.

#### Considerations

1. 👎👎 Validation implies the original state of the NodePool was correct and is something Karpenter can vet with certainty. A NodePool could have been correctly validated but then degraded.
2. 👎👎 Changes the meaning of `Validated` in terms of `Readiness`
3. 👎 Relies on statuses that were not part of the original validation of a NodePool
4. 👍 Status condition already exists

### Option 3: Utilize a Launch Result Buffer Per NodePool

Introduce a fixed size buffer that tracks launch results and is evaluated to determine the relative health of the NodePool. A controller then evaluates that buffer with some regularity (maybe 15 seconds) and updates the `Degraded` status condition based on the results. This looks like an int buffer and a positive means `Degraded: False` and negative means `Degraded: True`. This can also be done on a % basis to create a threshold for a NodePool to go `Degraded: Unknown`. The buffer doesn't need to persist because the controller should only evaluate recent launches. While the results below are for a contrived example, in actuality these entries could be seen as `Success`, `Registration Failure`, `Auth Failure`, etc to help improve visibility about why the NodePool has degraded.

See below for example evaluations:

```
Successful Launch: 1
Default: 0
Unsuccessful Launch: -1

[1, -1, 1, 1] = 3, `Degraded: False`
[1, -1 , 1, -1] = 0, `Degraded: Unknown`
[-1, -1, -1, 1] = -2, `Degraded: True`
```

One issue is determining when to clear or expire buffer entries. There are a few options:

1) Clear the buffer after 3x the registration TTL to appropriately capture enough failures, even in small clusters
2) Clear the buffer once every hour
3) Expire one entry in the buffer after every evaluation but this approach would mean that zeroes are used to continue with the current status condition as opposed to going `Unknown`

One major drawback of this approach is that it relies entirely on heuristics which can be wrong and mask failures.

#### Considerations

1. 👎👎 Due to heavy usage of heurstics, can mask Karpenter failures
2. 👎 Over-engineered
3. 👍 Keeps track of recent launches and addresses NodePool configurations that sometimes succeed
4. 👍 Can be easily expanded to use enumerated types of failures

### [Recommended] Option 4: Track if a NodePool/NodeClass Successfully Launched a Node

This approach proposes tracking if a NodePool configuration has ever successfully launched using a `Verified` status.

Given this would update only for the first node, this adds a neglible amount of apiserver traffic. There are a couple options to keep track of this info:

1) Karpenter persists no info about past successes, only the current NodePool/NodeClass configuration. This likely exists on `NodePoolStatus` as `Verfied`. In this case, the status could be skipped and Karpenter can keep this in memory. If kept in memory, Karpenter would do the same thing as the initial addition of a NodePool and not write the status until it sees a node launch. If nodes are already provisioned using a NodePool then Karpenter can determine if there are nodes from the NodePool via the `Resources` status.

```
// NodePoolStatus defines the observed state of NodePool
type NodePoolStatus struct {
	// Resources is the list of resources that have been provisioned.
	// +optional
	Resources v1.ResourceList `json:"resources,omitempty"`
	// LaunchedNode indicates if the current NodePool configuration has launched a node.
	// +optional
	Verified bool `json:"verified,omitempty"`
	// Conditions contains signals for health and readiness
	// +optional
	Conditions []status.Condition `json:"conditions,omitempty"`
}
```

2) [Recommended] Karpenter maintains successful launch history in the NodePool status as an bool map of NodeClass name+UIDs to successful launch. A NodeClass is only added on success.

```
// NodePoolStatus defines the observed state of NodePool
type NodePoolStatus struct {
	// Resources is the list of resources that have been provisioned.
	// +optional
	Resources v1.ResourceList `json:"resources,omitempty"`
	// Launches inidicates which NodeClasses this NodePool used to launch a node.
	// +optional
	VerifiedNodeClasses map[string]bool `json:"launches,omitempty"`
	// Conditions contains signals for health and readiness
	// +optional
	Conditions []status.Condition `json:"conditions,omitempty"`
}
```

Given the additional usefulness of historical successes, the second approach is recommended. The NodeClass is only added to the "Verified" map after a node has fully initialized in the cluster for that NodePool/NodeClass combination. It's also possible that a user could update the NodeClass reference on a NodePool and then revert it after some time but in that case Karpenter would still want to know if it used to be able to provision compute with that NodePool/NodeClass combination.

NodeClasses can be updated so one consideration is that the map is actually a map of NodeClass to observed generation. This is possible but would require a lot of GETs in order to correctly match generation on NodeClass and NodePool as well as race conditions where Karpenter is unable to read its own writes.

#### Considerations

1. 👎 Still prone to noise in metrics by allowing false positives when a NodePool/NodeClass combination hasn't yet been vetted
2. 👍👍 Abstracts away from individual nodeclaim results and increases awareness of unique NodePool/NodeClass interactions
3. 👍 By using previous successes as a way to filter metrics for a given NodePool/NodeClass, future failures can be seen as a stronger signal something has been changed and is misconfigured

### How Does this Affect Metrics and Improve Observability?
To improve observability, a new label will be added to metrics that tracks if the pod was expected to succeed for the NodePool configuration. Taking pod `provisioning_unbound_time_seconds` as an example, if a NodePool has never launched a node because a NACL blocks the network connectivity of the only zone for which it is configured, then this metric would be artificially higher than expected. Since Karpenter adds a label for if the NodePool has successfully launched a node before, users can view both the raw and filtered version of the pod unbound metric. From Karpenter's point-of-view, the pod should have bound successfully if the NodePool/NodeClass configuration had previously been used to launch a node.

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
	[]string{podName, podNamespace, nodePoolVerified},
)
```

`nodePoolVerified` can then be used as an additional label filter. There is still usefulness in the unfiltered metric and users should be able to compare the two metrics.

### Further Dicussion Needed

However this is tracked, should it be used to affect Karpenter functionality/scheduling or should it only exist to improve observability? For example, affected NodePools be seen as having a lower weight than normal so that other NodePools are prioritized. This is probably more surprising than not for most users and should not be considered pursuing.