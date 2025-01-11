# Node Overlays

Node launch decisions are the output a scheduling simulation algorithm that involves satisfying pod scheduling constraints with available instance type offerings. Cloud provider implementations are responsible for surfacing these offerings to the Karpenter scheduler through the [cloudprovider.GetInstanceTypes()](https://github.com/kubernetes-sigs/karpenter/blob/37db06f4742eada19934f76e616f2b1860aca57b/pkg/cloudprovider/types.go#L59) API. This separation of responsibilities enables cloud providers to make simplifying assumptions, but limits extensibility for more advanced use cases. NodeOverlays enable customers to inject alternative assumptions into the scheduling simulation for more accurate scheduling simulations.

## Use Cases

### Price

Each instance offering comes with a price, which is used as a minimization function in Karpenter's scheduling algorithm. However, Karpenter cannot perfectly understand all factors that influence price, such as an external vendor's fee, a pricing discount, or an external motiviation such as carbon-offset.

* https://github.com/aws/karpenter-provider-aws/pull/4686
* https://github.com/aws/karpenter-provider-aws/issues/3860
* https://github.com/aws/karpenter-provider-aws/pull/4697

### Extended Resources

InstanceTypes have a set of well known resource capacities, such as CPU, memory, or GPUs. However, Kubernetes supports arbitrary [extended resources](https://kubernetes.io/docs/tasks/administer-cluster/extended-resource-node/), which cannot be known prior to Node launch. Karpenter lacks a mechanism to teach the simulation about extended resource capacities.

* https://github.com/kubernetes-sigs/karpenter/issues/751
* https://github.com/kubernetes-sigs/karpenter/issues/729

### Resource Overhead

* https://github.com/aws/karpenter-provider-aws/issues/5161

System software such as containerd, kubelet, and the host operating system come with resource overhead like CPU and memory, which is substracted from a Node's capacity. Karpenter attempts to calculate this overhead where it is known, but cannot predict the overhead required by custom operating systems. To complicate this, the overhead often changes for each version of an operating system.

## Proposal

NodeOverlays are a new Karpenter API that enable customers to fine-tune the scheduling simulation for advanced use cases. NodeOverlays can be configured with a `metav1.Selector` which flexibly constrain when they are applied to a simulation. Selectors can match well-known labels like `node.kubernetes.io/instance-type` or `karpenter.sh/node-pool` and custom labels defined in a NodePool's `.spec.labels` or `.spec.requirements`. When multiple NodeOverlays match an offering for a given simulation, they are merged together using `spec.weight` to resolve conflicts.

### Examples

Configure price to match a compute savings plan
```yaml
kind: NodeOverlay
metadata:
  name: default-discount
spec:
  pricePercent: 90
```

Support extended resource types (e.g. smarter-devices/fuse)
* https://github.com/kubernetes-sigs/karpenter/issues/751
* https://github.com/kubernetes-sigs/karpenter/issues/729

```yaml
kind: NodeOverlay
metadata:
  name: extended-resource
spec:
  selector:
    matchLabels:
      karpenter.sh/capacity-type: on-demand
    matchExpressions:
    - key: node.kubernetes.io/instance-type
      operator: In
      values: ["m5.large", "m5.2xlarge", "m5.4xlarge", "m5.8xlarge", "m5.12xlarge"]
  capacity:
    smarter-devices/fuse: 1
```

Add memory overhead of of 10Mi and 50Mi for all instances with more than 2Gi memory
* https://github.com/aws/karpenter-provider-aws/issues/5161

```yaml
kind: NodeOverlay
metadata:
  name: memory-default
spec:
  overhead:
    memory: 10Mi
---
kind: NodeOverlay
metadata:
  name: memory
spec:
  weight: 1
  selector:
    matchExpressions:
      key: karpenter.k8s.aws/instance-memory
      operator: Gt
      values: [2048]
  overhead:
    memory: 50Mi
---
```

### Selectors

NodeOverlay's selector semantic allows for fine grained control over when values are injected into the scheduling simulation. An empty selector applies to all simulations, or 1:n with InstanceType. Selectors could be written that define a 1:1 relationship between NodeOverlay and InstanceType. Selectors can even be written to apply n:1 with InstanceType, through additional labels that constrain by NodePool, NodeClass, zone, or other scheduling dimensions.

A [previous proposal](https://github.com/aws/karpenter-provider-aws/pull/2390/files#diff-b7121b2d822e57b70203794f3b6367e7173b16d84070c3712cd2f15b8dbd11b2R133) explored the tradeoffs between a 1:1 semantic vs more flexible alternatives. It raised valid observability challenges around communicating to customers exactly how the CR was modifying the scheduling simulation. It recommended a 1:1 approach, with the assumption that some system would likely dynamically codegenerate the CRs for the relevant use case. We received [direct feedback](https://github.com/aws/karpenter-provider-aws/pull/2404#issuecomment-1250688882) from some customers that the number of variations would be combinatorial, and thus unmanagable for their use case. Ultimately, we paused this analysis in favor of other priorities.

After two years of additional customer feedback in this problem space, this proposal asserts that the flexibility of a selector-based approach is a signficant simplification for customers compared to a 1:1 approach and avoids the scalability challenges associated with persisting large numbers of CRs in the Kubernetes API. This decision will require us to work through the associated observability challenges.

### Merging

Our decision to pursue Selectors that allow for an n:m semantic presents a challenge where multiple NodeOverlays could match a simulation, but with different values. To address this, we will support an optional `.spec.weight` parameter (discussed in the [previous proposal](https://github.com/aws/karpenter-provider-aws/pull/2390/files#diff-b7121b2d822e57b70203794f3b6367e7173b16d84070c3712cd2f15b8dbd11b2R153)).

We have a few choices for dealing with conflicting NodeOverlays

1. Multiple NodeOverlays are merged in order of weight
2. Multiple NodeOverlays are applied sequentially in order of weight
3. One NodeOverlay is selected by weight

We recommend option 1, as it supports the common use case of default + override semantic. In the memory overhead example, both NodeOverlays attempt to define the same parameter, but one is more specific than the other. Option 1 and 3 allow us to support this use case, where option 2 would result in 50Mi+10Mi. We are not aware of use cases that would benefit from sequential application.

We choose option 1 over option 3 to support NodeOverlays that define multiple values. For example, we could collapse the example NodeOverlays using a merge semantic.

```yaml
kind: NodeOverlay
metadata:
  name: default
spec:
  pricePercent: 90
  overhead:
    memory: 10Mi
---
kind: NodeOverlay
metadata:
  name: memory
spec:
  weight: 1 # required to resolve conflicts with default
  selector:
    matchExpressions:
      key: karpenter.k8s.aws/instance-memory
      operator: Gt
      values: [2048]
  overhead:
    memory: 50Mi
---
kind: NodeOverlay
metadata:
  name: extended-resource
spec:
  # weight is not required since default does not conflict with extended resources
  selector:
    matchLabels:
      karpenter.sh/capacity-type: on-demand
    matchExpressions:
    - key: node.kubernetes.io/instance-type
      operator: In
      values: ["m5.large", "m5.2xlarge", "m5.4xlarge", "m5.8xlarge", "m5.12xlarge"]
  capacity:
    smarter-devices/fuse: 1
```

Option 1 enables us to be strictly more flexible than option 3. However, it introduces ambiguity when multiple NodeOverlays have the same weight. Option 3 would only allow one to apply, so any conflict must be a misconfiguration. However, our example demonstrates that Option 1 has valid use cases where multiple NodeOverlays share the same weight, as long as they don't define the same value. For Option 1, NodeOverlays with the same weight that both attempt to configure the same field are considered to be a misconfiguration.

### Misconfiguration

Given our merge semantics, it's possible for customers to define multiple NodeOverlays with undefined behavior. In both cases, we will identify and communicate these misconfigurations, but should we fail open or fail closed? Failing closed means that a misconfigured would halt Karpenter from launching new Nodes until the misconfiguration is resolved. Failing open means that Karpenter might launch capacity that differs from what an operator expects.

We could choose to fail closed to prevent undefined behavior, and within this, we have the option to fail closed for the entire provisioning loop, or for a single NodePool. For example, if multiple NodeOverlays match NodePoolA with the same weight, but only a single NodeOverlay matches NodePoolB, Karpenter could exclude NodePoolA as invalid, but proceed with NodePoolB for simulation. These semantics are very similar to how Karpenter deals with `.spec.limits` or `InsufficientCapacityErrors`. See: https://karpenter.sh/docs/concepts/scheduling/#fallback.

We choose to fail open, but to do so in a consistent way. If two NodeOverlays conflict and have the same weight, we will merge them in alphabetical order. Karpenter customers may find this semantic familiar, as this is exactly how NodePool weights are resolved. We prefer this conflict resolution approach over failing closed, since it avoids the failure mode where any customer (i.e. most use cases) using a single NodePool would suffer a scaling outage due to misconfigured NodeOverlays.

### Observability

Exposing the full details of the mutations caused by NodeOverlays cannot be feasibly stored in ETCD. Further, we do not want to require that customers read the Karpenter logs to debug this feature. When GetInstanceTypes is called per NodePool, we will emit an event on the NodePool to surface deeper semantic information about the effects of the NodeOverlays. Our implementation will need to be conservative on the volume and frequency of information, but provide sufficient insight for customers to verify that their configurations are working as expected.

### Per-Field Semantics

The semantics of each field carry individual nuance. Price could be modeled directly via a `price` field, but we can significantly reduce the number of NodeOverlays that need to be defined if we instead model the semantic as a function of the currently defined price (e.g. `pricePercent` or `priceAdjustment`). Similarly, `capacity` and `overhead` make sense to be modeled directly, but `capacityPercent` makes much less sense than `overheadPercent`.

We expect these semantics to be specific to each field, and do not attempt to define or support a domain specific language (e.g. CEL) for arbitrary control over these fields. We expect this API to evolve as we learn about the specific use cases.

### Launch Scope

To accelerate feedback on this new mechanism, we will release this API as v1alpha1, with support for a single field: `pricePercent`. All additional scope described in this doc will be left to followon RFCs and implementation effort.