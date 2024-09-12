/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduling

import (
	"fmt"
	"math"

	"github.com/awslabs/operatorpkg/option"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

type TopologyType byte

const (
	TopologyTypeSpread TopologyType = iota
	TopologyTypePodAffinity
	TopologyTypePodAntiAffinity
)

func (t TopologyType) String() string {
	switch t {
	case TopologyTypeSpread:
		return "topology spread"
	case TopologyTypePodAffinity:
		return "pod affinity"
	case TopologyTypePodAntiAffinity:
		return "pod anti-affinity"
	}
	return ""
}

// TopologyGroup is used to track pod counts that match a selector by the topology domain (e.g. SELECT COUNT(*) FROM pods GROUP BY(topology_ke
type TopologyGroup struct {
	// Hashed Fields
	Key        string
	Type       TopologyType
	maxSkew    int32
	minDomains *int32
	namespaces sets.Set[string]
	selector   *metav1.LabelSelector
	nodeFilter TopologyNodeFilter
	// Index
	owners       map[types.UID]struct{} // Pods that have this topology as a scheduling rule
	domains      map[string]int32       // TODO(ellistarn) explore replacing with a minheap
	emptyDomains sets.Set[string]       // domains for which we know that no pod exists
}

func NewTopologyGroup(topologyType TopologyType, topologyKey string, pod *v1.Pod, namespaces sets.Set[string], labelSelector *metav1.LabelSelector, maxSkew int32, minDomains *int32, taintPolicy *v1.NodeInclusionPolicy, affinityPolicy *v1.NodeInclusionPolicy, domainGroup TopologyDomainGroup) *TopologyGroup {
	// the nil *TopologyNodeFilter always passes which is what we need for affinity/anti-affinity
	var nodeSelector TopologyNodeFilter
	if topologyType == TopologyTypeSpread {
		nodeTaintsPolicy := v1.NodeInclusionPolicyIgnore
		if taintPolicy != nil {
			nodeTaintsPolicy = *taintPolicy
		}
		nodeAffinityPolicy := v1.NodeInclusionPolicyHonor
		if affinityPolicy != nil {
			nodeAffinityPolicy = *affinityPolicy
		}
		nodeSelector = MakeTopologyNodeFilter(pod, nodeTaintsPolicy, nodeAffinityPolicy)
	}

	domains := map[string]int32{}
	if nodeSelector.TaintPolicy == v1.NodeInclusionPolicyHonor {
		domainGroup.ForEachToleratedDomain(pod, func(domain string) {
			domains[domain] = 0
		})
	} else {
		domainGroup.ForEachDomain(func(domain string) {
			domains[domain] = 0
		})
	}

	emptyDomains := sets.New[string]()
	domainGroup.ForEachDomain(func(domain string) {
		emptyDomains.Insert(domain)
	})

	return &TopologyGroup{
		Type:         topologyType,
		Key:          topologyKey,
		namespaces:   namespaces,
		selector:     labelSelector,
		nodeFilter:   nodeSelector,
		maxSkew:      maxSkew,
		domains:      domains,
		emptyDomains: emptyDomains,
		owners:       map[types.UID]struct{}{},
		minDomains:   minDomains,
	}
}

func (t *TopologyGroup) Get(pod *v1.Pod, podDomains, nodeDomains *scheduling.Requirement) *scheduling.Requirement {
	switch t.Type {
	case TopologyTypeSpread:
		return t.nextDomainTopologySpread(pod, podDomains, nodeDomains)
	case TopologyTypePodAffinity:
		return t.nextDomainAffinity(pod, podDomains, nodeDomains)
	case TopologyTypePodAntiAffinity:
		return t.nextDomainAntiAffinity(podDomains)
	default:
		panic(fmt.Sprintf("Unrecognized topology group type: %s", t.Type))
	}
}

func (t *TopologyGroup) Record(domains ...string) {
	for _, domain := range domains {
		t.domains[domain]++
		t.emptyDomains.Delete(domain)
	}
}

// Counts returns true if the pod would count for the topology, given that it schedule to a node with the provided
// requirements
func (t *TopologyGroup) Counts(pod *v1.Pod, requirements scheduling.Requirements, compatabilityOptions ...option.Function[scheduling.CompatibilityOptions]) bool {
	return t.selects(pod) && t.nodeFilter.MatchesRequirements(requirements, compatabilityOptions...)
}

// Register ensures that the topology is aware of the given domain names.
func (t *TopologyGroup) Register(domains ...string) {
	for _, domain := range domains {
		if _, ok := t.domains[domain]; !ok {
			t.domains[domain] = 0
			t.emptyDomains.Insert(domain)
		}
	}
}

func (t *TopologyGroup) AddOwner(key types.UID) {
	t.owners[key] = struct{}{}
}

func (t *TopologyGroup) RemoveOwner(key types.UID) {
	delete(t.owners, key)
}

func (t *TopologyGroup) IsOwnedBy(key types.UID) bool {
	_, ok := t.owners[key]
	return ok
}

// Hash is used so we can track single topologies that affect multiple groups of pods.  If a deployment has 100x pods
// with self anti-affinity, we track that as a single topology with 100 owners instead of 100x topologies.
func (t *TopologyGroup) Hash() uint64 {
	return lo.Must(hashstructure.Hash(struct {
		TopologyKey   string
		Type          TopologyType
		Namespaces    sets.Set[string]
		LabelSelector *metav1.LabelSelector
		MaxSkew       int32
		NodeFilter    TopologyNodeFilter
	}{
		TopologyKey:   t.Key,
		Type:          t.Type,
		Namespaces:    t.namespaces,
		LabelSelector: t.selector,
		MaxSkew:       t.maxSkew,
		NodeFilter:    t.nodeFilter,
	}, hashstructure.FormatV2, &hashstructure.HashOptions{SlicesAsSets: true}))
}

// nextDomainTopologySpread returns a scheduling.Requirement that includes a node domain that a pod should be scheduled to.
// If there are multiple eligible domains, we return any random domain that satisfies the `maxSkew` configuration.
// If there are no eligible domains, we return a `DoesNotExist` requirement, implying that we could not satisfy the topologySpread requirement.
func (t *TopologyGroup) nextDomainTopologySpread(pod *v1.Pod, podDomains, nodeDomains *scheduling.Requirement) *scheduling.Requirement {
	// min count is calculated across all domains
	min := t.domainMinCount(podDomains)
	selfSelecting := t.selects(pod)

	minDomain := ""
	minCount := int32(math.MaxInt32)
	for domain := range t.domains {
		// but we can only choose from the node domains
		if nodeDomains.Has(domain) {
			// comment from kube-scheduler regarding the viable choices to schedule to based on skew is:
			// 'existing matching num' + 'if self-match (1 or 0)' - 'global min matching num' <= 'maxSkew'
			count := t.domains[domain]
			if selfSelecting {
				count++
			}
			if count-min <= t.maxSkew && count < minCount {
				minDomain = domain
				minCount = count
			}
		}
	}
	if minDomain == "" {
		// avoids an error message about 'zone in [""]', preferring 'zone in []'
		return scheduling.NewRequirement(podDomains.Key, v1.NodeSelectorOpDoesNotExist)
	}
	return scheduling.NewRequirement(podDomains.Key, v1.NodeSelectorOpIn, minDomain)
}

func (t *TopologyGroup) domainMinCount(domains *scheduling.Requirement) int32 {
	// hostname based topologies always have a min pod count of zero since we can create one
	if t.Key == v1.LabelHostname {
		return 0
	}

	min := int32(math.MaxInt32)
	var numPodSupportedDomains int32
	// determine our current min count
	for domain, count := range t.domains {
		if domains.Has(domain) {
			numPodSupportedDomains++
			if count < min {
				min = count
			}
		}
	}
	if t.minDomains != nil && numPodSupportedDomains < *t.minDomains {
		min = 0
	}
	return min
}

func (t *TopologyGroup) nextDomainAffinity(pod *v1.Pod, podDomains *scheduling.Requirement, nodeDomains *scheduling.Requirement) *scheduling.Requirement {
	options := scheduling.NewRequirement(podDomains.Key, v1.NodeSelectorOpDoesNotExist)
	for domain := range t.domains {
		if podDomains.Has(domain) && t.domains[domain] > 0 {
			options.Insert(domain)
		}
	}

	// If pod is self selecting and no pod has been scheduled yet, we can pick a domain at random to bootstrap scheduling

	if options.Len() == 0 && t.selects(pod) {
		// First try to find a domain that is within the intersection of pod/node domains. In the case of an in-flight node
		// this causes us to pick the domain that the existing in-flight node is already in if possible instead of picking
		// a random viable domain.
		intersected := podDomains.Intersection(nodeDomains)
		for domain := range t.domains {
			if intersected.Has(domain) {
				options.Insert(domain)
				break
			}
		}

		// and if there are no node domains, just return the first random domain that is viable
		for domain := range t.domains {
			if podDomains.Has(domain) {
				options.Insert(domain)
				break
			}
		}
	}
	return options
}

func (t *TopologyGroup) nextDomainAntiAffinity(domains *scheduling.Requirement) *scheduling.Requirement {
	options := scheduling.NewRequirement(domains.Key, v1.NodeSelectorOpDoesNotExist)
	// pods with anti-affinity must schedule to a domain where there are currently none of those pods (an empty
	// domain). If there are none of those domains, then the pod can't schedule and we don't need to walk this
	// list of domains.  The use case where this optimization is really great is when we are launching nodes for
	// a deployment of pods with self anti-affinity.  The domains map here continues to grow, and we continue to
	// fully scan it each iteration.
	for domain := range t.emptyDomains {
		if domains.Has(domain) && t.domains[domain] == 0 {
			options.Insert(domain)
		}
	}
	return options
}

// selects returns true if the given pod is selected by this topology
func (t *TopologyGroup) selects(pod *v1.Pod) bool {
	selector, err := metav1.LabelSelectorAsSelector(t.selector)
	if err != nil {
		selector = labels.Nothing()
	}
	return t.namespaces.Has(pod.Namespace) && selector.Matches(labels.Set(pod.Labels))
}
