#!/usr/bin/env bash

# Add the conversion stanza to the CRD spec to enable conversion via webhook
yq eval '.spec.conversion = {"strategy": "Webhook", "webhook": {"conversionReviewVersions": ["v1beta1", "v1"], "clientConfig": {"service": {"name": "karpenter", "namespace": "kube-system", "port": 8443}}}}' -i pkg/apis/crds/karpenter.sh_nodeclaims.yaml
yq eval '.spec.conversion = {"strategy": "Webhook", "webhook": {"conversionReviewVersions": ["v1beta1", "v1"], "clientConfig": {"service": {"name": "karpenter", "namespace": "kube-system", "port": 8443}}}}' -i pkg/apis/crds/karpenter.sh_nodepools.yaml