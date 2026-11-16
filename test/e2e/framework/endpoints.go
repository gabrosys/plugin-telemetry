package framework

import (
	"context"
	"fmt"
	"slices"

	discoveryv1 "k8s.io/api/discovery/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ReadyEndpoints returns the sorted names of the Pods that are ready endpoints of the Service's EndpointSlices.
func ReadyEndpoints(ctx context.Context, c client.Client, namespace, service string) ([]string, error) {
	var endpointSlices discoveryv1.EndpointSliceList
	if err := c.List(ctx, &endpointSlices, client.InNamespace(namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: service}); err != nil {
		return nil, fmt.Errorf("list endpoint slices of %s/%s: %w", namespace, service, err)
	}

	var names []string
	for _, slice := range endpointSlices.Items {
		for _, ep := range slice.Endpoints {
			if ep.TargetRef == nil || ep.TargetRef.Kind != "Pod" {
				continue
			}
			if ep.Conditions.Ready == nil || *ep.Conditions.Ready {
				names = append(names, ep.TargetRef.Name)
			}
		}
	}
	slices.Sort(names)

	return names, nil
}
