// Package v1alpha1 holds the TelemetryExporter and MetricQuerySet custom resources.
//
// +kubebuilder:object:generate=true
// +groupName=telemetry.gabrosys.github.io
package v1alpha1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the group and version of the custom resources.
var GroupVersion = schema.GroupVersion{Group: "telemetry.gabrosys.github.io", Version: "v1alpha1"}

var schemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

// AddToScheme registers the custom resources in s.
func AddToScheme(s *runtime.Scheme) error {
	if err := schemeBuilder.AddToScheme(s); err != nil {
		return fmt.Errorf("add telemetry.gabrosys.github.io/v1alpha1 to scheme: %w", err)
	}

	return nil
}

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion,
		&TelemetryExporter{}, &TelemetryExporterList{},
		&MetricQuerySet{}, &MetricQuerySetList{},
	)
	metav1.AddToGroupVersion(s, GroupVersion)

	return nil
}
