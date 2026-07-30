package monitor

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestParseClusterNames(t *testing.T) {
	got := ParseClusterNames(" cluster1-fis, cluster2-fis ,")
	if len(got) != 2 || got[0] != "cluster1-fis" || got[1] != "cluster2-fis" {
		t.Fatalf("unexpected: %v", got)
	}
	if ParseClusterNames("") != nil {
		t.Fatal("expected nil for empty")
	}
}

func TestConditionTrue(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{"type": "Available", "status": "True"},
				map[string]interface{}{"type": "SubmarinerConnectionDegraded", "status": "False"},
			},
		},
	}}
	if !conditionTrue(obj, "Available") {
		t.Fatal("expected Available=True")
	}
	if conditionTrue(obj, "Degraded") {
		t.Fatal("expected Degraded missing")
	}
	if conditionStatus(obj, "SubmarinerConnectionDegraded") != "False" {
		t.Fatal("expected SubmarinerConnectionDegraded=False")
	}
}
