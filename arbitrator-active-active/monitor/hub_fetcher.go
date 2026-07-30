package monitor

import (
	"context"
	"fmt"
	"os"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	managedClusterGVR = schema.GroupVersionResource{
		Group:    "cluster.open-cluster-management.io",
		Version:  "v1",
		Resource: "managedclusters",
	}
	managedClusterAddonGVR = schema.GroupVersionResource{
		Group:    "addon.open-cluster-management.io",
		Version:  "v1beta1",
		Resource: "managedclusteraddons",
	}
)

// HubFetcher reads ManagedCluster + Submariner addon status from the ACM hub.
type HubFetcher struct {
	client   dynamic.Interface
	clusters []string
}

// NewHubFetcherFromEnv builds a fetcher using in-cluster config or KUBECONFIG.
func NewHubFetcherFromEnv(clusters []string) (*HubFetcher, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = clientcmd.RecommendedHomeFile
		}
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("load kubeconfig: %w", err)
		}
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	return &HubFetcher{client: client, clusters: clusters}, nil
}

// Fetch returns per-cluster hub reachability and whether Submariner addons look healthy.
// Signature matches SubmarinerMonitor fetcher: (reachability, submarinerConnected, error).
//
// Reachability: ManagedClusterJoined=True and Available is not False.
// (Available=Unknown is common when lease heartbeats stall; Joined is the durable signal.)
//
// Submariner: all watched sites report SubmarinerConnectionDegraded=False
// (reason ConnectionsEstablished). Addon Available is ignored — it tracks registration
// leases, not cable health.
func (f *HubFetcher) Fetch() (map[string]bool, bool, error) {
	ctx := context.Background()
	reachability := make(map[string]bool, len(f.clusters))
	for _, name := range f.clusters {
		reachability[name] = false
	}

	list, err := f.client.Resource(managedClusterGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("list managedclusters: %w", err)
	}

	for _, item := range list.Items {
		name := item.GetName()
		if !contains(f.clusters, name) {
			continue
		}
		joined := conditionTrue(&item, "ManagedClusterJoined")
		availableFalse := conditionStatus(&item, "ManagedClusterConditionAvailable") == "False"
		reachability[name] = joined && !availableFalse
	}

	submarinerOK := true
	for _, name := range f.clusters {
		if !reachability[name] {
			submarinerOK = false
			continue
		}
		addon, err := f.client.Resource(managedClusterAddonGVR).Namespace(name).Get(ctx, "submariner", metav1.GetOptions{})
		if err != nil {
			submarinerOK = false
			continue
		}
		// Degraded=True means the cable is not established.
		if conditionTrue(addon, "SubmarinerConnectionDegraded") {
			submarinerOK = false
			continue
		}
		// Prefer an explicit healthy signal when present.
		if status := conditionStatus(addon, "SubmarinerConnectionDegraded"); status != "False" {
			// Missing/Unknown degraded condition: fall back to agent not degraded + manifests applied.
			if conditionTrue(addon, "SubmarinerAgentDegraded") || !conditionTrue(addon, "ManifestApplied") {
				submarinerOK = false
			}
		}
	}

	return reachability, submarinerOK, nil
}

func conditionTrue(obj *unstructured.Unstructured, condType string) bool {
	return strings.EqualFold(conditionStatus(obj, condType), "True")
}

func conditionStatus(obj *unstructured.Unstructured, condType string) string {
	conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return ""
	}
	for _, c := range conditions {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if fmt.Sprint(m["type"]) == condType {
			return fmt.Sprint(m["status"])
		}
	}
	return ""
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// ParseClusterNames splits CLUSTER_NAMES env (comma-separated).
func ParseClusterNames(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
