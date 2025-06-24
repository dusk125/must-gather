package resources

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strings"

	"github.com/openshift/must-gather/pkg/flags"
	"k8s.io/apimachinery/pkg/util/wait"
)

var (
	resourceGatherError = errors.New("gather resource error")
	Name                = "resources"
	NamedResources      = []string{
		"ns/openshift-cluster-version",                                      // Cluster version information
		"ns/default", "ns/openshift", "ns/kube-system", "ns/openshift-etcd", // Namespaces/project resources
		"ns/assisted-installer", // Assisted installer
	}
	GroupResources = []string{
		"clusterversion",                  // Cluster version information
		"clusteroperators", "apiservices", // Operator and APIService resources
		"certificatesigningrequests",                                                                                                                                 // Certificate resources
		"nodes",                                                                                                                                                      // Machine/nodes resources
		"storageclasses", "persistentvolumes", "volumeattachments", "csidrivers", "csinodes", "volumesnapshotclasses", "volumesnapshotcontents", "clustercsidrivers", // Storage resources
		"imagecontentsourcepolicies.operator.openshift.io",                                                     // Image-source resources
		"networks.operator.openshift.io",                                                                       // Networking resources
		"prioritylevelconfigurations.flowcontrol.apiserver.k8s.io", "flowschemas.flowcontrol.apiserver.k8s.io", // Flowcontrol - API priority and fairness (APF)
		"clusterresourcequotas.quota.openshift.io", // ClusterResourceQuota
	}
	AllNamespacesResources = []string{
		"csistoragecapacities", // Storage resources
		"leases",
	}
)

func runInspect(ctx context.Context, logger *slog.Logger, args ...string) {
	a := []string{
		"adm", "inspect",
	}
	a = append(a, args...)
	cmd := exec.CommandContext(ctx, "oc", a...)
	fmt.Println(cmd.String())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = flags.BaseCollectionPath
	cmd.Env = os.Environ()

	if err := cmd.Run(); err != nil {
		logger.Error(resourceGatherError.Error(), "error", err)
		return
	}
}

func gatherResources(ctx context.Context, logger *slog.Logger, logCollectionArgs string, resources []string, allNamespaces bool) {
	args := []string{}

	if logCollectionArgs != "" {
		args = append(args, logCollectionArgs)
	}

	args = append(args, "--dest-dir", "must-gather", "--rotated-pod-logs")
	args = append(args, strings.Join(resources, ","))

	if allNamespaces {
		args = append(args, "--all-namespaces")
	}

	runInspect(ctx, logger, args...)
}

func Gather(ctx context.Context, logger *slog.Logger) {
	var logCollectionArgs string

	if !flags.SinceTime.IsZero() {
		logCollectionArgs = fmt.Sprintf("--since=\"%v\"", flags.SinceTime)
	}

	g := wait.Group{}

	g.Start(func() {
		gatherResources(ctx, logger, logCollectionArgs, NamedResources, false)
	})
	g.Start(func() {
		filtered := slices.DeleteFunc(GroupResources, func(r string) bool {
			return exec.CommandContext(ctx, "oc", "get", r).Run() != nil
		})
		gatherResources(ctx, logger, logCollectionArgs, filtered, false)
	})
	g.Start(func() {
		gatherResources(ctx, logger, logCollectionArgs, AllNamespacesResources, true)
	})

	g.Wait()
}
