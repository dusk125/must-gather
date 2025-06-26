package priorityandfairness

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"

	"github.com/openshift/must-gather/internal"
	"github.com/openshift/must-gather/pkg/flags"
	"github.com/openshift/must-gather/pkg/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/client-go/kubernetes"
)

var (
	Name = "priority_and_fairness"

	pafError      = errors.New("gather priority and fairness error")
	namespacePath = "namespaces/openshift-kube-apiserver"
	podPath       = "kube-apiserver/kube-apiserver/api_priority_and_fairness"
	cutoffVersion = version.MustParseGeneric("4.6")
)

func getClusterVersion(ctx context.Context) (v *version.Version, err error) {
	var stdout strings.Builder
	cmd := exec.CommandContext(ctx, "oc", "get", "clusterversion", "version", "-o", "jsonpath='{.status.desired.version}'")
	cmd.Stdout = &stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = flags.BaseCollectionPath
	cmd.Env = os.Environ()

	if err = cmd.Run(); err != nil {
		return
	}

	s := strings.Trim(stdout.String(), "'")
	return version.ParseMajorMinor(s)
}

func getKubeAPIServers(ctx context.Context, clientset *kubernetes.Clientset) (pods *v1.PodList, err error) {
	return util.RunningPods(ctx, clientset, "openshift-kube-apiserver", "apiserver")
}

func collect(ctx context.Context, podIP string, podPort int32, query, outDir, outFile string) {
	stdout, err := os.Create(path.Join(outDir, outFile))
	if err != nil {
		slog.Error(pafError.Error(), "error", err)
		return
	}
	defer stdout.Close()

	cmd := exec.CommandContext(ctx, "oc", "get", "--raw", fmt.Sprintf("https://%v:%v/debug/api_priority_and_fairness/%v", podIP, podPort, query))
	cmd.Stderr = os.Stderr
	cmd.Stdout = stdout
	cmd.Env = os.Environ()

	_ = cmd.Run()
}

func Gather(ctx context.Context, logger *slog.Logger) {
	version, err := getClusterVersion(ctx)
	if err != nil {
		logger.Error(pafError.Error(), "error", err)
		return
	}

	if version.LessThan(cutoffVersion) {
		logger.Info("current verison is less than cutoff version", "current", version, "cutoff", cutoffVersion)
		// skip this if less than version 4.6
		return
	}

	clientset, err := util.NewClientSet()
	if err != nil {
		logger.Error(pafError.Error(), "error", err)
		return
	}

	apiServers, err := getKubeAPIServers(ctx, clientset)
	if err != nil {
		logger.Error(pafError.Error(), "error", err)
		return
	}

	apfPath := path.Join(flags.BaseCollectionPath, namespacePath)
	for _, api := range apiServers.Items {
		internal.Group.Start(func() {
			outDir := path.Join(apfPath, "pods", api.Name, podPath)
			if err := os.MkdirAll(outDir, 0755); err != nil {
				logger.Error(pafError.Error(), "error", err)
				return
			}

			podIP := api.Status.PodIP
			if podIP == "" {
				logger.Error("unabled to get podIP", "api-server", api.Name)
				return
			}

			i := slices.IndexFunc(api.Spec.Containers, func(cont v1.Container) bool {
				return cont.Name == "kube-apiserver"
			})
			if i < 0 || len(api.Spec.Containers[i].Ports) == 0 || api.Spec.Containers[i].Ports[0].HostPort == 0 {
				logger.Error("unabled to get hostPort", "api-server", api.Name)
				return
			}
			podPort := api.Spec.Containers[i].Ports[0].HostPort

			collect(ctx, podIP, podPort, "dump_priority_levels", outDir, "priority_levels")
			collect(ctx, podIP, podPort, "dump_queues", outDir, "queues")
			collect(ctx, podIP, podPort, "dump_requests", outDir, "requests")
		})
	}
}
