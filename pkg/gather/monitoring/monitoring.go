package monitoring

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path"

	"github.com/openshift/must-gather/internal"
	"github.com/openshift/must-gather/pkg/flags"
	"github.com/openshift/must-gather/pkg/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

var (
	Name            = "monitoring"
	monitoringError = errors.New("gather monitoring error")
)

func runningMonitoringPods(ctx context.Context, clientset *kubernetes.Clientset) (pods *v1.PodList, err error) {
	return util.RunningPods(ctx, clientset, "openshift-monitoring", "prometheus=k8s")
}

func runningAlertManagerPods(ctx context.Context, clientset *kubernetes.Clientset) (pods *v1.PodList, err error) {
	return util.RunningPods(ctx, clientset, "openshift-monitoring", "alertmanager=main")
}

func promGet(ctx context.Context, pod v1.Pod, object string, outName ...string) {

	out := object
	if len(outName) > 0 && outName[0] != "" {
		out = outName[0]
	}

	result := path.Join(flags.BaseCollectionPath, "prometheus", out)
	if err := os.MkdirAll(path.Dir(result), 0755); err != nil {
		slog.Error(monitoringError.Error(), "error", err)
		return
	}

	stdout, err := os.Create(result + ".json")
	if err != nil {
		slog.Error(monitoringError.Error(), "error", err)
		return
	}
	defer stdout.Close()

	stderr, err := os.Create(result + ".stderr")
	if err != nil {
		slog.Error(monitoringError.Error(), "error", err)
		return
	}
	defer stderr.Close()

	args := []string{
		"exec", pod.Name,
		"-c", "prometheus",
		"-n", "openshift-monitoring",
		"--",
		"/bin/bash", "-c", fmt.Sprintf("curl -sG http://localhost:9090/api/v1/%v", object),
	}

	cmd := exec.CommandContext(ctx, "oc", args...)
	cmd.Env = os.Environ()
	cmd.Stderr = stderr
	cmd.Stdout = stdout

	_ = cmd.Run()
}

func alertmanagerGet(ctx context.Context, pod v1.Pod, object string, outName ...string) {
	out := object
	if len(outName) > 0 && outName[0] != "" {
		out = outName[0]
	}

	result := path.Join(flags.BaseCollectionPath, "alertmanager", out)
	if err := os.MkdirAll(path.Dir(result), 0755); err != nil {
		slog.Error(monitoringError.Error(), "error", err)
		return
	}

	stdout, err := os.Create(result + ".json")
	if err != nil {
		slog.Error(monitoringError.Error(), "error", err)
		return
	}
	defer stdout.Close()

	stderr, err := os.Create(result + ".stderr")
	if err != nil {
		slog.Error(monitoringError.Error(), "error", err)
		return
	}
	defer stderr.Close()

	args := []string{
		"exec", pod.Name,
		"-c", "alertmanager",
		"-n", "openshift-monitoring",
		"--",
		"/bin/bash", "-c", fmt.Sprintf("curl -sG http://localhost:9090/api/v2/%v", object),
	}

	cmd := exec.CommandContext(ctx, "oc", args...)
	cmd.Env = os.Environ()
	cmd.Stderr = stderr
	cmd.Stdout = stdout

	_ = cmd.Run()
}

func Gather(ctx context.Context, logger *slog.Logger) {
	clientset, err := util.NewClientSet()
	if err != nil {
		logger.Error(monitoringError.Error(), "error", err)
		return
	}

	internal.Group.Start(func() {
		pods, err := runningMonitoringPods(ctx, clientset)
		if err != nil {
			logger.Error(monitoringError.Error(), "error", err)
			return
		}

		promGet(ctx, pods.Items[0], "alertmanager")
		promGet(ctx, pods.Items[0], "rules")
		promGet(ctx, pods.Items[0], "status/config")
		promGet(ctx, pods.Items[0], "status/flags")

		// Get the following from each of the replicas
		for _, pod := range pods.Items {
			promGet(ctx, pod, "status/runtimeinfo")
			promGet(ctx, pod, "targets?state=active", "active-targets")
			promGet(ctx, pod, "status/tsdb")
		}
	})

	internal.Group.Start(func() {
		pods, err := runningAlertManagerPods(ctx, clientset)
		if err != nil {
			logger.Error(monitoringError.Error(), "error", err)
			return
		}

		alertmanagerGet(ctx, pods.Items[0], "status")
	})
}
