package monitoring

import (
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"

	"github.com/openshift/must-gather/pkg/flags"

	"k8s.io/client-go/kubernetes"
)

func GatherMetrics(ctx context.Context, clientset *kubernetes.Clientset, basePath string, matches ...string) (err error) {
	pods, err := runningMonitoringPods(ctx, clientset)
	if err != nil {
		return
	}

	out, err := os.Create(path.Join(basePath, "metrics.openmetrics.gz"))
	if err != nil {
		return
	}
	defer out.Close()

	w := gzip.NewWriter(out)
	defer w.Close()

	stderr, err := os.Create(path.Join(basePath, "metrics.stderr"))
	if err != nil {
		return
	}
	defer stderr.Close()

	args := []string{
		"exec", pods.Items[0].Name, "-c", "prometheus", "-n", "openshift-monitoring",
		"--",
		"promtool", "tsdb", "dump-openmetrics", "/prometheus", "--sandbox-dir-root=/prometheus",
	}

	if !flags.SinceTime.IsZero() {
		args = append(args, fmt.Sprintf("--min-time=%v", flags.SinceTime.UnixMilli()))
	}

	for _, m := range matches {
		args = append(args, fmt.Sprintf("--match=%v", m))
	}

	cmd := exec.CommandContext(ctx, "oc", args...)

	cmd.Stdout = w
	cmd.Stderr = stderr
	cmd.Env = os.Environ()

	return cmd.Run()
}
