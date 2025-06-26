package insights

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/openshift/must-gather/internal"
	"github.com/openshift/must-gather/pkg/flags"
	"github.com/openshift/must-gather/pkg/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

var (
	Name          = "insights"
	insightsError = errors.New("gather insights error")
)

func insightsOperatorPod(ctx context.Context, clientset *kubernetes.Clientset) (pods *v1.PodList, err error) {
	return util.RunningPods(ctx, clientset, "openshift-insights", "")
}

func gatherInsightArchives(ctx context.Context, operatorPod v1.Pod, outPath string) {
	var stderr strings.Builder

	args := []string{
		"rsync",
		"-n", "openshift-insights",
		fmt.Sprintf("openshift-insights/%v:/var/lib/insights-operator", operatorPod.Name),
		outPath,
	}

	cmd := exec.CommandContext(ctx, "oc", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = &stderr
	cmd.Env = os.Environ()

	if err := cmd.Run(); err != nil {
		slog.Error(insightsError.Error(), "error", err, "stderr", stderr.String())
		return
	}
}

func Gather(ctx context.Context, logger *slog.Logger) {
	clientset, err := util.NewClientSet()
	if err != nil {
		slog.Error(insightsError.Error(), "error", err)
		return
	}

	pods, err := insightsOperatorPod(ctx, clientset)
	if err != nil {
		slog.Error(insightsError.Error(), "error", err)
		return
	}

	outPath := path.Join(flags.BaseCollectionPath, "insights-data")
	if err := os.MkdirAll(outPath, 0755); err != nil {
		slog.Error(insightsError.Error(), "error", err)
		return
	}

	internal.Group.Start(func() {
		gatherInsightArchives(ctx, pods.Items[0], outPath)
	})
}
