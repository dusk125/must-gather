package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"

	"github.com/openshift/must-gather/pkg/flags"
	"github.com/openshift/must-gather/pkg/gather/metrics"
	"github.com/openshift/must-gather/pkg/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

var (
	Name = "etcd"
)

const (
	ETCDCTL_CONTAINER = "etcdctl"
)

func runningEtcdPods(ctx context.Context, clientset *kubernetes.Clientset) (pods *v1.PodList, err error) {
	return util.RunningPods(ctx, clientset, "openshift-etcd", "app=etcd")
}

func etcdEndpoints(ctx context.Context, runningEtcdPod v1.Pod) (endpoints []string, err error) {
	var stdout, stderr strings.Builder
	c := exec.CommandContext(ctx, "oc", "exec", runningEtcdPod.Name, "-n", "openshift-etcd", "-c", ETCDCTL_CONTAINER, "--", "etcdctl", "member", "list")
	c.Stdout = &stdout
	c.Stderr = &stderr
	c.Env = os.Environ()
	c.Dir = flags.BaseCollectionPath

	if err = c.Run(); err != nil {
		err = fmt.Errorf("%v [%v]", err, stderr.String())
		return nil, err
	}

	for s := range strings.SplitSeq(stdout.String(), "\n") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}

		pieces := strings.Split(s, ", ")
		endpoints = append(endpoints, pieces[4])
	}

	return
}

func ocp4etcdctl(ctx context.Context, logger *slog.Logger, outFile any, runningEtcdPod v1.Pod, etcdEndpoints []string, args ...string) {
	var stdout io.Writer
	switch t := outFile.(type) {
	case string:
		out, err := os.Create(t)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer out.Close()
		stdout = out
	case io.Writer:
		stdout = t
	}

	var stderr strings.Builder
	a := append([]string{
		"exec", runningEtcdPod.Name, "-n", "openshift-etcd", "-c", ETCDCTL_CONTAINER, "--", "etcdctl",
	}, args...)
	c := exec.CommandContext(ctx, "oc", a...)
	c.Stdout = stdout
	c.Stderr = &stderr
	c.Env = append(slices.Clone(os.Environ()), "ETCDCTL_ENDPOINTS="+strings.Join(etcdEndpoints, ","))
	c.Dir = flags.BaseCollectionPath

	if err := c.Run(); err != nil {
		logger.Error(gatherEtcdError.Error(), "error", err, "stderr", stderr.String())
		return
	}
}

var (
	gatherEtcdError = errors.New("gather etcd error")
)

func Gather(ctx context.Context, logger *slog.Logger) {
	etcdLogPath := path.Join(flags.BaseCollectionPath, "etcd_info")
	if err := os.MkdirAll(etcdLogPath, 0755); err != nil {
		logger.Error(gatherEtcdError.Error(), "error", err)
		return
	}

	clientset, err := util.NewClientSet()
	if err != nil {
		logger.Error(gatherEtcdError.Error(), "error", err)
		return
	}

	pods, err := runningEtcdPods(ctx, clientset)
	if err != nil {
		logger.Error(gatherEtcdError.Error(), "error", err)
		return
	}

	if len(pods.Items) == 0 {
		logger.Error(gatherEtcdError.Error(), "error", "no running etcd pods found")
		return
	}

	endpoints, err := etcdEndpoints(ctx, pods.Items[0])
	if err != nil {
		logger.Error(gatherEtcdError.Error(), "error", err)
		return
	}

	slog.Info("Getting information from pod", "pod", pods.Items[0].Name, "container", ETCDCTL_CONTAINER)
	slog.Info("Using endpoints", "endpoints", endpoints)

	g := wait.Group{}
	g.Start(func() {
		ocp4etcdctl(ctx, logger, path.Join(etcdLogPath, "member_list.json"), pods.Items[0], endpoints, "member", "list", "-w", "json")
	})
	g.Start(func() {
		ocp4etcdctl(ctx, logger, path.Join(etcdLogPath, "endpoint_status.json"), pods.Items[0], endpoints, "endpoint", "status", "-w", "json")
	})
	g.Start(func() {
		ocp4etcdctl(ctx, logger, path.Join(etcdLogPath, "endpoint_health.json"), pods.Items[0], endpoints, "endpoint", "health", "-w", "json")
	})
	g.Start(func() {
		ocp4etcdctl(ctx, logger, path.Join(etcdLogPath, "alarm_list.json"), pods.Items[0], endpoints, "alarm", "list", "-w", "json")
	})
	g.Start(func() {
		getObjectCounts(ctx, logger, path.Join(etcdLogPath, "object_count.json"), pods.Items[0], endpoints)
	})
	g.Start(func() {
		err := metrics.GatherMetrics(ctx, clientset, etcdLogPath,
			"etcd_disk_wal_fsync_duration_seconds_bucket{job=~\".*etcd.*\"}",
			"etcd_network_peer_sent_failures_total{job=~\".*etcd.*\"}",
			"etcd_network_peer_round_trip_time_seconds_bucket{job=~\".*etcd.*\"}",
			"etcd_server_proposals_failed_total{job=~\".*etcd.*\"}",
			"etcd_disk_backend_commit_duration_seconds_bucket{job=~\".*etcd.*\"}",
			"grpc_server_handling_seconds_bucket{job=~\".*etcd.*\", grpc_method!=\"Defragment\", grpc_type=\"unary\"}",
			"grpc_server_handled_total{job=~\".*etcd.*\"}",
		)
		if err != nil {
			logger.Error(gatherEtcdError.Error(), "error", err)
		}
	})

	g.Wait()
}

func getObjectCounts(ctx context.Context, logger *slog.Logger, outFile string, pod v1.Pod, endpoints []string) {
	var out strings.Builder

	ocp4etcdctl(ctx, logger, &out, pod, endpoints, "get", "/", "--prefix", "--keys-only")

	countMap := util.SortedMap[string, int]{}
	for key := range strings.SplitSeq(out.String(), "\n") {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		for i, keyType := range util.SplitSeq2(key, "/") {
			if i == 2 {
				if c, has := countMap[keyType]; has {
					countMap[keyType] = c + 1
				} else {
					countMap[keyType] = 1
				}
				break
			}
		}
	}

	f, err := os.Create(outFile)
	if err != nil {
		logger.Error(gatherEtcdError.Error(), "error", err)
		return
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(countMap); err != nil {
		logger.Error(gatherEtcdError.Error(), "error", err)
		return
	}
}
