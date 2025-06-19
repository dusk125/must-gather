package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	BaseCollectionPath string
)

func main() {
	root := cobra.Command{}

	root.PersistentFlags().StringVar(&BaseCollectionPath, "base-collection-path", "/must-gather", "")

	root.AddCommand(NewGatherCommand())

	_ = root.Execute()
}

func NewGatherCommand() *cobra.Command {
	c := &cobra.Command{
		Use: "gather",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 && args[0] == "all" {
				switch args[0] {
				case "all":
					for _, c := range cmd.Commands() {
						if err := c.RunE(cmd, args[1:]); err != nil {
							return err
						}
					}
				}
			}
			return nil
		},
	}

	c.AddCommand(NewGatherEtcdCommand())

	return c
}

const (
	ETCDCTL_CONTAINER = "etcdctl"
)

func runningEtcdPods(ctx context.Context, clientset *kubernetes.Clientset) (pods *v1.PodList, err error) {
	pods, err = clientset.CoreV1().Pods("openshift-etcd").List(ctx, metav1.ListOptions{LabelSelector: "app=etcd"})
	if err != nil {
		return nil, err
	}
	pods.Items = slices.DeleteFunc(pods.Items, func(p v1.Pod) bool {
		return p.Status.Phase != "Running"
	})

	return pods, nil
}

func etcdEndpoints(ctx context.Context, runningEtcdPod v1.Pod) (endpoints []string, err error) {
	var stdout, stderr strings.Builder
	c := exec.CommandContext(ctx, "oc", "exec", runningEtcdPod.Name, "-n", "openshift-etcd", "-c", ETCDCTL_CONTAINER, "--", "etcdctl", "member", "list")
	c.Stdout = &stdout
	c.Stderr = &stderr
	c.Env = os.Environ()
	c.Dir = BaseCollectionPath

	if err = c.Run(); err != nil {
		fmt.Println(stderr.String())
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

func ocp4etcdctl(ctx context.Context, outFile any, runningEtcdPod v1.Pod, etcdEndpoints []string, args ...string) {
	wg := ctx.Value("waitgroup").(*sync.WaitGroup)
	wg.Add(1)
	defer wg.Done()

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
	c.Dir = BaseCollectionPath

	if err := c.Run(); err != nil {
		fmt.Println(err, stderr.String())
	}
}

func NewGatherEtcdCommand() *cobra.Command {
	c := &cobra.Command{
		Use: "etcd",
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdLogPath := path.Join(BaseCollectionPath, "etcd_info")
			if err := os.MkdirAll(etcdLogPath, 0755); err != nil {
				return err
			}

			config, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
			if err != nil {
				return err
			}

			clientset, err := kubernetes.NewForConfig(config)
			if err != nil {
				return err
			}

			pods, err := runningEtcdPods(cmd.Context(), clientset)
			if err != nil {
				return err
			}

			endpoints, err := etcdEndpoints(cmd.Context(), pods.Items[0])
			if err != nil {
				return err
			}

			wg := sync.WaitGroup{}

			ctx, cancel := context.WithCancel(context.WithValue(cmd.Context(), "waitgroup", &wg))
			defer cancel()

			go func() {
				wg.Wait()
				cancel()
			}()

			go ocp4etcdctl(ctx, path.Join(etcdLogPath, "member_list.json"), pods.Items[0], endpoints, "member", "list", "-w", "json")
			go ocp4etcdctl(ctx, path.Join(etcdLogPath, "endpoint_status.json"), pods.Items[0], endpoints, "endpoint", "status", "-w", "json")
			go ocp4etcdctl(ctx, path.Join(etcdLogPath, "endpoint_health.json"), pods.Items[0], endpoints, "endpoint", "health", "-w", "json")
			go ocp4etcdctl(ctx, path.Join(etcdLogPath, "alarm_list.json"), pods.Items[0], endpoints, "alarm", "list", "-w", "json")
			go getObjectCounts(ctx, path.Join(etcdLogPath, "object_count.json"), pods.Items[0], endpoints)

			<-ctx.Done()

			return nil
		},
	}

	return c
}

func getObjectCounts(ctx context.Context, outFile string, pod v1.Pod, endpoints []string) {
	var out strings.Builder

	wg := ctx.Value("waitgroup").(*sync.WaitGroup)
	wg.Add(1)
	defer wg.Done()

	ocp4etcdctl(ctx, &out, pod, endpoints, "get", "/", "--prefix", "--keys-only")

	countMap := SortedMap[string, int]{}
	for key := range strings.SplitSeq(out.String(), "\n") {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		for i, keyType := range SplitSeq2(key, "/") {
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
		fmt.Println(err)
		return
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(countMap); err != nil {
		fmt.Println(err)
		return
	}
}

type SortedMap[K comparable, V cmp.Ordered] map[K]V

func (s SortedMap[K, V]) MarshalJSON() (b []byte, err error) {
	buf := bytes.Buffer{}
	enc := json.NewEncoder(&buf)

	type kv struct {
		key   K
		value V
	}

	buf.WriteRune('{')

	sorted := make([]kv, 0, len(s))
	for k, v := range s {
		sorted = append(sorted, kv{k, v})
	}

	slices.SortStableFunc(sorted, func(a, b kv) int {
		return cmp.Compare(b.value, a.value)
	})

	for i, kv := range sorted {
		if i > 0 {
			buf.WriteRune(',')
		}
		if err := enc.Encode(kv.key); err != nil {
			return nil, err
		}
		buf.WriteRune(':')
		if err := enc.Encode(kv.value); err != nil {
			return nil, err
		}
	}

	buf.WriteRune('}')

	return buf.Bytes(), nil
}

func SplitSeq2(s, sep string) iter.Seq2[int, string] {
	split := strings.SplitSeq(s, sep)
	i := 0
	return func(yield func(int, string) bool) {
		for s := range split {
			if !yield(i, s) {
				break
			}
			i++
		}
	}
}
