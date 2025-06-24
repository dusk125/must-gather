package util

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"iter"
	"os"
	"slices"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

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

func NewClientSet() (clientset *kubernetes.Clientset, err error) {
	config, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		return
	}

	return kubernetes.NewForConfig(config)
}

func RunningPods(ctx context.Context, clientset *kubernetes.Clientset, namespace string, labelSelector string) (pods *v1.PodList, err error) {
	pods, err = clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "status.phase==Running"})
	if err != nil {
		return nil, err
	}

	return pods, nil
}
