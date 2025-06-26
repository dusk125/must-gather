package gather

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/openshift/must-gather/internal"
	"github.com/openshift/must-gather/pkg/flags"
	"github.com/openshift/must-gather/pkg/gather/etcd"
	"github.com/openshift/must-gather/pkg/gather/insights"
	"github.com/openshift/must-gather/pkg/gather/monitoring"
	"github.com/openshift/must-gather/pkg/gather/olm"
	priorityandfairness "github.com/openshift/must-gather/pkg/gather/priority_and_fairness"
	"github.com/openshift/must-gather/pkg/gather/resources"
	"github.com/spf13/cobra"
)

type GatherFunc func(ctx context.Context, logger *slog.Logger)

var (
	// TODO: convert this to a list that the individual pieces register to
	DefaultList = []string{
		etcd.Name,
		resources.Name,
		insights.Name,
		monitoring.Name,
		olm.Name,
		priorityandfairness.Name,
	}
	ExtraList = []string{}
	AllList   = slices.Concat(DefaultList, ExtraList)
	Mapping   = map[string]GatherFunc{
		etcd.Name:                etcd.Gather,
		resources.Name:           resources.Gather,
		insights.Name:            insights.Gather,
		monitoring.Name:          monitoring.Gather,
		olm.Name:                 olm.Gather,
		priorityandfairness.Name: priorityandfairness.Gather,
	}
)

const (
	OFDirectory string = "dir"
	OFTarGz     string = "targz"
)

func NewGatherCommand() *cobra.Command {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	list := DefaultList
	outFormat := OFDirectory

	c := &cobra.Command{
		Use: "gather",
		RunE: func(cmd *cobra.Command, args []string) error {
			if v, has := os.LookupEnv("MUST_GATHER_SINCE"); has {
				d, err := time.ParseDuration(v)
				if err != nil {
					return err
				}

				flags.SinceTime = time.Now().Add(-d)
			}

			if v, has := os.LookupEnv("MUST_GATHER_SINCE_TIME"); has {
				var err error
				flags.SinceTime, err = time.Parse(time.RFC3339, v)
				if err != nil {
					return err
				}
			}

			for _, l := range list {
				gather, has := Mapping[l]
				if !has {
					return fmt.Errorf("invalid gather command: %v", l)
				}
				internal.Group.Start(func() {
					gather(cmd.Context(), slog.With("command", l))
				})
			}

			internal.Group.Wait()

			if outFormat == OFTarGz {
				f, err := os.Create("gather.tar.gz")
				if err != nil {
					return err
				}
				defer f.Close()

				gzw := gzip.NewWriter(f)
				defer gzw.Close()
				tarw := tar.NewWriter(gzw)
				defer tarw.Close()

				err = filepath.Walk(flags.BaseCollectionPath, func(path string, info fs.FileInfo, err error) error {
					if err != nil {
						return err
					}

					hdr, err := tar.FileInfoHeader(info, "")
					if err != nil {
						return err
					}

					relPath, err := filepath.Rel(flags.BaseCollectionPath, path)
					if err != nil {
						return err
					}
					hdr.Name = relPath
					if err := tarw.WriteHeader(hdr); err != nil {
						return err
					}

					if info.Mode().IsRegular() {
						fh, err := os.Open(path)
						if err != nil {
							return err
						}
						defer fh.Close()
						if _, err := io.Copy(tarw, fh); err != nil {
							return err
						}
					}

					return nil
				})
				if err != nil {
					return err
				}
			}

			return nil
		},
	}

	c.Flags().StringVar(&flags.BaseCollectionPath, "base-collection-path", "/must-gather", "")
	c.Flags().StringSliceVar(&list, "commands", DefaultList, "")
	c.Flags().StringVar(&outFormat, "format", outFormat, "")

	return c
}
