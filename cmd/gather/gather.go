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

	"github.com/openshift/must-gather/pkg/flags"
	"github.com/openshift/must-gather/pkg/gather/etcd"
	"github.com/openshift/must-gather/pkg/gather/resources"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/wait"
)

type GatherFunc func(ctx context.Context, logger *slog.Logger)

var (
	DefaultList = []string{
		etcd.Name,
		resources.Name,
	}
	ExtraList = []string{}
	AllList   = slices.Concat(DefaultList, ExtraList)
	Mapping   = map[string]GatherFunc{
		etcd.Name:      etcd.Gather,
		resources.Name: resources.Gather,
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
			g := wait.Group{}

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
				g.Start(func() {
					gather(cmd.Context(), slog.With("command", l))
				})
			}

			g.Wait()

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
