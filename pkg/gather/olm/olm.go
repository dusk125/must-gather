package olm

import (
	"context"
	"log/slog"
	"os"
	"os/exec"

	"github.com/openshift/must-gather/pkg/flags"
)

var (
	Name = "olm"
)

func Gather(ctx context.Context, logger *slog.Logger) {
	args := []string{
		"adm", "inspect",
	}

	if a := flags.LogCollectionArgs(); a != "" {
		args = append(args, a)
	}

	args = append(args,
		"--dest-dir=.",
		"-A", "olm",
	)

	cmd := exec.CommandContext(ctx, "oc", args...)
	cmd.Env = os.Environ()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = flags.BaseCollectionPath

	_ = cmd.Run()
}
