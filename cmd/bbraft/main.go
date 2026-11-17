// Command bbraft is the bamboo-raft CLI.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/nathabonfim59/bamboo-raft/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	app := cli.NewApp(cli.Options{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	})
	code := app.Run(ctx)
	stop()
	os.Exit(code)
}
