package main

import (
	"context"
	"fmt"
	"github.com/emacampolo/gomparator/internal/pipeline"
	"github.com/emacampolo/gomparator/internal/platform/http"
	"github.com/emacampolo/gomparator/internal/stages"
	"github.com/emacampolo/gomparator/internal/stages/consumers"
	"github.com/urfave/cli"
	"go.uber.org/ratelimit"
	"io"
	"log"
	"os"
	"runtime"
	"time"
)

func main() {
	app := cli.NewApp()
	app.Name = "Gomparator"
	app.Usage = "Compares API responses by status code and response body"
	app.HideVersion = true

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "path",
			Usage: "the named file for reading URL. It should contain one column only with a rel path. eg: /v1/cards?query=123",
		},
		cli.StringSliceFlag{
			Name:  "host",
			Usage: "exactly 2 hosts must be specified. eg: --host 'http://host1.com --host 'http://host2.com'",
		},
		cli.StringFlag{
			Name:  "headers",
			Usage: `headers separated by commas. eg: "X-Auth-Token: token, X-Public: false"`,
		},
		cli.IntFlag{
			Name:  "ratelimit, r",
			Value: 5,
			Usage: "operation rate limit per second",
		},
		cli.IntFlag{
			Name:  "workers, w",
			Value: 1,
			Usage: "number of workers running concurrently",
		},
		cli.BoolFlag{
			Name:  "show-diff",
			Usage: "whether or not it shows differences when comparison fails",
		},
		cli.BoolFlag{
			Name:  "status-code-only",
			Usage: "whether or not it only compares status code ignoring response body",
		},
		cli.DurationFlag{
			Name:  "timeout",
			Value: http.DefaultTimeout,
			Usage: "requests timeout",
		},
		cli.DurationFlag{
			Name:  "duration",
			Value: 0,
			Usage: "duration of the comparision [0 = forever]",
		},
		cli.IntFlag{
			Name:  "connections",
			Value: http.DefaultConnections,
			Usage: "max open idle connections per target host",
		},
	}

	app.Action = Action

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

type options struct {
	filePath       string
	hosts          []string
	headers        string
	timeout        time.Duration
	duration       time.Duration
	connections    int
	workers        int
	rateLimit      int
	showDiff       bool
	statusCodeOnly bool
}

func Action(cli *cli.Context) {
	opts := parseFlags(cli)
	headers := http.ParseHeaders(opts.headers)
	fetcher := http.New(
		http.Timeout(opts.timeout),
		http.Connections(opts.connections))

	var ctx context.Context
	var cancel context.CancelFunc

	t := opts.duration
	if t == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		// The request has a timeout, so create a context that is
		// canceled automatically when the timeout expires.
		ctx, cancel = context.WithTimeout(context.Background(), t)
	}
	defer cancel()

	file, err := os.Open(opts.filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer cl(file)

	reader := stages.NewReader(file, opts.hosts)
	producer := stages.NewProducer(opts.workers, headers,
		ratelimit.New(opts.rateLimit), fetcher)
	comparator := consumers.NewComparator(opts.showDiff, opts.statusCodeOnly)
	p := pipeline.New(reader, producer, ctx, comparator)
	start := time.Now()
	p.Run()
	PrintMemUsage()
	fmt.Println(time.Since(start))
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func parseFlags(cli *cli.Context) *options {
	opts := &options{}

	if opts.hosts = cli.StringSlice("host"); len(opts.hosts) != 2 {
		log.Fatal("invalid number of hosts provided")
	}

	opts.filePath = cli.String("path")
	opts.headers = cli.String("headers")
	opts.timeout = cli.Duration("timeout")
	opts.connections = cli.Int("connections")
	opts.duration = cli.Duration("duration")
	opts.workers = cli.Int("workers")
	opts.rateLimit = cli.Int("ratelimit")
	opts.showDiff = cli.Bool("show-diff")
	opts.statusCodeOnly = cli.Bool("status-code-only")
	return opts
}

func cl(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Fatal(err)
	}
}
