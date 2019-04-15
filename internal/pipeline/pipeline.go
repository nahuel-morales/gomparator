package pipeline

import (
	"context"
	"github.com/emacampolo/gomparator/internal/stages"
	"sync"
)

type Reader interface {
	Read() <-chan *stages.URLPair
}

type Producer interface {
	Produce(in <-chan *stages.URLPair) <-chan *stages.HostsResponse
}

type Consumer interface {
	Consume(in <-chan *stages.HostsResponse)
}

func New(reader Reader, producer Producer, ctx context.Context, consumers ...Consumer) *Pipeline {
	return &Pipeline{
		reader:    reader,
		producer:  producer,
		consumers: consumers,
		ctx:       ctx,
	}
}

type Pipeline struct {
	reader    Reader
	producer  Producer
	consumers []Consumer
	ctx       context.Context
}

func (p *Pipeline) Run() {
	var wg sync.WaitGroup
	wg.Add(p.totalConsumers())

	streams := p.tee()
	for i := range streams {
		go func(i int) {
			defer wg.Done()
			p.consumers[i].Consume(streams[i])
		}(i)
	}

	wg.Wait()
}

func (p *Pipeline) totalConsumers() int {
	return len(p.consumers)
}

func (p *Pipeline) tee() []chan *stages.HostsResponse {
	// Create channels for each consumer that will all receive the same values mimicking unix tee
	teeStreams := make([]chan *stages.HostsResponse, p.totalConsumers())
	for i := range teeStreams {
		teeStreams[i] = make(chan *stages.HostsResponse, 1)
	}

	readStream := p.reader.Read()
	producerStream := p.producer.Produce(readStream)

	closeStreams := func(streams []chan *stages.HostsResponse) {
		for _, s := range streams {
			close(s)
		}
	}

	orDone := func(ctx context.Context, c <-chan *stages.HostsResponse) <-chan *stages.HostsResponse {
		valStream := make(chan *stages.HostsResponse)
		go func() {
			defer close(valStream)
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-c:
					if !ok {
						return
					}
					select {
					case valStream <- v:
					case <-ctx.Done():
					}
				}
			}
		}()
		return valStream
	}

	go func() {
		defer closeStreams(teeStreams)
		for val := range orDone(p.ctx, producerStream) {
			for _, s := range teeStreams {
				s <- val
			}
		}
	}()

	return teeStreams
}
