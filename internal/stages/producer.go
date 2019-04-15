package stages

import (
	"github.com/emacampolo/gomparator/internal/platform/http"
	"go.uber.org/ratelimit"
	"net/url"
	"sync"
)

type Fetcher interface {
	Fetch(url string, headers map[string]string) (*http.Response, error)
}

type HostsPair struct {
	Left, Right *Host
}

func (h *HostsPair) EqualStatusCode() bool {
	return h.Left.StatusCode == h.Right.StatusCode
}

type Host struct {
	StatusCode int
	Body       []byte
	URL        *url.URL
	Error      error
}

type Producer struct {
	concurrency int
	headers     map[string]string
	limiter     ratelimit.Limiter
	fetcher     Fetcher
}

func (p *Producer) Produce(in <-chan *URLPair) <-chan *HostsPair {
	stream := make(chan *HostsPair)
	go func() {
		defer close(stream)

		var wg sync.WaitGroup
		wg.Add(p.concurrency)

		for w := 0; w < p.concurrency; w++ {
			go func() {
				defer wg.Done()
				for val := range in {
					p.limiter.Take()
					stream <- p.produce(val)
				}
			}()
		}
		wg.Wait()
	}()

	return stream
}

func NewProducer(concurrency int, headers map[string]string, limiter ratelimit.Limiter, fetcher Fetcher) *Producer {
	return &Producer{
		concurrency: concurrency,
		headers:     headers,
		limiter:     limiter,
		fetcher:     fetcher,
	}
}

func (p *Producer) produce(u *URLPair) *HostsPair {
	work := func(u *URL) <-chan *Host {
		ch := make(chan *Host, 1)
		go func() {
			defer close(ch)
			ch <- p.fetch(u)
		}()
		return ch
	}

	response := &HostsPair{}

	leftCh := work(u.Left)
	rightCh := work(u.Right)

	response.Left = <-leftCh
	response.Right = <-rightCh

	return response
}

func (p *Producer) fetch(u *URL) *Host {
	host := &Host{}

	if u.Error != nil {
		host.Error = u.Error
	} else if response, err := p.fetcher.Fetch(u.URL.String(), p.headers); err == nil {
		host.URL = u.URL
		host.Body = response.Body
		host.StatusCode = response.StatusCode
	} else {
		host.Error = err
	}
	return host
}
