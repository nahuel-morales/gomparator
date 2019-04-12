package comparator

import (
	"context"
	"github.com/emacampolo/gomparator/internal/platform/http"
	"go.uber.org/ratelimit"
	"net/url"
	"sync"
)

type Fetcher interface {
	Fetch(url string, headers map[string]string) (*http.Response, error)
}

type HostPairResponse struct {
	Left, Right *Host
}

type Host struct {
	StatusCode int
	Body       []byte
	URL        *url.URL
	Error      error
}

func NewProducer(
	ctx context.Context, urls <-chan *URLPairResponse, concurrency int, headers map[string]string,
	limiter ratelimit.Limiter, fetcher Fetcher) <-chan *HostPairResponse {

	ch := make(chan *HostPairResponse)
	var wg sync.WaitGroup
	go func() {
		defer close(ch)

		for w := 0; w < concurrency; w++ {
			wg.Add(1)

			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case u, ok := <-urls:
						if !ok {
							return
						}
						limiter.Take()
						ch <- produce(u, fetcher, headers)
					}
				}
			}()
		}

		wg.Wait()
	}()

	return ch
}

func produce(u *URLPairResponse, fetcher Fetcher, headers map[string]string) *HostPairResponse {
	work := func(u *URL, f Fetcher, h map[string]string) <-chan *Host {
		ch := make(chan *Host, 1)
		go func() {
			defer close(ch)
			ch <- fetch(u, f, h)
		}()
		return ch
	}

	response := &HostPairResponse{}

	leftCh := work(u.Left, fetcher, headers)
	rightCh := work(u.Right, fetcher, headers)

	response.Left = <-leftCh
	response.Right = <-rightCh

	return response
}

func fetch(u *URL, fetcher Fetcher, headers map[string]string) *Host {
	host := &Host{}

	if u.Error != nil {
		host.Error = u.Error
	} else if response, err := fetcher.Fetch(u.URL.String(), headers); err == nil {
		host.URL = u.URL
		host.Body = response.Body
		host.StatusCode = response.StatusCode
	} else {
		host.Error = err
	}
	return host
}
