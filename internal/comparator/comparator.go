package comparator

import (
	"fmt"
	"github.com/emacampolo/gomparator/internal/platform/http"
	"github.com/emacampolo/gomparator/internal/platform/json"
	"github.com/google/go-cmp/cmp"
	"go.uber.org/ratelimit"
	"log"
	"os"
	"sync"
)

var logger = log.New(os.Stdout, "[gomparator] ", 2)

type Fetcher interface {
	Fetch(host string, relPath string, headers map[string]string) (*http.Response, error)
}

func New(fetcher Fetcher, rateLimiter ratelimit.Limiter) Comparator {
	return Comparator{
		fetcher,
		rateLimiter,
	}
}

type Comparator struct {
	Fetcher
	ratelimit.Limiter
}

func (comp Comparator) Compare(hosts []string, headers map[string]string, jobs <-chan string, wg *sync.WaitGroup,
	showDiff bool, statusCodeOnly bool) {
	defer wg.Done()

	comp.Limiter.Take()
	for relUrl := range jobs {
		first, err := comp.Fetch(hosts[0], relUrl, headers)
		if err != nil {
			logger.Fatalf("host: %s, path: %s, error %v", hosts[0], relUrl, err)
		}

		second, err := comp.Fetch(hosts[1], relUrl, headers)
		if err != nil {
			logger.Fatalf("host: %s, path: %s, error %v", hosts[1], relUrl, err)
		}

		if first.StatusCode == second.StatusCode && statusCodeOnly {
			logger.Println(fmt.Sprintf("ok status code %d url %s", first.StatusCode, relUrl))
		} else if first.StatusCode == second.StatusCode {
			if j1, j2 := unmarshal(first), unmarshal(second); j1 == nil || j2 == nil {
				continue
			} else if json.Equal(j1, j2) {
				logger.Println("ok")
			} else {
				if showDiff {
					logger.Println(fmt.Sprintf("nok json diff url %s", relUrl), cmp.Diff(j1, j2))
				} else {
					logger.Println(fmt.Sprintf("nok json diff url %s", relUrl))
				}
			}
		} else {
			logger.Println(fmt.Sprintf("nok status code url %s, %s: %d - %s: %d", relUrl, first.URL.Host, first.StatusCode, second.URL.Host, second.StatusCode))
		}
	}
}

func unmarshal(r *http.Response) interface{} {
	j, err := json.Unmarshal(r.JSON)
	if err != nil {
		logger.Printf("nok error unmarshaling from %s with error %v", r.URL, err)
		return nil
	}

	return j
}