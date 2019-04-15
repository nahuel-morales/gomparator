package consumers

import (
	"github.com/emacampolo/gomparator/internal/platform/json"
	"github.com/emacampolo/gomparator/internal/stages"
	"github.com/google/go-cmp/cmp"
	"log"
	"os"
)

var logger = log.New(os.Stdout, "[gomparator] ", log.Ltime)

type Comparator struct {
	showDiff       bool
	statusCodeOnly bool
}

func NewComparator(showDiff bool, statusCodeOnly bool) *Comparator {
	return &Comparator{
		showDiff:       showDiff,
		statusCodeOnly: statusCodeOnly,
	}
}

func (c *Comparator) Consume(stream <-chan *stages.HostsResponse) {
	for val := range stream {
		if val.Left.Error != nil {
			logger.Printf("error %v", val.Left.Error)
			continue
		}

		if val.Right.Error != nil {
			logger.Printf("error %v", val.Right.Error)
			continue
		}

		if val.EqualStatusCode() && c.statusCodeOnly {
			logger.Printf("ok status code %d", val.Left.StatusCode)
		} else if val.EqualStatusCode() {
			if j1, j2 := unmarshal(val.Left), unmarshal(val.Right); j1 == nil || j2 == nil {
				continue
			} else if json.Equal(j1, j2) {
				logger.Println("ok")
			} else {
				if c.showDiff {
					logger.Printf("nok json diff url %s?%s \n%s", val.Left.URL.Path, val.Left.URL.RawQuery, cmp.Diff(j1, j2))
				} else {
					logger.Printf("nok json diff url %s?%s", val.Left.URL.Path, val.Left.URL.RawQuery)
				}
			}
		} else {
			logger.Printf("nok status code url %s?%s, %s: %d - %s: %d",
				val.Left.URL.Path, val.Left.URL.RawQuery, val.Left.URL.Host, val.Left.StatusCode, val.Right.URL.Host, val.Right.StatusCode)
		}
	}
}

func unmarshal(h *stages.Host) interface{} {
	j, err := json.Unmarshal(h.Body)
	if err != nil {
		logger.Printf("nok error unmarshaling from %s with error %v", h.URL, err)
		return nil
	}

	return j
}
