package transformers

import (
	"context"
	"github.com/ivikasavnish/datapipe/pkg/pipeline"
)

// FilterFunc is a function type that determines whether a record should be included
type FilterFunc func(pipeline.Record) bool

// Filter implements a filtering transformer
type Filter struct {
	predicate FilterFunc
}

// NewFilter creates a new Filter transformer
func NewFilter(predicate FilterFunc) *Filter {
	return &Filter{
		predicate: predicate,
	}
}

// Transform implements pipeline.Transformer
func (f *Filter) Transform(ctx context.Context, in <-chan pipeline.Record) (<-chan pipeline.Record, error) {
	out := make(chan pipeline.Record)

	go func() {
		defer close(out)

		for record := range in {
			select {
			case <-ctx.Done():
				return
			default:
				if f.predicate(record) {
					out <- record
				}
			}
		}
	}()

	return out, nil
}
