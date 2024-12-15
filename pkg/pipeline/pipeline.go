package pipeline

import (
	"context"
	"fmt"
	"sync"
)

// Record represents a single data record in the pipeline
type Record struct {
	ID        string
	Data      map[string]interface{}
	Metadata  map[string]string
	Timestamp int64
}

// Source is an interface for data sources
type Source interface {
	Read(ctx context.Context) (<-chan Record, error)
	Close() error
}

// Transformer is an interface for data transformers
type Transformer interface {
	Transform(ctx context.Context, in <-chan Record) (<-chan Record, error)
}

// Sink is an interface for data sinks
type Sink interface {
	Write(ctx context.Context, in <-chan Record) error
	Close() error
}

// Pipeline represents a data processing pipeline
type Pipeline struct {
	name         string
	source       Source
	transformers []Transformer
	sink         Sink
	errorChan    chan error
	metrics      *Metrics
}

// Metrics holds pipeline metrics
type Metrics struct {
	Mu               sync.RWMutex
	RecordsProcessed int64
	Errors           int64
	StartTime        int64
	EndTime          int64
}

// NewPipeline creates a new data pipeline
func NewPipeline(name string, source Source, sink Sink, transformers ...Transformer) *Pipeline {
	return &Pipeline{
		name:         name,
		source:       source,
		transformers: transformers,
		sink:         sink,
		errorChan:    make(chan error, 100),
		metrics:      &Metrics{},
	}
}

// Run executes the pipeline
func (p *Pipeline) Run(ctx context.Context) error {
	// Start reading from source
	records, err := p.source.Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to read from source: %w", err)
	}

	// Apply transformers in sequence
	current := records
	for _, t := range p.transformers {
		transformed, err := t.Transform(ctx, current)
		if err != nil {
			return fmt.Errorf("failed to apply transformer: %w", err)
		}
		current = transformed
	}

	// Write to sink
	if err := p.sink.Write(ctx, current); err != nil {
		return fmt.Errorf("failed to write to sink: %w", err)
	}

	return nil
}

// Stop gracefully stops the pipeline
func (p *Pipeline) Stop() error {
	if err := p.source.Close(); err != nil {
		return fmt.Errorf("failed to close source: %w", err)
	}
	if err := p.sink.Close(); err != nil {
		return fmt.Errorf("failed to close sink: %w", err)
	}
	return nil
}

// GetMetrics returns pipeline metrics
func (p *Pipeline) GetMetrics() Metrics {
	p.metrics.Mu.RLock()
	defer p.metrics.Mu.RUnlock()
	return *p.metrics
}
