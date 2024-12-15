package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// Record represents a single data record in the pipeline
type Record struct {
	ID        string
	Data      map[string]interface{}
	Metadata  map[string]string
	Timestamp int64
}

// Timer represents a timing configuration
type Timer struct {
	Interval time.Duration
	Timeout  time.Duration
}

// Filter represents a data filter
type Filter interface {
	Apply(record Record) bool
}

// CronConfig represents a cron schedule configuration
type CronConfig struct {
	Schedule string // cron expression
	Enabled  bool
}

// PullConfig represents pull-based source configuration
type PullConfig struct {
	BatchSize  int
	MaxRetries int
	RetryDelay time.Duration
}

// PushConfig represents push-based sink configuration
type PushConfig struct {
	BatchSize      int
	FlushInterval  time.Duration
	RetryStrategy  string
	MaxRetries     int
	BackoffFactor  float64
}

// Source is an interface for data sources
type Source interface {
	Read(ctx context.Context) (<-chan Record, error)
	Close() error
}

// PullSource extends Source with pull-based functionality
type PullSource interface {
	Source
	Pull(ctx context.Context, config PullConfig) (<-chan Record, error)
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

// PushSink extends Sink with push-based functionality
type PushSink interface {
	Sink
	Push(ctx context.Context, records []Record, config PushConfig) error
}

// Pipeline represents a data processing pipeline
type Pipeline struct {
	name         string
	source       Source
	transformers []Transformer
	sink         Sink
	errorChan    chan error
	metrics      *Metrics
	timer        *Timer
	filters      []Filter
	cronConfig   *CronConfig
	pullConfig   *PullConfig
	pushConfig   *PushConfig
}

// Metrics holds pipeline metrics
type Metrics struct {
	Mu               sync.RWMutex
	RecordsProcessed int64
	Errors           int64
	StartTime        int64
	EndTime          int64
	LastPullTime     int64
	LastPushTime     int64
	FilteredRecords  int64
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

// WithTimer adds a timer configuration to the pipeline
func (p *Pipeline) WithTimer(timer *Timer) *Pipeline {
	p.timer = timer
	return p
}

// WithFilter adds a filter to the pipeline
func (p *Pipeline) WithFilter(filter Filter) *Pipeline {
	p.filters = append(p.filters, filter)
	return p
}

// WithCron adds cron configuration to the pipeline
func (p *Pipeline) WithCron(config *CronConfig) *Pipeline {
	p.cronConfig = config
	return p
}

// WithPullConfig adds pull configuration to the pipeline
func (p *Pipeline) WithPullConfig(config *PullConfig) *Pipeline {
	p.pullConfig = config
	return p
}

// WithPushConfig adds push configuration to the pipeline
func (p *Pipeline) WithPushConfig(config *PushConfig) *Pipeline {
	p.pushConfig = config
	return p
}

// Run executes the pipeline
func (p *Pipeline) Run(ctx context.Context) error {
	// Start reading from source
	records, err := p.executePull(ctx)
	if err != nil {
		return fmt.Errorf("failed to read from source: %w", err)
	}

	// Apply filters
	filteredRecords := make(chan Record)
	go func() {
		for record := range records {
			for _, filter := range p.filters {
				if !filter.Apply(record) {
					p.metrics.Mu.Lock()
					p.metrics.FilteredRecords++
					p.metrics.Mu.Unlock()
					continue
				}
			}
			filteredRecords <- record
		}
		close(filteredRecords)
	}()

	// Apply transformers in sequence
	var current <-chan Record = filteredRecords
	for _, t := range p.transformers {
		transformed, err := t.Transform(ctx, current)
		if err != nil {
			return fmt.Errorf("failed to apply transformer: %w", err)
		}
		current = transformed
	}

	// Write to sink
	if err := p.executePush(ctx, p.recordsToSlice(current)); err != nil {
		return fmt.Errorf("failed to write to sink: %w", err)
	}

	return nil
}

// RunWithTimer executes the pipeline with timing configuration
func (p *Pipeline) RunWithTimer(ctx context.Context) error {
	if p.timer == nil {
		return p.Run(ctx)
	}

	ticker := time.NewTicker(p.timer.Interval)
	defer ticker.Stop()

	timeoutCtx := ctx
	if p.timer.Timeout > 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, p.timer.Timeout)
		defer cancel()
	}

	for {
		select {
		case <-timeoutCtx.Done():
			return timeoutCtx.Err()
		case <-ticker.C:
			if err := p.Run(timeoutCtx); err != nil {
				p.metrics.Mu.Lock()
				p.metrics.Errors++
				p.metrics.Mu.Unlock()
				p.errorChan <- err
			}
		}
	}
}

// RunWithCron executes the pipeline with cron configuration
func (p *Pipeline) RunWithCron(ctx context.Context) error {
	if p.cronConfig == nil || !p.cronConfig.Enabled {
		return p.Run(ctx)
	}

	schedule, err := cron.ParseStandard(p.cronConfig.Schedule)
	if err != nil {
		return fmt.Errorf("invalid cron schedule: %w", err)
	}

	nextRun := schedule.Next(time.Now())
	timer := time.NewTimer(time.Until(nextRun))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if err := p.Run(ctx); err != nil {
				p.metrics.Mu.Lock()
				p.metrics.Errors++
				p.metrics.Mu.Unlock()
				p.errorChan <- err
			}
			nextRun = schedule.Next(time.Now())
			timer.Reset(time.Until(nextRun))
		}
	}
}

// executePull executes a pull operation if the source supports it
func (p *Pipeline) executePull(ctx context.Context) (<-chan Record, error) {
	if pullSource, ok := p.source.(PullSource); ok && p.pullConfig != nil {
		records, err := pullSource.Pull(ctx, *p.pullConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to pull from source: %w", err)
		}
		p.metrics.Mu.Lock()
		p.metrics.LastPullTime = time.Now().Unix()
		p.metrics.Mu.Unlock()
		return records, nil
	}
	return p.source.Read(ctx)
}

// executePush executes a push operation if the sink supports it
func (p *Pipeline) executePush(ctx context.Context, records []Record) error {
	if pushSink, ok := p.sink.(PushSink); ok && p.pushConfig != nil {
		err := pushSink.Push(ctx, records, *p.pushConfig)
		if err != nil {
			return fmt.Errorf("failed to push to sink: %w", err)
		}
		p.metrics.Mu.Lock()
		p.metrics.LastPushTime = time.Now().Unix()
		p.metrics.Mu.Unlock()
		return nil
	}
	
	recordChan := make(chan Record)
	go func() {
		defer close(recordChan)
		for _, record := range records {
			recordChan <- record
		}
	}()
	
	return p.sink.Write(ctx, recordChan)
}

// recordsToSlice converts a record channel to a slice
func (p *Pipeline) recordsToSlice(records <-chan Record) []Record {
	slice := make([]Record, 0)
	for record := range records {
		slice = append(slice, record)
	}
	return slice
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
