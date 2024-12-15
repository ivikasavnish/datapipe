# DataPipe

A flexible and extensible data pipeline framework in Go, designed for building robust ETL (Extract, Transform, Load) pipelines.

## Features

- Modular architecture with interfaces for Sources, Transformers, and Sinks
- Built-in support for:
  - Sources: Kafka
  - Transformers: Filter
  - Sinks: Elasticsearch
- Pipeline metrics and monitoring
- Graceful shutdown handling
- Batch processing support
- Error handling and recovery
- Context-based cancellation

## Installation

```bash
go get github.com/ivikasavnish/datapipe
```

## Quick Start

Here's a simple example that reads data from Kafka, filters it, and writes to Elasticsearch:

```go
package main

import (
    "github.com/ivikasavnish/datapipe/pkg/pipeline"
    "github.com/ivikasavnish/datapipe/pkg/sources"
    "github.com/ivikasavnish/datapipe/pkg/transformers"
    "github.com/ivikasavnish/datapipe/pkg/sinks"
)

func main() {
    // Create source
    source, _ := sources.NewKafkaSource(
        []string{"localhost:9092"},
        "input-topic",
        "my-consumer-group",
    )

    // Create transformer
    filter := transformers.NewFilter(func(r pipeline.Record) bool {
        return len(r.Data) > 0
    })

    // Create sink
    sink, _ := sinks.NewElasticsearchSink(
        []string{"http://localhost:9200"},
        "output-index",
        1000,
    )

    // Create pipeline
    p := pipeline.NewPipeline(
        "my-pipeline",
        source,
        sink,
        filter,
    )

    // Run pipeline
    p.Run(context.Background())
}
```

## Architecture

The framework is built around three main interfaces:

1. **Source**: Reads data from input sources
```go
type Source interface {
    Read(ctx context.Context) (<-chan Record, error)
    Close() error
}
```

2. **Transformer**: Processes and transforms data
```go
type Transformer interface {
    Transform(ctx context.Context, in <-chan Record) (<-chan Record, error)
}
```

3. **Sink**: Writes data to output destinations
```go
type Sink interface {
    Write(ctx context.Context, in <-chan Record) error
    Close() error
}
```

## Extending the Framework

You can easily add new sources, transformers, and sinks by implementing the respective interfaces.

### Adding a New Source

```go
type MySource struct {
    // your source configuration
}

func (s *MySource) Read(ctx context.Context) (<-chan pipeline.Record, error) {
    // implement reading logic
}

func (s *MySource) Close() error {
    // implement cleanup logic
}
```

### Adding a New Transformer

```go
type MyTransformer struct {
    // your transformer configuration
}

func (t *MyTransformer) Transform(ctx context.Context, in <-chan pipeline.Record) (<-chan pipeline.Record, error) {
    // implement transformation logic
}
```

### Adding a New Sink

```go
type MySink struct {
    // your sink configuration
}

func (s *MySink) Write(ctx context.Context, in <-chan pipeline.Record) error {
    // implement writing logic
}

func (s *MySink) Close() error {
    // implement cleanup logic
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file
