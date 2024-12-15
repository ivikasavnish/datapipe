package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ivikasavnish/datapipe/pkg/pipeline"
	"github.com/ivikasavnish/datapipe/pkg/sinks"
	"github.com/ivikasavnish/datapipe/pkg/sources"
	"github.com/ivikasavnish/datapipe/pkg/transformers"
)

func main() {
	// Create a source
	source, err := sources.NewKafkaSource(
		[]string{"localhost:9092"},
		"input-topic",
		"my-consumer-group",
	)
	if err != nil {
		log.Fatalf("Failed to create Kafka source: %v", err)
	}

	// Create transformers
	// Example: Filter out records with empty data
	filter := transformers.NewFilter(func(r pipeline.Record) bool {
		return len(r.Data) > 0
	})

	// Create a sink
	sink, err := sinks.NewElasticsearchSink(
		[]string{"http://localhost:9200"},
		"output-index",
		1000,
	)
	if err != nil {
		log.Fatalf("Failed to create Elasticsearch sink: %v", err)
	}

	// Create and configure the pipeline
	p := pipeline.NewPipeline(
		"kafka-to-elasticsearch",
		source,
		sink,
		filter,
	)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal. Stopping pipeline...")
		cancel()
	}()

	// Run the pipeline
	log.Println("Starting pipeline...")
	if err := p.Run(ctx); err != nil {
		log.Printf("Pipeline error: %v", err)
	}

	// Stop the pipeline
	if err := p.Stop(); err != nil {
		log.Printf("Error stopping pipeline: %v", err)
	}

	// Print metrics
	metrics := p.GetMetrics()
	log.Printf("Pipeline processed %d records with %d errors", 
		metrics.recordsProcessed, metrics.errors)
}
