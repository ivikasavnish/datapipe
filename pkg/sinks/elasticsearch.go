package sinks

import (
	"context"
	"encoding/json"
	"fmt"
	"bytes"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/ivikasavnish/datapipe/pkg/pipeline"
)

// ElasticsearchSink implements pipeline.Sink for Elasticsearch
type ElasticsearchSink struct {
	client    *elasticsearch.Client
	index     string
	batchSize int
}

// NewElasticsearchSink creates a new Elasticsearch sink
func NewElasticsearchSink(addresses []string, index string, batchSize int) (*ElasticsearchSink, error) {
	cfg := elasticsearch.Config{
		Addresses: addresses,
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	return &ElasticsearchSink{
		client:    client,
		index:     index,
		batchSize: batchSize,
	}, nil
}

// Write implements pipeline.Sink
func (s *ElasticsearchSink) Write(ctx context.Context, in <-chan pipeline.Record) error {
	batch := make([]pipeline.Record, 0, s.batchSize)

	for record := range in {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			batch = append(batch, record)

			if len(batch) >= s.batchSize {
				if err := s.writeBatch(ctx, batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}

	// Write remaining records
	if len(batch) > 0 {
		return s.writeBatch(ctx, batch)
	}

	return nil
}

func (s *ElasticsearchSink) writeBatch(ctx context.Context, batch []pipeline.Record) error {
	var buf []byte
	for _, record := range batch {
		// Create the metadata for the Elasticsearch operation
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": s.index,
				"_id":    record.ID,
			},
		}

		// Add metadata line
		metaLine, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}
		buf = append(buf, metaLine...)
		buf = append(buf, '\n')

		// Add data line
		dataLine, err := json.Marshal(record.Data)
		if err != nil {
			return fmt.Errorf("failed to marshal data: %w", err)
		}
		buf = append(buf, dataLine...)
		buf = append(buf, '\n')
	}

	// Send the batch to Elasticsearch
	res, err := s.client.Bulk(bytes.NewReader(buf), s.client.Bulk.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to perform bulk operation: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk operation failed: %s", res.String())
	}

	return nil
}

// Close implements pipeline.Sink
func (s *ElasticsearchSink) Close() error {
	return nil
}
