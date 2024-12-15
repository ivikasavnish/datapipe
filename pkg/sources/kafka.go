package sources

import (
	"context"
	"encoding/json"
	"github.com/ivikasavnish/datapipe/pkg/pipeline"
	"github.com/segmentio/kafka-go"
)

// KafkaSource implements pipeline.Source for Kafka
type KafkaSource struct {
	reader *kafka.Reader
}

// NewKafkaSource creates a new Kafka source
func NewKafkaSource(brokers []string, topic string, groupID string) (*KafkaSource, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	return &KafkaSource{
		reader: reader,
	}, nil
}

// Read implements pipeline.Source
func (s *KafkaSource) Read(ctx context.Context) (<-chan pipeline.Record, error) {
	out := make(chan pipeline.Record)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := s.reader.ReadMessage(ctx)
				if err != nil {
					continue
				}

				var data map[string]interface{}
				if err := json.Unmarshal(msg.Value, &data); err != nil {
					continue
				}

				record := pipeline.Record{
					ID:   string(msg.Key),
					Data: data,
					Metadata: map[string]string{
						"topic":     msg.Topic,
						"partition": string(msg.Partition),
						"offset":    string(msg.Offset),
					},
					Timestamp: msg.Time.Unix(),
				}

				select {
				case <-ctx.Done():
					return
				case out <- record:
				}
			}
		}
	}()

	return out, nil
}

// Close implements pipeline.Source
func (s *KafkaSource) Close() error {
	return s.reader.Close()
}
