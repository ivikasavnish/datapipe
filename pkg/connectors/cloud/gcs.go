package cloud

import (
	"context"
	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

type GCSConnector struct {
	BaseConnector
	Config  GCSConfig
	client  *storage.Client
	ctx     context.Context
}

type GCSConfig struct {
	ProjectID      string
	Bucket         string
	CredentialsFile string
}

func NewGCSConnector(config GCSConfig) *GCSConnector {
	return &GCSConnector{
		BaseConnector: BaseConnector{
			Name:        "Google Cloud Storage",
			Description: "Google Cloud Storage connector",
			Version:     "1.0.0",
			Type:        "cloud_storage",
		},
		Config: config,
		ctx:    context.Background(),
	}
}

func (g *GCSConnector) Connect() error {
	var err error
	g.client, err = storage.NewClient(g.ctx, option.WithCredentialsFile(g.Config.CredentialsFile))
	return err
}

func (g *GCSConnector) Disconnect() error {
	if g.client != nil {
		return g.client.Close()
	}
	return nil
}

func (g *GCSConnector) Read() (interface{}, error) {
	// Implement read logic
	return nil, nil
}

func (g *GCSConnector) Write(data interface{}) error {
	// Implement write logic
	return nil
}

func (g *GCSConnector) GetConfig() interface{} {
	return g.Config
}

// Additional GCS-specific methods
func (g *GCSConnector) ListBuckets() ([]*storage.BucketHandle, error) {
	it := g.client.Buckets(g.ctx, g.Config.ProjectID)
	var buckets []*storage.BucketHandle
	for {
		bucketAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, g.client.Bucket(bucketAttrs.Name))
	}
	return buckets, nil
}

func (g *GCSConnector) ListObjects(prefix string) ([]*storage.ObjectHandle, error) {
	bucket := g.client.Bucket(g.Config.Bucket)
	var objects []*storage.ObjectHandle
	it := bucket.Objects(g.ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		objects = append(objects, bucket.Object(attrs.Name))
	}
	return objects, nil
}
