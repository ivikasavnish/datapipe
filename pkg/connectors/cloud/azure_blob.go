package cloud

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/ivikasavnish/datapipe/pkg/connectors"
)

type AzureBlobConnector struct {
	connectors.BaseConnector
	Config        AzureBlobConfig
	credential    *azblob.SharedKeyCredential
	serviceClient *azblob.Client
	ctx           context.Context
}

type AzureBlobConfig struct {
	AccountName    string
	AccountKey     string
	ContainerName  string
	EndpointSuffix string
}

func NewAzureBlobConnector(config AzureBlobConfig) *AzureBlobConnector {
	return &AzureBlobConnector{
		BaseConnector: connectors.BaseConnector{
			Name:        "Azure Blob Storage",
			Description: "Azure Blob Storage connector",
			Version:     "1.0.0",
			Type:        "cloud_storage",
		},
		Config: config,
		ctx:    context.Background(),
	}
}

func (a *AzureBlobConnector) Connect() error {
	credential, err := azblob.NewSharedKeyCredential(a.Config.AccountName, a.Config.AccountKey)
	if err != nil {
		return err
	}

	serviceClient, err := azblob.NewClientWithSharedKeyCredential(a.Config.AccountName, credential, &azblob.ClientOptions{})
	if err != nil {
		return err
	}

	a.serviceClient = serviceClient
	a.credential = credential
	return nil
}

func (a *AzureBlobConnector) Disconnect() error {
	// Azure SDK handles connection management
	return nil
}

func (a *AzureBlobConnector) Read() (interface{}, error) {
	// Implement read logic
	return nil, nil
}

func (a *AzureBlobConnector) Write(data interface{}) error {
	// Implement write logic
	return nil
}

func (a *AzureBlobConnector) GetConfig() interface{} {
	return a.Config
}

// Additional Azure Blob-specific methods
func (a *AzureBlobConnector) ListContainers() ([]*service.ContainerItem, error) {
	var containers []*service.ContainerItem

	pager := a.serviceClient.NewListContainersPager(nil)
	for pager.More() {
		resp, err := pager.NextPage(a.ctx)
		if err != nil {
			return nil, err
		}
		containers = append(containers, resp.ContainerItems...)
	}

	return containers, nil
}

func (a *AzureBlobConnector) ListBlobs(prefix string) ([]*container.BlobItem, error) {
	containerClient := a.serviceClient.ServiceClient().NewContainerClient(a.Config.ContainerName)

	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix})
	var blobs []*container.BlobItem
	for pager.More() {
		resp, err := pager.NextPage(a.ctx)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, resp.Segment.BlobItems...)
	}

	return blobs, nil
}
