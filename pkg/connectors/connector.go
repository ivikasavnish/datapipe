package connectors

// Connector interface defines the basic operations that all connectors must implement
type Connector interface {
	// Connect establishes a connection to the data source
	Connect() error
	// Disconnect closes the connection to the data source
	Disconnect() error
	// Read reads data from the source
	Read() (interface{}, error)
	// Write writes data to the destination
	Write(data interface{}) error
	// GetConfig returns the connector configuration
	GetConfig() interface{}
}

// BaseConnector provides common functionality for all connectors
type BaseConnector struct {
	Name        string
	Description string
	Version     string
	Type        string
}

// GetName returns the connector name
func (b *BaseConnector) GetName() string {
	return b.Name
}

// GetDescription returns the connector description
func (b *BaseConnector) GetDescription() string {
	return b.Description
}

// GetVersion returns the connector version
func (b *BaseConnector) GetVersion() string {
	return b.Version
}

// GetType returns the connector type
func (b *BaseConnector) GetType() string {
	return b.Type
}
