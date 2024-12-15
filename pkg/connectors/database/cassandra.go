package database

import (
	"github.com/gocql/gocql"
	"github.com/ivikasavnish/datapipe/pkg/connectors"
)

type CassandraConnector struct {
	connectors.BaseConnector
	Config  CassandraConfig
	session *gocql.Session
}

type CassandraConfig struct {
	Hosts    []string
	Keyspace string
	Username string
	Password string
}

func NewCassandraConnector(config CassandraConfig) *CassandraConnector {
	return &CassandraConnector{
		BaseConnector: connectors.BaseConnector{
			Name:        "Cassandra",
			Description: "Apache Cassandra connector",
			Version:     "1.0.0",
			Type:        "database",
		},
		Config: config,
	}
}

func (c *CassandraConnector) Connect() error {
	cluster := gocql.NewCluster(c.Config.Hosts...)
	cluster.Keyspace = c.Config.Keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: c.Config.Username,
		Password: c.Config.Password,
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}

	c.session = session
	return nil
}

func (c *CassandraConnector) Disconnect() error {
	if c.session != nil {
		c.session.Close()
	}
	return nil
}

func (c *CassandraConnector) Read() (interface{}, error) {
	// Implement read logic
	return nil, nil
}

func (c *CassandraConnector) Write(data interface{}) error {
	// Implement write logic
	return nil
}

func (c *CassandraConnector) GetConfig() interface{} {
	return c.Config
}
