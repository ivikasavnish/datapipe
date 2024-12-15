package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
)

type GraphQLConnector struct {
	BaseConnector
	Config  GraphQLConfig
	client  *http.Client
	headers map[string]string
}

type GraphQLConfig struct {
	Endpoint    string
	Headers     map[string]string
	AuthType    string // "bearer", "api_key"
	BearerToken string
	APIKey      string
	Timeout     int
}

type GraphQLRequest struct {
	Query         string                 `json:"query"`
	Variables     map[string]interface{} `json:"variables,omitempty"`
	OperationName string                 `json:"operationName,omitempty"`
}

type GraphQLResponse struct {
	Data   interface{}    `json:"data"`
	Errors []GraphQLError `json:"errors,omitempty"`
}

type GraphQLError struct {
	Message    string                 `json:"message"`
	Locations  []GraphQLErrorLocation `json:"locations,omitempty"`
	Path       []interface{}          `json:"path,omitempty"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
}

type GraphQLErrorLocation struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

func NewGraphQLConnector(config GraphQLConfig) *GraphQLConnector {
	return &GraphQLConnector{
		BaseConnector: BaseConnector{
			Name:        "GraphQL",
			Description: "GraphQL API connector",
			Version:     "1.0.0",
			Type:        "api",
		},
		Config:  config,
		headers: make(map[string]string),
	}
}

func (g *GraphQLConnector) Connect() error {
	g.client = &http.Client{
		Timeout: time.Duration(g.Config.Timeout) * time.Second,
	}

	// Set default headers
	g.headers["Content-Type"] = "application/json"

	// Add custom headers
	for k, v := range g.Config.Headers {
		g.headers[k] = v
	}

	// Set authentication
	switch g.Config.AuthType {
	case "bearer":
		g.headers["Authorization"] = "Bearer " + g.Config.BearerToken
	case "api_key":
		g.headers["X-API-Key"] = g.Config.APIKey
	}

	return nil
}

func (g *GraphQLConnector) Disconnect() error {
	// HTTP client doesn't need explicit disconnection
	return nil
}

func (g *GraphQLConnector) Read() (interface{}, error) {
	// Implement read logic
	return nil, nil
}

func (g *GraphQLConnector) Write(data interface{}) error {
	// Implement write logic
	return nil
}

func (g *GraphQLConnector) GetConfig() interface{} {
	return g.Config
}

// Execute GraphQL query
func (g *GraphQLConnector) Execute(query string, variables map[string]interface{}, operationName string) (*GraphQLResponse, error) {
	request := GraphQLRequest{
		Query:         query,
		Variables:     variables,
		OperationName: operationName,
	}

	jsonBody, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", g.Config.Endpoint, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}

	for k, v := range g.headers {
		req.Header.Set(k, v)
	}

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var graphQLResponse GraphQLResponse
	if err := json.NewDecoder(resp.Body).Decode(&graphQLResponse); err != nil {
		return nil, err
	}

	return &graphQLResponse, nil
}
