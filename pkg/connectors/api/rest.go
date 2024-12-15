package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"

	"github.com/ivikasavnish/datapipe/pkg/connectors"
)

type RESTConnector struct {
	connectors.BaseConnector
	Config  RESTConfig
	client  *http.Client
	headers map[string]string
}

type RESTConfig struct {
	BaseURL     string
	Headers     map[string]string
	AuthType    string // "basic", "bearer", "api_key"
	Username    string
	Password    string
	BearerToken string
	APIKey      string
	Timeout     int
}

func NewRESTConnector(config RESTConfig) *RESTConnector {
	return &RESTConnector{
		BaseConnector: connectors.BaseConnector{
			Name:        "REST API",
			Description: "REST API connector",
			Version:     "1.0.0",
			Type:        "api",
		},
		Config:  config,
		headers: make(map[string]string),
	}
}

func (r *RESTConnector) Connect() error {
	r.client = &http.Client{
		Timeout: time.Duration(r.Config.Timeout) * time.Second,
	}

	// Set default headers
	r.headers["Content-Type"] = "application/json"

	// Add custom headers
	for k, v := range r.Config.Headers {
		r.headers[k] = v
	}

	// Set authentication
	switch r.Config.AuthType {
	case "basic":
		r.headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString(
			[]byte(r.Config.Username+":"+r.Config.Password))
	case "bearer":
		r.headers["Authorization"] = "Bearer " + r.Config.BearerToken
	case "api_key":
		r.headers["X-API-Key"] = r.Config.APIKey
	}

	return nil
}

func (r *RESTConnector) Disconnect() error {
	// HTTP client doesn't need explicit disconnection
	return nil
}

func (r *RESTConnector) Read() (interface{}, error) {
	// Implement read logic
	return nil, nil
}

func (r *RESTConnector) Write(data interface{}) error {
	// Implement write logic
	return nil
}

func (r *RESTConnector) GetConfig() interface{} {
	return r.Config
}

// Additional REST-specific methods
func (r *RESTConnector) Get(endpoint string) (*http.Response, error) {
	req, err := http.NewRequest("GET", r.Config.BaseURL+endpoint, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range r.headers {
		req.Header.Set(k, v)
	}

	return r.client.Do(req)
}

func (r *RESTConnector) Post(endpoint string, body interface{}) (*http.Response, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", r.Config.BaseURL+endpoint, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}

	for k, v := range r.headers {
		req.Header.Set(k, v)
	}

	return r.client.Do(req)
}
