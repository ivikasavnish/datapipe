package streaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type FlinkConnector struct {
	BaseConnector
	Config FlinkConfig
	client *http.Client
	ctx    context.Context
}

type FlinkConfig struct {
	JobManagerURL string
	JobName       string
	Parallelism   int
	SavepointDir  string
	EntryClass    string
	ProgramArgs   []string
}

type FlinkJob struct {
	JobID       string    `json:"jid"`
	Name        string    `json:"name"`
	State       string    `json:"state"`
	StartTime   time.Time `json:"start-time"`
	EndTime     time.Time `json:"end-time,omitempty"`
	Duration    int64     `json:"duration"`
	Parallelism int       `json:"parallelism"`
}

func NewFlinkConnector(config FlinkConfig) *FlinkConnector {
	return &FlinkConnector{
		BaseConnector: BaseConnector{
			Name:        "Apache Flink",
			Description: "Apache Flink streaming connector",
			Version:     "1.0.0",
			Type:        "streaming",
		},
		Config: config,
		ctx:    context.Background(),
	}
}

func (f *FlinkConnector) Connect() error {
	f.client = &http.Client{
		Timeout: time.Second * 30,
	}
	
	// Test connection to Flink JobManager
	resp, err := f.client.Get(fmt.Sprintf("%s/overview", f.Config.JobManagerURL))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to connect to Flink JobManager: %s", resp.Status)
	}
	
	return nil
}

func (f *FlinkConnector) Disconnect() error {
	return nil
}

func (f *FlinkConnector) Read() (interface{}, error) {
	return nil, nil
}

func (f *FlinkConnector) Write(data interface{}) error {
	return nil
}

func (f *FlinkConnector) GetConfig() interface{} {
	return f.Config
}

// Additional Flink-specific methods
func (f *FlinkConnector) SubmitJob(jarPath string) (*FlinkJob, error) {
	// Upload JAR
	jarID, err := f.uploadJar(jarPath)
	if err != nil {
		return nil, err
	}
	
	// Submit job
	payload := map[string]interface{}{
		"entryClass":    f.Config.EntryClass,
		"parallelism":   f.Config.Parallelism,
		"programArgs":   f.Config.ProgramArgs,
		"savepointPath": f.Config.SavepointDir,
	}
	
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	
	url := fmt.Sprintf("%s/jars/%s/run", f.Config.JobManagerURL, jarID)
	resp, err := f.client.Post(url, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	var result struct {
		JobID string `json:"jobid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	
	return f.GetJobStatus(result.JobID)
}

func (f *FlinkConnector) uploadJar(jarPath string) (string, error) {
	file, err := os.Open(jarPath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	
	url := fmt.Sprintf("%s/jars/upload", f.Config.JobManagerURL)
	resp, err := f.client.Post(url, "application/x-java-archive", file)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	
	var result struct {
		Filename string `json:"filename"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	
	return result.Filename, nil
}

func (f *FlinkConnector) GetJobStatus(jobID string) (*FlinkJob, error) {
	url := fmt.Sprintf("%s/jobs/%s", f.Config.JobManagerURL, jobID)
	resp, err := f.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	var job FlinkJob
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}
	
	return &job, nil
}

func (f *FlinkConnector) CancelJob(jobID string, withSavepoint bool) error {
	url := fmt.Sprintf("%s/jobs/%s/cancel", f.Config.JobManagerURL, jobID)
	if withSavepoint {
		url = fmt.Sprintf("%s?targetDirectory=%s", url, f.Config.SavepointDir)
	}
	
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return err
	}
	
	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to cancel job %s: %s", jobID, resp.Status)
	}
	
	return nil
}

func (f *FlinkConnector) GetJobMetrics(jobID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/jobs/%s/metrics", f.Config.JobManagerURL, jobID)
	resp, err := f.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	var metrics map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		return nil, err
	}
	
	return metrics, nil
}
