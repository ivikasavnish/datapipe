package filesystem

import (
	"github.com/colinmarc/hdfs/v2"
	"io"
	"os"
	"path/filepath"
)

type HDFSConnector struct {
	BaseConnector
	Config HDFSConfig
	client *hdfs.Client
}

type HDFSConfig struct {
	NameNodeAddrs []string
	User         string
	BasePath     string
}

func NewHDFSConnector(config HDFSConfig) *HDFSConnector {
	return &HDFSConnector{
		BaseConnector: BaseConnector{
			Name:        "HDFS",
			Description: "Hadoop Distributed File System connector",
			Version:     "1.0.0",
			Type:        "filesystem",
		},
		Config: config,
	}
}

func (h *HDFSConnector) Connect() error {
	var err error
	options := hdfs.ClientOptions{
		Addresses: h.Config.NameNodeAddrs,
		User:     h.Config.User,
	}
	
	h.client, err = hdfs.NewClient(options)
	if err != nil {
		return err
	}
	
	// Verify base path exists
	_, err = h.client.Stat(h.Config.BasePath)
	return err
}

func (h *HDFSConnector) Disconnect() error {
	if h.client != nil {
		return h.client.Close()
	}
	return nil
}

func (h *HDFSConnector) Read() (interface{}, error) {
	// Implement read logic
	return nil, nil
}

func (h *HDFSConnector) Write(data interface{}) error {
	// Implement write logic
	return nil
}

func (h *HDFSConnector) GetConfig() interface{} {
	return h.Config
}

// Additional HDFS-specific methods
func (h *HDFSConnector) ListFiles(path string) ([]string, error) {
	fullPath := filepath.Join(h.Config.BasePath, path)
	var files []string
	
	fileInfos, err := h.client.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}
	
	for _, info := range fileInfos {
		if !info.IsDir() {
			relPath, err := filepath.Rel(h.Config.BasePath, filepath.Join(fullPath, info.Name()))
			if err != nil {
				return nil, err
			}
			files = append(files, relPath)
		}
	}
	
	return files, nil
}

func (h *HDFSConnector) ReadFile(path string) ([]byte, error) {
	fullPath := filepath.Join(h.Config.BasePath, path)
	return h.client.ReadFile(fullPath)
}

func (h *HDFSConnector) WriteFile(path string, data []byte) error {
	fullPath := filepath.Join(h.Config.BasePath, path)
	
	// Ensure parent directory exists
	parent := filepath.Dir(fullPath)
	if err := h.client.MkdirAll(parent, 0755); err != nil {
		return err
	}
	
	writer, err := h.client.Create(fullPath)
	if err != nil {
		return err
	}
	defer writer.Close()
	
	_, err = writer.Write(data)
	return err
}

func (h *HDFSConnector) CopyFile(src, dst string) error {
	srcPath := filepath.Join(h.Config.BasePath, src)
	dstPath := filepath.Join(h.Config.BasePath, dst)
	
	reader, err := h.client.Open(srcPath)
	if err != nil {
		return err
	}
	defer reader.Close()
	
	// Ensure parent directory exists
	parent := filepath.Dir(dstPath)
	if err := h.client.MkdirAll(parent, 0755); err != nil {
		return err
	}
	
	writer, err := h.client.Create(dstPath)
	if err != nil {
		return err
	}
	defer writer.Close()
	
	_, err = io.Copy(writer, reader)
	return err
}

func (h *HDFSConnector) DeleteFile(path string) error {
	fullPath := filepath.Join(h.Config.BasePath, path)
	return h.client.Remove(fullPath)
}

func (h *HDFSConnector) FileExists(path string) bool {
	fullPath := filepath.Join(h.Config.BasePath, path)
	_, err := h.client.Stat(fullPath)
	return err == nil
}

func (h *HDFSConnector) GetFileInfo(path string) (os.FileInfo, error) {
	fullPath := filepath.Join(h.Config.BasePath, path)
	return h.client.Stat(fullPath)
}
