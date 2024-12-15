package filesystem

import (
	"io"
	"os"
	"path/filepath"

	"github.com/ivikasavnish/datapipe/pkg/connectors"
)

type LocalFSConnector struct {
	connectors.BaseConnector
	Config LocalFSConfig
}

type LocalFSConfig struct {
	BasePath string
}

func NewLocalFSConnector(config LocalFSConfig) *LocalFSConnector {
	return &LocalFSConnector{
		BaseConnector: connectors.BaseConnector{
			Name:        "Local File System",
			Description: "Local file system connector",
			Version:     "1.0.0",
			Type:        "filesystem",
		},
		Config: config,
	}
}

func (l *LocalFSConnector) Connect() error {
	// Verify base path exists and is accessible
	_, err := os.Stat(l.Config.BasePath)
	return err
}

func (l *LocalFSConnector) Disconnect() error {
	// No disconnection needed for local filesystem
	return nil
}

func (l *LocalFSConnector) Read() (interface{}, error) {
	// Implement read logic
	return nil, nil
}

func (l *LocalFSConnector) Write(data interface{}) error {
	// Implement write logic
	return nil
}

func (l *LocalFSConnector) GetConfig() interface{} {
	return l.Config
}

// Additional Local FS-specific methods
func (l *LocalFSConnector) ListFiles(path string) ([]string, error) {
	fullPath := filepath.Join(l.Config.BasePath, path)
	var files []string

	err := filepath.Walk(fullPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relPath, err := filepath.Rel(l.Config.BasePath, path)
			if err != nil {
				return err
			}
			files = append(files, relPath)
		}
		return nil
	})

	return files, err
}

func (l *LocalFSConnector) ReadFile(path string) ([]byte, error) {
	fullPath := filepath.Join(l.Config.BasePath, path)
	return os.ReadFile(fullPath)
}

func (l *LocalFSConnector) WriteFile(path string, data []byte) error {
	fullPath := filepath.Join(l.Config.BasePath, path)

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return os.WriteFile(fullPath, data, 0644)
}

func (l *LocalFSConnector) CopyFile(src, dst string) error {
	srcPath := filepath.Join(l.Config.BasePath, src)
	dstPath := filepath.Join(l.Config.BasePath, dst)

	sourceFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// Ensure destination directory exists
	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return err
	}

	destFile, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

func (l *LocalFSConnector) DeleteFile(path string) error {
	fullPath := filepath.Join(l.Config.BasePath, path)
	return os.Remove(fullPath)
}

func (l *LocalFSConnector) FileExists(path string) bool {
	fullPath := filepath.Join(l.Config.BasePath, path)
	_, err := os.Stat(fullPath)
	return err == nil
}
