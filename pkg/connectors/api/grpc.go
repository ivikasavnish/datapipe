package api

import (
	"context"
	"fmt"
	"time"

	"github.com/ivikasavnish/datapipe/pkg/connectors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type GRPCConnector struct {
	connectors.BaseConnector
	Config     GRPCConfig
	connection *grpc.ClientConn
	ctx        context.Context
}

type GRPCConfig struct {
	Target      string
	ServiceName string
	TLS         bool
	CertFile    string
	Timeout     time.Duration
	Headers     map[string]string
}

func NewGRPCConnector(config GRPCConfig) *GRPCConnector {
	return &GRPCConnector{
		BaseConnector: connectors.BaseConnector{
			Name:        "gRPC",
			Description: "Generic gRPC connector",
			Version:     "1.0.0",
			Type:        "api",
		},
		Config: config,
		ctx:    context.Background(),
	}
}

func (g *GRPCConnector) Connect() error {
	var opts []grpc.DialOption

	if g.Config.TLS {
		creds, err := credentials.NewClientTLSFromFile(g.Config.CertFile, "")
		if err != nil {
			return fmt.Errorf("failed to create TLS credentials: %v", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	// Add timeout
	opts = append(opts, grpc.WithTimeout(g.Config.Timeout))

	// Connect to the server
	conn, err := grpc.Dial(g.Config.Target, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to gRPC server: %v", err)
	}

	g.connection = conn
	return nil
}

func (g *GRPCConnector) Disconnect() error {
	if g.connection != nil {
		return g.connection.Close()
	}
	return nil
}

func (g *GRPCConnector) Read() (interface{}, error) {
	return nil, nil
}

func (g *GRPCConnector) Write(data interface{}) error {
	return nil
}

func (g *GRPCConnector) GetConfig() interface{} {
	return g.Config
}

// Additional gRPC-specific methods
func (g *GRPCConnector) GetConnection() *grpc.ClientConn {
	return g.connection
}

func (g *GRPCConnector) GetContext() context.Context {
	// Create context with headers
	md := metadata.New(g.Config.Headers)
	return metadata.NewOutgoingContext(g.ctx, md)
}

// Generic method to invoke unary RPC
func (g *GRPCConnector) InvokeUnary(method string, req interface{}, resp interface{}) error {
	ctx, cancel := context.WithTimeout(g.GetContext(), g.Config.Timeout)
	defer cancel()

	return g.connection.Invoke(ctx, fmt.Sprintf("/%s/%s", g.Config.ServiceName, method), req, resp)
}

// Generic method to create a client stream
func (g *GRPCConnector) NewClientStream(method string, desc *grpc.StreamDesc) (grpc.ClientStream, error) {
	ctx, cancel := context.WithTimeout(g.GetContext(), g.Config.Timeout)
	defer cancel()

	return g.connection.NewStream(ctx, desc, fmt.Sprintf("/%s/%s", g.Config.ServiceName, method))
}

// Helper method to create interceptor
func (g *GRPCConnector) UnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()

		// Add headers from config
		if len(g.Config.Headers) > 0 {
			ctx = metadata.NewOutgoingContext(ctx, metadata.New(g.Config.Headers))
		}

		err := invoker(ctx, method, req, reply, cc, opts...)

		// Log the call duration and error if any
		duration := time.Since(start)
		if err != nil {
			fmt.Printf("gRPC call failed: method=%s duration=%v error=%v\n", method, duration, err)
		} else {
			fmt.Printf("gRPC call succeeded: method=%s duration=%v\n", method, duration)
		}

		return err
	}
}

// Helper method to create stream interceptor
func (g *GRPCConnector) StreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		start := time.Now()

		// Add headers from config
		if len(g.Config.Headers) > 0 {
			ctx = metadata.NewOutgoingContext(ctx, metadata.New(g.Config.Headers))
		}

		stream, err := streamer(ctx, desc, cc, method, opts...)

		// Log the stream creation
		duration := time.Since(start)
		if err != nil {
			fmt.Printf("gRPC stream creation failed: method=%s duration=%v error=%v\n", method, duration, err)
		} else {
			fmt.Printf("gRPC stream creation succeeded: method=%s duration=%v\n", method, duration)
		}

		return stream, err
	}
}
