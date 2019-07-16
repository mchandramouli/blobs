package main

import (
	"context" // Use "golang.org/x/net/context" for Golang version <= 1.6
	"flag"
	"net/http"
	"log"
	"github.com/golang/glog"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"

	gw "example.com/main/blob"
)

var (
	// command-line options:
	// gRPC server endpoint
	grpcServerEndpoint    = flag.String("grpc-server-endpoint", "localhost:34001", "gRPC server endpoint")
	proxyServerPortNumber = flag.String("http-port", ":34002", "Proxy server port number")
)

func run() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Register gRPC server endpoint
	// Note: Make sure the gRPC server is running properly and accessible
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}
	err := gw.RegisterBlobAgentHandlerFromEndpoint(ctx, mux, *grpcServerEndpoint, opts)
	if err != nil {
		return err
	}

	// Start HTTP server (and proxy calls to gRPC server endpoint)
	log.Print("HTTP Server started at port " + *proxyServerPortNumber + " for grpc server endpoint=" + *grpcServerEndpoint)
	return http.ListenAndServe(*proxyServerPortNumber, mux)
}

func main() {
	flag.Parse()
	defer glog.Flush()

	if err := run(); err != nil {
		glog.Fatal(err)
	}
}
