package main

import (
	"context"
	"flag"
	"net/http"
	"log"
	"github.com/golang/glog"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"

	gw "example.com/main/blob"
	"os"
)

const GRPC_SERVER_ENDPOINT_ENV_VAR = "grpc-server-endpoint"
const DEFAULT_GRPC_SERVER_ENDPOINT = "haystack-agent:35001"
const PROXY_SERVER_HTTP_PORT = "http-port"
const DEFAULT_PROXY_SERVER_HTTP_PORT = ":35002"

func getGrpcServerEndpoint() string {
	grpcServerEndpoint := os.Getenv(GRPC_SERVER_ENDPOINT_ENV_VAR)
	if grpcServerEndpoint == "" {
		return DEFAULT_GRPC_SERVER_ENDPOINT
	}
	return grpcServerEndpoint
}

func getProxyServerHttpPort() string {
	proxyServerHttpPort := os.Getenv(PROXY_SERVER_HTTP_PORT)
	if proxyServerHttpPort == "" {
		return DEFAULT_PROXY_SERVER_HTTP_PORT
	}

	return proxyServerHttpPort
}

func run() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	grpcServerEndpoint := getGrpcServerEndpoint()
	proxyServerHttpPort := getProxyServerHttpPort()
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}
	err := gw.RegisterBlobAgentHandlerFromEndpoint(ctx, mux, grpcServerEndpoint, opts)
	if err != nil {
		return err
	}

	log.Print("HTTP Server started at port " + proxyServerHttpPort + " for grpc server endpoint=" + grpcServerEndpoint)
	return http.ListenAndServe(proxyServerHttpPort, mux)
}

func main() {
	flag.Parse()
	defer glog.Flush()

	if err := run(); err != nil {
		glog.Fatal(err)
	}
}
