variable "kubectl_context_name" {}
variable "kubectl_executable_name" {}
variable "namespace" {}
variable "node_selector_label"{}
variable "grpc_server_endpoint" {}

variable "reverse-proxy" {
  type = "map"
}

variable "service_port" {
  default = 35002
}