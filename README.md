## Blobs

This repository contains all the modules needed for an application. From creation of a blob, write it to a particular storage sink to read that blob back anytime you want. A blob can be any data that you may need to store somewhere for future need, preferably request and response data of a service. 

This library is divided into two for different types of usages: 

1. Standalone usage

	One can use [Stores](#stores) present in the library to directly dump blob to a particular sink and retrieve it whenever needed.
    
    You can look at our [sample application](https://github.com/ExpediaDotCom/blobs-example) for how to save a blob through a simple web application.
2. Use with [Haystack](https://expediadotcom.github.io/haystack/)
	
    One can use [haystack-blobs](haystack-blobs/README.md) to send blobs to [Haystack-Agent](https://github.com/ExpediaDotCom/haystack-agent) which then dispatches a blob via a dispatcher.
    
    Click [here](haystack-blobs/README.md) for more details.

## Table of content

- [Setup](#setup)
- [Blobs Core](#blobs-core)
- [Stores](#stores)
	* [File Store](#file-store)
	* [S3 Store](#s3-store)

## Setup

##### Clone

Use the following command to clone the repository including the submodules present in it:

`git clone --recursive git@github.com:ExpediaDotCom/blobs.git`

##### Build

Use the following command to build the repository:

`mvn clean package`

## Blobs Core

This module contains all the core classes needed to instrument the creation of the blobs and then start the process of writing it to a [store](#stores). Only the Blob Model is created by the `blob.proto` present in [blobs-grpc-models](haystack-blobs/README.md#models) module inside the package `com.expedia.www.blobs.model`.

## Stores

The stores are the sinks that can directly be integrated with a micro-service to dump a blob to a defined location.

#### File Store

This store is used to dump a blob to a local directory of a system where the micro-service is running.

#### S3 Store

This store will dump a blob to a given S3 bucket directly without using Haystack-Agent's dispatcher.