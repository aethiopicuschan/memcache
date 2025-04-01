# memcache

[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen?style=flat-square)](/LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/aethiopicuschan/memcache.svg)](https://pkg.go.dev/github.com/aethiopicuschan/memcache)
[![Go Report Card](https://goreportcard.com/badge/github.com/aethiopicuschan/memcache)](https://goreportcard.com/report/github.com/aethiopicuschan/memcache)
[![CI](https://github.com/aethiopicuschan/memcache/actions/workflows/ci.yaml/badge.svg)](https://github.com/aethiopicuschan/memcache/actions/workflows/ci.yaml)

Client library for [memcached](https://memcached.org/) written in Go.

## Installation

```bash
go get -u github.com/aethiopicuschan/memcache
```

## Features

It supports connections to multiple servers. In addition, it supports almost all commands and comes with an Extra feature that allows users to specify custom commands and receive responses.

## Usage

```go
package main

import (
	"fmt"
	"log"

	"github.com/aethiopicuschan/memcache"
)

func main() {
	client, err := memcache.NewClient("localhost:11211")
	if err != nil {
		log.Fatal(err)
	}
	err = client.Set("key", "value", 0)
	if err != nil {
		log.Fatal(err)
	}
	value, err := client.Get("key")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Value:", value)
	// Output: Value: value
}
```
