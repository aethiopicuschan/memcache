package memcache

import "errors"

var ErrEmptyAddresses = errors.New("no addresses provided")
var ErrStoreFailed = errors.New("store command failed")
var ErrWriteFailed = errors.New("write command failed")
var ErrReadFailed = errors.New("read failed")
var ErrNotFound = errors.New("key not found")
var ErrUnexpectedResponse = errors.New("unexpected response from server")
var ErrInternal = errors.New("internal error")
var ErrNoServers = errors.New("no servers available")
