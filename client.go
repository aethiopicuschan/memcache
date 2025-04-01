package memcache

import (
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
)

// Server has the address of a memcached server and a connection to it.
// Client is a wrapper around multiple servers.
type Client struct {
	servers []*Server
	mu      sync.RWMutex
}

// NewClient creates a new Client instance with the provided memcached server addresses.
// It initializes the servers by creating a new Server instance for each address.
// If no addresses are provided, it returns ErrEmptyAddresses.
func NewClient(addresses ...string) (c *Client, err error) {
	if len(addresses) == 0 {
		err = ErrEmptyAddresses
		return
	}
	servers := make([]*Server, len(addresses))
	for i, addr := range addresses {
		servers[i], err = NewServer(addr)
		if err != nil {
			return
		}
	}
	c = &Client{servers: servers}
	return
}

// pickServer selects the appropriate server for a given key using a CRC32 hash.
// It returns an error if there are no servers available.
func (c *Client) pickServer(key string) (s *Server, err error) {
	if len(c.servers) == 0 {
		err = ErrNoServers
		return
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	hash := crc32.ChecksumIEEE([]byte(key))
	idx := int(hash) % len(c.servers)
	s = c.servers[idx]
	return
}

// pickServerFromAddr finds a server based on its address.
// It returns the server, its index in the list, and an error if the server is not found.
func (c *Client) pickServerFromAddr(addr string) (s *Server, index int, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, server := range c.servers {
		if server.Address == addr {
			s = server
			return
		}
		index++
	}
	err = ErrNotFound
	return
}

// Set sends a "set" command to store a key-value pair in the memcached server.
// The expiration parameter specifies the time until the key expires.
// It returns an error if the command fails or the store operation is not acknowledged.
func (c *Client) Set(key, value string, expiration int) (err error) {
	server, err := c.pickServer(key)
	if err != nil {
		return
	}
	// set <key> <flags> <exptime> <bytes>\r\n<data>\r\n
	command := fmt.Sprintf("set %s 0 %d %d\r\n%s\r\n", key, expiration, len(value), value)
	resp, err := server.WriteCommand(command)
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}
	if resp != "STORED" {
		err = ErrStoreFailed
		return
	}
	return nil
}

// Add sends an "add" command to store a key-value pair only if the key does not already exist.
// The expiration parameter specifies the time until the key expires.
// It returns an error if the command fails or the store operation is not acknowledged.
func (c *Client) Add(key, value string, expiration int) (err error) {
	server, err := c.pickServer(key)
	if err != nil {
		return
	}
	// add <key> <flags> <exptime> <bytes>\r\n<data>\r\n
	command := fmt.Sprintf("add %s 0 %d %d\r\n%s\r\n", key, expiration, len(value), value)
	resp, err := server.WriteCommand(command)
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}
	if resp != "STORED" {
		err = ErrStoreFailed
		return
	}
	return nil
}

// Replace sends a "replace" command to update the value of an existing key.
// The expiration parameter specifies the time until the key expires.
// It returns an error if the command fails or the store operation is not acknowledged.
func (c *Client) Replace(key, value string, expiration int) (err error) {
	server, err := c.pickServer(key)
	if err != nil {
		return
	}
	// replace <key> <flags> <exptime> <bytes>\r\n<data>\r\n
	command := fmt.Sprintf("replace %s 0 %d %d\r\n%s\r\n", key, expiration, len(value), value)
	resp, err := server.WriteCommand(command)
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}
	if resp != "STORED" {
		err = ErrStoreFailed
		return
	}
	return nil
}

// Append sends an "append" command to add data to the end of the existing value for a key.
// It returns an error if the command fails or the operation is not acknowledged.
func (c *Client) Append(key, value string) (err error) {
	server, err := c.pickServer(key)
	if err != nil {
		return
	}
	// append <key> <flags> <exptime> <bytes>\r\n<data>\r\n
	command := fmt.Sprintf("append %s 0 0 %d\r\n%s\r\n", key, len(value), value)
	resp, err := server.WriteCommand(command)
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}
	if resp != "STORED" {
		err = ErrStoreFailed
		return
	}
	return nil
}

// Prepend sends a "prepend" command to add data to the beginning of the existing value for a key.
// It returns an error if the command fails or the operation is not acknowledged.
func (c *Client) Prepend(key, value string) (err error) {
	server, err := c.pickServer(key)
	if err != nil {
		return
	}
	// prepend <key> <flags> <exptime> <bytes>\r\n<data>\r\n
	command := fmt.Sprintf("prepend %s 0 0 %d\r\n%s\r\n", key, len(value), value)
	resp, err := server.WriteCommand(command)
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}
	if resp != "STORED" {
		err = ErrStoreFailed
		return
	}
	return nil
}

// CAS (Check And Set) sends a "cas" command to update a key's value only if it has not been modified since it was last read.
// The cas parameter is the unique value used for this check.
// It returns an error if the command fails or the store operation is not acknowledged.
func (c *Client) CAS(key, value string, expiration int, cas uint64) (err error) {
	server, err := c.pickServer(key)
	if err != nil {
		return
	}
	// cas <key> <flags> <exptime> <bytes> <cas_unique>\r\n<data>\r\n
	command := fmt.Sprintf("cas %s 0 %d %d %d\r\n%s\r\n", key, expiration, len(value), cas, value)
	resp, err := server.WriteCommand(command)
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}
	if resp != "STORED" {
		err = ErrStoreFailed
		return
	}
	return nil
}

// Get retrieves the value associated with the given key using a "get" command.
// It returns the value and an error if any.
func (c *Client) Get(key string) (value string, err error) {
	server, err := c.pickServer(key)
	if err != nil {
		return
	}
	// GetValue returns the value along with a cas token if requested; here we don't need the cas.
	value, _, err = server.GetValue(key, false)
	return
}

// Gets retrieves the value and its CAS (Check And Set) token for the given key using a "gets" command.
// It returns the value, the CAS token, and an error if any.
func (c *Client) Gets(key string) (value string, cas uint64, err error) {
	server, err := c.pickServer(key)
	if err != nil {
		return
	}
	value, cas, err = server.GetValue(key, true)
	return
}

// Delete sends a "delete" command to remove the key from the memcached server.
// It returns an error if the command fails or the deletion is not acknowledged.
func (c *Client) Delete(key string) (err error) {
	server, err := c.pickServer(key)
	if err != nil {
		return
	}
	// delete <key>\r\n
	command := fmt.Sprintf("delete %s\r\n", key)
	resp, err := server.WriteCommand(command)
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}
	if resp != "DELETED" {
		err = ErrStoreFailed
		return
	}
	return nil
}

// FlushAll sends a "flush_all" command to all servers to clear all keys after the specified delay in seconds.
// It returns an error if any server fails to acknowledge the command.
func (c *Client) FlushAll(sec int) (err error) {
	// flush_all <exptime>\r\n
	command := fmt.Sprintf("flush_all %d\r\n", sec)
	for _, server := range c.servers {
		resp, err := server.WriteCommand(command)
		if err != nil {
			err = errors.Join(ErrWriteFailed, err)
			return err
		}
		if resp != "OK" {
			err = ErrStoreFailed
			return err
		}
	}
	return
}

// Increment sends an "incr" command to increase the numeric value stored at the given key by delta.
// It returns the new value and an error if the command fails or if the key is not found.
func (c *Client) Increment(key string, delta int) (newValue uint64, err error) {
	server, err := c.pickServer(key)
	if err != nil {
		return
	}
	// incr <key> <delta>\r\n
	command := fmt.Sprintf("incr %s %d\r\n", key, delta)
	resp, err := server.WriteCommand(command)
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}
	if resp == "NOT_FOUND" {
		err = ErrNotFound
		return
	}
	fmt.Sscanf(resp, "%d", &newValue)
	return
}

// Decrement sends a "decr" command to decrease the numeric value stored at the given key by delta.
// It returns the new value and an error if the command fails or if the key is not found.
func (c *Client) Decrement(key string, delta int) (newValue uint64, err error) {
	server, err := c.pickServer(key)
	if err != nil {
		return
	}
	// decr <key> <delta>\r\n
	command := fmt.Sprintf("decr %s %d\r\n", key, delta)
	resp, err := server.WriteCommand(command)
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}
	if resp == "NOT_FOUND" {
		err = ErrNotFound
		return
	}
	fmt.Sscanf(resp, "%d", &newValue)
	return
}

// Touch sends a "touch" command to update the expiration time of the given key without modifying its value.
// It returns an error if the command fails or if the key is not acknowledged.
func (c *Client) Touch(key string, expiration int) (err error) {
	server, err := c.pickServer(key)
	if err != nil {
		return
	}
	// touch <key> <exptime>\r\n
	command := fmt.Sprintf("touch %s %d\r\n", key, expiration)
	resp, err := server.WriteCommand(command)
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}
	if resp != "OK" {
		err = ErrStoreFailed
		return
	}
	return nil
}

// Stats retrieves statistics from the memcached server identified by the given address.
// It returns a map of stat keys and values, along with any error encountered.
func (c *Client) Stats(addr string) (stats map[string]string, err error) {
	server, _, err := c.pickServerFromAddr(addr)
	if err != nil {
		return
	}
	return server.GetStats()
}

// StatsAll retrieves and merges statistics from all memcached servers in the client.
// It returns a merged map of stat keys and values, along with any error encountered.
func (c *Client) StatsAll() (mergedStats map[string]string, err error) {
	mergedStats = make(map[string]string)
	for _, server := range c.servers {
		stats, err := server.GetStats()
		if err != nil {
			err = errors.Join(ErrWriteFailed, err)
			return mergedStats, err
		}
		for key, value := range stats {
			if _, exists := mergedStats[key]; !exists {
				mergedStats[key] = value
			}
		}
	}
	return
}

// Version retrieves the version string from the memcached server identified by the given address.
// It sends a "version" command and returns the trimmed version string or an error.
func (c *Client) Version(addr string) (version string, err error) {
	server, _, err := c.pickServerFromAddr(addr)
	if err != nil {
		return
	}
	// version\r\n
	command := "version\r\n"
	resp, err := server.WriteCommand(command)
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}
	version = strings.TrimSpace(resp)
	return
}

// Versions retrieves the version strings from all memcached servers in the client.
// It returns a map where the key is the server address and the value is its version string.
func (c *Client) Versions() (versions map[string]string, err error) {
	versions = make(map[string]string)
	for _, server := range c.servers {
		// version\r\n
		command := "version\r\n"
		resp, err := server.WriteCommand(command)
		if err != nil {
			err = errors.Join(ErrWriteFailed, err)
			return nil, err
		}
		version := strings.TrimSpace(resp)
		versions[server.Address] = version
	}
	return
}

// Quit closes the connection to the memcached server identified by the given address,
// removes it from the client's server list, and returns an error if any.
func (c *Client) Quit(addr string) (err error) {
	_, index, err := c.pickServerFromAddr(addr)
	if err != nil {
		return
	}
	c.servers[index].Close()
	c.servers = append(c.servers[:index], c.servers[index+1:]...)
	return
}

// QuitAll closes the connections to all memcached servers and clears the server list.
// It returns an error if any operation fails.
func (c *Client) QuitAll() (err error) {
	for _, server := range c.servers {
		server.Close()
	}
	c.servers = nil
	return
}

// Verbosity sends a "verbosity" command to all memcached servers to adjust their logging level.
// It returns an error if any server fails to acknowledge the command.
func (c *Client) Verbosity(level int) (err error) {
	for _, server := range c.servers {
		// verbosity <level>\r\n
		command := fmt.Sprintf("verbosity %d\r\n", level)
		resp, err := server.WriteCommand(command)
		if err != nil {
			err = errors.Join(ErrWriteFailed, err)
			return err
		}
		if resp != "OK" {
			err = ErrStoreFailed
			return err
		}
	}
	return
}

// Extra sends a custom command (provided by cmd) to the memcached server identified by the given address.
// It returns the response from the server and an error if any.
func (c *Client) Extra(addr string, cmd string) (res string, err error) {
	s, _, err := c.pickServerFromAddr(addr)
	if err != nil {
		return
	}
	res, err = s.Extra(cmd)
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return res, err
	}
	return
}
