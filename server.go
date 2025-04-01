package memcache

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Server represents a memcached server with its address, connection, and a mutex for thread-safety.
type Server struct {
	Address string     // The network address of the memcached server.
	conn    *Conn      // The connection to the memcached server.
	mu      sync.Mutex // Mutex to ensure thread-safe operations on the server connection.
}

// NewServer creates a new Server instance using the provided address.
// It establishes a connection to the server and returns an error if the connection fails.
func NewServer(address string) (s *Server, err error) {
	conn, err := NewConn(address)
	if err != nil {
		return
	}
	s = &Server{
		Address: address,
		conn:    conn,
		mu:      sync.Mutex{},
	}
	return
}

// WriteCommand sends a command string to the memcached server and reads a single-line response.
// It locks the connection for thread-safety, and returns the trimmed response or an error.
func (s *Server) WriteCommand(cmd string) (res string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write the command to the server.
	_, err = s.conn.Write([]byte(cmd))
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}
	// Create a buffered reader for reading the response.
	reader := bufio.NewReader(s.conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		err = errors.Join(ErrReadFailed, err)
		return
	}
	// Trim any extra whitespace from the response.
	res = strings.TrimSpace(response)
	return
}

// GetValue retrieves the value associated with the given key from the memcached server.
// If withCAS is true, it sends a "gets" command to also retrieve the CAS token; otherwise, it uses "get".
// It returns the value, CAS token (if requested), and an error if any.
func (s *Server) GetValue(key string, withCAS bool) (value string, cas uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Determine the command based on whether CAS is needed.
	var cmd string
	if withCAS {
		cmd = fmt.Sprintf("gets %s\r\n", key)
	} else {
		cmd = fmt.Sprintf("get %s\r\n", key)
	}
	_, err = s.conn.Write([]byte(cmd))
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}

	reader := bufio.NewReader(s.conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		err = errors.Join(ErrReadFailed, err)
		return
	}
	line = strings.TrimSpace(line)
	// If the response indicates the key was not found, return an error.
	if line == "END" {
		err = ErrNotFound
		return
	}
	// Parse the response header line.
	parts := strings.Split(line, " ")
	if len(parts) < 4 || parts[0] != "VALUE" {
		err = ErrUnexpectedResponse
		return
	}
	// If CAS is requested, extract the CAS token.
	if withCAS {
		if len(parts) < 5 {
			err = ErrUnexpectedResponse
			return
		}
		cas, err = strconv.ParseUint(parts[4], 10, 64)
		if err != nil {
			err = errors.Join(ErrInternal, err)
			return
		}
	}

	// Determine the length of the data block.
	byteCount, err := strconv.Atoi(parts[3])
	if err != nil {
		err = errors.Join(ErrInternal, err)
		return
	}
	// Read the data block which includes the terminating "\r\n".
	data := make([]byte, byteCount+2)
	_, err = reader.Read(data)
	if err != nil {
		err = errors.Join(ErrReadFailed, err)
		return
	}
	// Extract the actual value by trimming the trailing "\r\n".
	value = string(data[:byteCount])
	// Read the terminating "END" line.
	endLine, err := reader.ReadString('\n')
	if err != nil {
		err = errors.Join(ErrReadFailed, err)
		return
	}
	if strings.TrimSpace(endLine) != "END" {
		err = ErrUnexpectedResponse
		return
	}
	return
}

// GetStats sends a "stats" command to the memcached server to retrieve various statistics.
// It returns a map of statistic keys to their values and an error if encountered.
func (s *Server) GetStats() (stats map[string]string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cmd := "stats\r\n"
	_, err = s.conn.Write([]byte(cmd))
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}

	reader := bufio.NewReader(s.conn)
	// Read each line until the "END" marker is found.
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			err = errors.Join(ErrReadFailed, err)
			return nil, err
		}
		line = strings.TrimSpace(line)
		if line == "END" {
			break
		}
		// Each stat line is expected to have the format: "STAT <key> <value>"
		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 3 {
			err = ErrUnexpectedResponse
			return nil, err
		}
		if parts[0] != "STAT" {
			err = ErrUnexpectedResponse
			return nil, err
		}
		key := parts[1]
		value := parts[2]
		if stats == nil {
			stats = make(map[string]string)
		}
		stats[key] = value
	}

	return
}

// Close terminates the connection to the memcached server.
func (s *Server) Close() {
	s.conn.conn.Close()
}

// Extra sends a custom command (cmd) to the memcached server and collects multi-line responses.
// It continues reading until an "END" line is encountered, then returns the concatenated response or an error.
func (s *Server) Extra(cmd string) (res string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err = s.conn.Write([]byte(cmd))
	if err != nil {
		err = errors.Join(ErrWriteFailed, err)
		return
	}

	reader := bufio.NewReader(s.conn)
	// Read lines until "END" is encountered.
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			err = errors.Join(ErrReadFailed, err)
			return res, err
		}
		line = strings.TrimSpace(line)
		if line == "END" {
			break
		}
		res += line + "\n"
	}

	return
}
