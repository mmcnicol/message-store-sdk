package messagestoresdk

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ms "github.com/mmcnicol/message-store"
	"github.com/stretchr/testify/assert"
)

func TestPollForNextEntry_shouldReturnAnEntryAndNoError(t *testing.T) {
	// Mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate successful response
		// Create a mock entry
		entry := ms.Entry{
			Key:       []byte("key"),
			Value:     []byte("value"),
			Timestamp: time.Now(),
		}
		//w.Header().Set("x-offset", "123")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(entry)
	}))
	defer server.Close()

	//fmt.Println(server.URL)
	host, port, err := extractHostAndPort(server.URL)
	if err != nil {
		fmt.Println("Error:", err)
	}

	// Create a Consumer with mock configuration
	config := NewPollingConsumerConfig()
	config.Host = host
	config.Port = port

	consumer := NewPollingConsumer(config)

	// Get an entry
	pollDuration := 100 * time.Millisecond
	topicEntry, err := consumer.PollForNextEntry("topic1", -1, pollDuration)

	// Verify the result
	assert.NoError(t, err)
	assert.NotNil(t, topicEntry)
	assert.Equal(t, string(topicEntry.Key), "key")
	assert.Equal(t, string(topicEntry.Value), "value")
}

func TestPollForNextEntry_shouldReturnNoEntryAndNoError(t *testing.T) {
	// Mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate successful response
		//w.Header().Set("x-offset", "123")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	//fmt.Println(server.URL)
	host, port, err := extractHostAndPort(server.URL)
	if err != nil {
		fmt.Println("Error:", err)
	}

	// Create a Consumer with mock configuration
	config := NewPollingConsumerConfig()
	config.Host = host
	config.Port = port

	consumer := NewPollingConsumer(config)

	// Get an entry
	pollDuration := 100 * time.Millisecond
	topicEntry, err := consumer.PollForNextEntry("topic1", -1, pollDuration)

	// Verify the result
	assert.NoError(t, err)
	assert.Nil(t, topicEntry)
}

func TestPollForNextEntry_Error(t *testing.T) {
	// Mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate error response
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	//fmt.Println(server.URL)
	host, port, err := extractHostAndPort(server.URL)
	if err != nil {
		fmt.Println("Error:", err)
	}

	// Create a PollingConsumer with mock configuration
	config := NewPollingConsumerConfig()
	config.Host = host
	config.Port = port

	consumer := NewPollingConsumer(config)

	// Get an entry
	pollDuration := 100 * time.Millisecond
	_, err = consumer.PollForNextEntry("topic1", -1, pollDuration)

	// Verify the error
	assert.Error(t, err)
}
