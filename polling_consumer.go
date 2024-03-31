package messagestoresdk

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	ms "github.com/mmcnicol/message-store"
)

// PollingConsumerConfig represents the configuration for the message polling consumer
type PollingConsumerConfig struct {
	Host    string        // Host of the message store server
	Port    int           // Port of the message store server
	Timeout time.Duration // Timeout for HTTP requests
}

// NewPollingConsumerConfig creates a new instance of PollingConsumerConfig
func NewPollingConsumerConfig() *PollingConsumerConfig {
	return &PollingConsumerConfig{}
}

// PollingConsumer represents a message polling consumer
type PollingConsumer struct {
	Config *PollingConsumerConfig // Configuration for the message polling consumer
}

// NewPollingConsumer creates a new instance of PollingConsumer
func NewPollingConsumer(config *PollingConsumerConfig) *PollingConsumer {
	// Check if timeout is zero, and if so, default it to 30 seconds
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	return &PollingConsumer{
		Config: config,
	}
}

// PollForNextEntry reads an entry from the given offset+1 from the specified topic, after sleeping for the specified poll interval
func (c *PollingConsumer) PollForNextEntry(topic string, offset int64, pollDuration time.Duration) (*ms.Entry, error) {

	// Construct the endpoint URL
	endpoint := fmt.Sprintf("http://%s:%d/consume?topic=%s&offset=%d&pollDuration=%s", c.Config.Host, c.Config.Port, topic, offset, pollDuration.String())

	// Create a custom HTTP client with a timeout
	httpClient := &http.Client{
		Timeout: c.Config.Timeout,
	}

	// perform HTTP request
	resp, err := httpClient.Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to send GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var topicEntry TopicEntry
	err = json.NewDecoder(resp.Body).Decode(&topicEntry)
	if err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %v", err)
	}

	key, err := base64.StdEncoding.DecodeString(topicEntry.Key)
	if err != nil {
		log.Fatal("error:", err)
		return nil, fmt.Errorf("failed to decode base64 string for Key: %v", err)
	}
	value, err := base64.StdEncoding.DecodeString(topicEntry.Value)
	if err != nil {
		log.Fatal("error:", err)
		return nil, fmt.Errorf("failed to decode base64 string for Value: %v", err)
	}

	entry := &ms.Entry{}
	entry.Key = []byte(key)
	entry.Value = []byte(value)
	entry.Timestamp = topicEntry.Timestamp

	return entry, nil
}
