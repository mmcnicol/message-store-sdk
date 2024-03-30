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

// ConsumerConfig represents the configuration for the message consumer
type ConsumerConfig struct {
	Host    string        // Host of the message store server
	Port    int           // Port of the message store server
	Timeout time.Duration // Timeout for HTTP requests
}

// NewConsumerConfig creates a new instance of ConsumerConfig
func NewConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{}
}

// Consumer represents a message consumer
type Consumer struct {
	Config *ConsumerConfig // Configuration for the message consumer
}

// NewConsumer creates a new instance of Producer
func NewConsumer(config *ConsumerConfig) *Consumer {
	// Check if timeout is zero, and if so, default it to 30 seconds
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	return &Consumer{
		Config: config,
	}
}

// GetEntry retrieves a topic entry from the message store server
func (c *Consumer) GetEntry(topic string, offset int64) (ms.Entry, error) {

	// Create a custom HTTP client with a timeout
	httpClient := &http.Client{
		Timeout: c.Config.Timeout,
	}
	endpoint := fmt.Sprintf("http://%s:%d/consume?topic=%s&offset=%d", c.Config.Host, c.Config.Port, topic, offset)
	resp, err := httpClient.Get(endpoint)
	if err != nil {
		return ms.Entry{}, fmt.Errorf("error sending GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ms.Entry{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var topicEntry TopicEntry
	err = json.NewDecoder(resp.Body).Decode(&topicEntry)
	if err != nil {
		return ms.Entry{}, fmt.Errorf("error decoding JSON: %v", err)
	}

	key, err := base64.StdEncoding.DecodeString(topicEntry.Key)
	if err != nil {
		log.Fatal("error:", err)
		return ms.Entry{}, fmt.Errorf("error when base64 decode Key")
	}
	value, err := base64.StdEncoding.DecodeString(topicEntry.Value)
	if err != nil {
		log.Fatal("error:", err)
		return ms.Entry{}, fmt.Errorf("error when base64 decode Value")
	}

	entry := &ms.Entry{}
	entry.Key = []byte(key)
	entry.Value = []byte(value)
	entry.Timestamp = topicEntry.Timestamp

	return *entry, nil
}
