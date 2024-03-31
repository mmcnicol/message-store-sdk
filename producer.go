package messagestoresdk

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	ms "github.com/mmcnicol/message-store"
)

// TopicEntry represents a generic topic entry
type TopicEntry struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// ProducerConfig represents the configuration for the message producer
type ProducerConfig struct {
	Host    string        // Host of the message store server
	Port    int           // Port of the message store server
	Timeout time.Duration // Timeout for HTTP requests
}

// NewProducerConfig creates a new instance of ProducerConfig
func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{}
}

// Producer represents a message producer
type Producer struct {
	Config *ProducerConfig // Configuration for the message producer
}

// NewProducer creates a new instance of Producer
func NewProducer(config *ProducerConfig) *Producer {
	// Check if timeout is zero, and if so, default it to 30 seconds
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	return &Producer{
		Config: config,
	}
}

// SendEntry sends a topic entry to the message store server
func (p *Producer) SendEntry(topic string, entry ms.Entry) (int64, error) {

	key := base64.StdEncoding.EncodeToString(entry.Key)
	value := base64.StdEncoding.EncodeToString(entry.Value)

	topicEntry := &TopicEntry{}
	topicEntry.Key = key
	topicEntry.Value = value
	topicEntry.Timestamp = entry.Timestamp

	jsonData, err := json.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal JSON: %v", err)
	}

	// Construct the endpoint URL
	endpoint := fmt.Sprintf("http://%s:%d/produce?topic=%s", p.Config.Host, p.Config.Port, topic)

	// Create a custom HTTP client with a timeout
	httpClient := &http.Client{
		Timeout: p.Config.Timeout,
	}

	// perform HTTP request
	resp, err := httpClient.Post(endpoint, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return 0, fmt.Errorf("failed to send POST request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	offsetHeader := resp.Header.Get("x-offset")
	offset, err := strconv.ParseInt(offsetHeader, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse offset header: %v", err)
	}

	return offset, nil
}
