package messagestoresdk

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	ms "github.com/mmcnicol/message-store"
	"github.com/stretchr/testify/assert"
)

func TestSendEntry_Success(t *testing.T) {
	// Mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate successful response with offset header
		w.Header().Set("x-offset", "123")
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	//fmt.Println(server.URL)
	host, portString, err := extractHostAndPort(server.URL)
	if err != nil {
		fmt.Println("Error:", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		fmt.Println("Error:", err)
	}

	// Create a Producer with mock configuration
	config := NewProducerConfig()
	config.Host = host
	config.Port = port

	producer := NewProducer(config)

	// Create a mock entry
	entry := ms.Entry{
		Key:       []byte("key"),
		Value:     []byte("value"),
		Timestamp: time.Now(),
	}

	// Send the entry
	offset, err := producer.SendEntry("topic1", entry)

	// Verify the result
	assert.NoError(t, err)
	assert.Equal(t, int64(123), offset)
}

func extractHostAndPort(urlString string) (string, string, error) {
	// Parse the URL
	parsedURL, err := url.Parse(urlString)
	if err != nil {
		return "", "", err
	}

	// Extract host and port
	host := parsedURL.Hostname()
	port := parsedURL.Port()

	return host, port, nil
}

func TestSendEntry_Error(t *testing.T) {
	// Create a Producer with mock configuration
	config := NewProducerConfig()
	config.Host = "invalid-url" // Use an invalid URL
	//config.Port = 80            // Use any available port

	producer := NewProducer(config)

	// Create a mock entry
	entry := ms.Entry{
		Key:       []byte("key"),
		Value:     []byte("value"),
		Timestamp: time.Now(),
	}

	// Send the entry
	_, err := producer.SendEntry("topic1", entry)

	// Verify the error
	assert.Error(t, err)
}

/*
import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	ms "github.com/mmcnicol/message-store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockHTTPClient is a mock for http.Client
type MockHTTPClient struct {
	mock.Mock
}

// Do mocks the Do method of http.Client
func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	args := m.Called(req)
	return args.Get(0).(*http.Response), args.Error(1)
}

func TestSendEntry(t *testing.T) {
	config := &ProducerConfig{
		Host: "localhost",
		Port: 8080,
	}

	mockClient := new(MockHTTPClient)

	topic := "test"
	entry := ms.Entry{
		Key:       []byte("key"),
		Value:     []byte("value"),
		Timestamp: time.Now(),
	}

	// Mocking the POST request and response
	mockResponse := &http.Response{
		StatusCode: http.StatusCreated,
		Header:     http.Header{"x-offset": []string{"123"}},
		Body:       ioutil.NopCloser(bytes.NewBufferString("")),
	}
	mockClient.On("Do", mock.Anything).Return(mockResponse, nil)

	p := &Producer{
		Config: config,
	}

	// Injecting the mock client into the Producer
	p.httpClient = mockClient

	offset, err := p.SendEntry(topic, entry)
	assert.NoError(t, err)
	assert.Equal(t, int64(123), offset)

	mockClient.AssertExpectations(t)
}
*/

/*
import (
	"testing"

	ms "github.com/mmcnicol/message-store"
)

func TestSendEntry(t *testing.T) {

	producerConfig := NewProducerConfig()
	producerConfig.Host = "localhost"
	producerConfig.Port = 8080

	producer := NewProducer(producerConfig)
	topic := "topic1"

	entry1 := &ms.Entry{
		Key:   nil,
		Value: []byte("test1"),
	}
	offset, err := producer.SendEntry(topic, *entry1)
	if err != nil {
		t.Fatalf("SendEntry(), err: %+v", err)
	}
	if offset != 0 {
		t.Fatalf("SendEntry(), got:%d, want: %d", offset, 0)
	}

	entry2 := &ms.Entry{
		Key:   nil,
		Value: []byte("test2"),
	}
	offset, err = producer.SendEntry(topic, *entry2)
	if err != nil {
		t.Fatalf("SendEntry(), err: %+v", err)
	}
	if offset != 1 {
		t.Fatalf("SendEntry(), got:%d, want: %d", offset, 1)
	}
}

*/
