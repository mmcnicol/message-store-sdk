package messagestoresdk

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
