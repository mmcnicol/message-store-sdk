package messagestoresdk

import (
	"testing"

	ms "github.com/mmcnicol/message-store"
)

func TestGetEntry(t *testing.T) {

	consumerConfig := NewConsumerConfig()
	consumerConfig.Host = "localhost"
	consumerConfig.Port = 8080

	consumer := NewConsumer(consumerConfig)
	topic := "topic1"

	entry1 := &ms.Entry{
		Key:   nil,
		Value: []byte("test1"),
	}
	got, err := consumer.GetEntry(topic, 0)
	if err != nil {
		t.Fatalf("GetEntry(), err: %+v", err)
	}
	/*
		if got == nil {
			t.Fatalf("GetEntry(), got:%+v, want: %+v", got, entry1)
		}
	*/
	if string(got.Key) != string(entry1.Key) {
		t.Fatalf("GetEntry() Key, got:%s, want:%s", string(got.Key), string(entry1.Key))
	}
	if string(got.Value) != string(entry1.Value) {
		t.Fatalf("GetEntry() Value, got:%s, want:%s", string(got.Value), string(entry1.Value))
	}

	entry2 := &ms.Entry{
		Key:   nil,
		Value: []byte("test2"),
	}
	got, err = consumer.GetEntry(topic, 1)
	if err != nil {
		t.Fatalf("GetEntry(), err: %+v", err)
	}
	/*
		if got == nil {
			t.Fatalf("GetEntry(), got:%+v, want: %+v", got, entry1)
		}
	*/
	if string(got.Key) != string(entry2.Key) {
		t.Fatalf("GetEntry() Key, got:%s, want:%s", string(got.Key), string(entry2.Key))
	}
	if string(got.Value) != string(entry2.Value) {
		t.Fatalf("GetEntry() Value, got:%s, want:%s", string(got.Value), string(entry2.Value))
	}
}
