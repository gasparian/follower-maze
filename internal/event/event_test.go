package event

import (
	"testing"
)

func TestNewEvent(t *testing.T) {
	msgOrig := "666|F|60|50"
	parsed := &Event{
		Raw:        msgOrig,
		Number:     666,
		MsgType:    Follow,
		FromUserID: 60,
		ToUserID:   50,
	}
	newEvent, _ := NewEvent(msgOrig)
	if !Equal(parsed, newEvent) {
		t.Errorf("Expected: %v, but got: %v", parsed, newEvent)
	}
	msgBad := "666|"
	_, err := NewEvent(msgBad)
	if err == nil {
		t.Error("Should not parse event with just a single field")
	}
}
