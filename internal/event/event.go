package event

import (
	"errors"
	"strconv"
	"strings"
)

const (
	Follow         = 70
	Unfollow       = 85
	Broadcast      = 66
	PrivateMsg     = 80
	StatusUpdate   = 83
	ServerShutdown = -1
)

var (
	ShutdownEvent = &Event{MsgType: ServerShutdown}

	badEventError = errors.New("Event contains less then 2 fields")
)

type Event struct {
	Raw        string
	Number     uint64
	MsgType    int
	FromUserID uint64
	ToUserID   uint64
}

func Equal(a, b *Event) bool {
	if a.Raw == b.Raw &&
		a.Number == b.Number &&
		a.MsgType == b.MsgType &&
		a.FromUserID == b.FromUserID &&
		a.ToUserID == b.ToUserID {
		return true
	}
	return false
}

func NewEvent(raw string) (*Event, error) {
	var fromUserID, toUserID uint64
	var parsed []string = strings.Split(raw, "|")
	var cleaned []string
	// defer log.Println("DEBUG: parsed event: ", raw, "; ", cleaned, fromUserID, toUserID)
	for _, p := range parsed {
		if len(p) > 0 {
			cleaned = append(cleaned, p)
		}
	}
	if len(cleaned) < 2 {
		return nil, badEventError
	}
	number, err := strconv.ParseUint(cleaned[0], 10, 64)
	if err != nil {
		return nil, err
	}
	if len(parsed) > 2 {
		fromUserID, err = strconv.ParseUint(cleaned[2], 10, 64)
	}
	if len(parsed) > 3 {
		toUserID, err = strconv.ParseUint(cleaned[3], 10, 64)
	}
	if err != nil {
		return nil, err
	}
	return &Event{
		Raw:        raw,
		Number:     number,
		MsgType:    int(cleaned[1][0]),
		FromUserID: fromUserID,
		ToUserID:   toUserID,
	}, nil
}
