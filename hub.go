package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// wsMessage is one line pushed to the browser.
type wsMessage struct {
	Type    string `json:"type"` // "log" | "session" | "notice"
	TS      int64  `json:"ts,omitempty"`
	Stream  string `json:"stream,omitempty"`
	Message string `json:"message"`
}

type subscriber struct {
	hub     *Hub
	send    chan []byte
	dropped uint64 // lines dropped because this connection was too slow
}

// Hub owns a single StartLiveTail session for one log group (+ filter)
// and fans its events out to every subscriber watching it.
type Hub struct {
	mgr     *Manager
	key     string  // map key in the Manager
	groupID string  // log group name or ARN
	filter  *string // nil = no filter pattern

	mu        sync.Mutex
	subs      map[*subscriber]struct{}
	idleTimer *time.Timer
	closed    bool

	ctx    context.Context
	cancel context.CancelFunc
}

func (h *Hub) add(s *subscriber) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false // GC'd between lookup and add; the caller retries
	}
	if h.idleTimer != nil {
		h.idleTimer.Stop()
		h.idleTimer = nil
	}
	h.subs[s] = struct{}{}
	return true
}

func (h *Hub) remove(s *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[s]; !ok {
		return
	}
	delete(h.subs, s)
	close(s.send)
	if len(h.subs) == 0 && h.idleTimer == nil {
		// Nobody is watching: close the (billed) session after a grace period.
		h.idleTimer = time.AfterFunc(h.mgr.linger, func() { h.mgr.gc(h) })
	}
}

func (h *Hub) broadcast(msg []byte) {
	h.mu.Lock()
	for s := range h.subs {
		select {
		case s.send <- msg:
		default:
			atomic.AddUint64(&s.dropped, 1) // drop rather than stall the whole hub
		}
	}
	h.mu.Unlock()
}

// tailLoop keeps a Live Tail session open, reconnecting when it ends
// (the 3h session cap or transient errors) for as long as the hub lives.
func (h *Hub) tailLoop(client *cloudwatchlogs.Client) {
	const backoff = time.Second
	for h.ctx.Err() == nil {
		if err := h.runSession(client); err != nil && h.ctx.Err() == nil {
			log.Printf("[%s] live tail ended: %v (reconnecting)", h.groupID, err)
		}
		if h.ctx.Err() != nil {
			return
		}
		select {
		case <-h.ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

func (h *Hub) runSession(client *cloudwatchlogs.Client) error {
	out, err := client.StartLiveTail(h.ctx, &cloudwatchlogs.StartLiveTailInput{
		LogGroupIdentifiers:   []string{h.groupID},
		LogEventFilterPattern: h.filter,
	})
	if err != nil {
		return err // pre-stream error (e.g. missing group, no permission)
	}
	stream := out.GetStream()
	defer stream.Close()

	for {
		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		case ev, ok := <-stream.Events():
			if !ok || ev == nil {
				return stream.Err() // session ended (includes the 3h cap)
			}
			switch e := ev.(type) {
			case *types.StartLiveTailResponseStreamMemberSessionStart:
				h.emit(wsMessage{Type: "session", Message: "live tail started"})
			case *types.StartLiveTailResponseStreamMemberSessionUpdate:
				if md := e.Value.SessionMetadata; md != nil && md.Sampled {
					h.emit(wsMessage{Type: "notice", Message: "sampled: >500 events/sec"})
				}
				for _, le := range e.Value.SessionResults {
					h.emit(wsMessage{
						Type:    "log",
						TS:      aws.ToInt64(le.Timestamp),
						Stream:  aws.ToString(le.LogStreamName),
						Message: aws.ToString(le.Message),
					})
				}
			}
		}
	}
}

func (h *Hub) emit(m wsMessage) {
	if b, err := json.Marshal(m); err == nil {
		h.broadcast(b) // encode once, fan out to all subscribers
	}
}

// Manager maps a "log group (+ filter)" key to its Hub.
type Manager struct {
	client *cloudwatchlogs.Client
	linger time.Duration

	// Used to turn a bare log group name into an ARN, which StartLiveTail
	// requires. Empty account/region means names cannot be resolved.
	partition string
	region    string
	account   string

	mu   sync.Mutex
	hubs map[string]*Hub
}

func NewManager(c *cloudwatchlogs.Client, linger time.Duration, partition, region, account string) *Manager {
	return &Manager{
		client:    c,
		linger:    linger,
		partition: partition,
		region:    region,
		account:   account,
		hubs:      map[string]*Hub{},
	}
}

// resolveARN turns a bare log group name into the ARN that StartLiveTail
// requires. An ARN passed in is used as-is (minus any trailing ":*",
// which StartLiveTail rejects).
func (m *Manager) resolveARN(group string) string {
	if strings.HasPrefix(group, "arn:") {
		return strings.TrimSuffix(group, ":*")
	}
	return fmt.Sprintf("arn:%s:logs:%s:%s:log-group:%s",
		m.partition, m.region, m.account, group)
}

func (m *Manager) subscribe(groupID string, filter *string, s *subscriber) {
	key := groupID
	if filter != nil {
		key += "\x00" + *filter
	}
	for {
		h := m.getOrCreate(key, groupID, filter)
		s.hub = h
		if h.add(s) {
			return
		}
		// The hub was GC'd in the race window between lookup and add: retry.
	}
}

func (m *Manager) getOrCreate(key, groupID string, filter *string) *Hub {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h := m.hubs[key]; h != nil && !h.closed {
		return h
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		mgr:     m,
		key:     key,
		groupID: groupID,
		filter:  filter,
		subs:    map[*subscriber]struct{}{},
		ctx:     ctx,
		cancel:  cancel,
	}
	m.hubs[key] = h
	go h.tailLoop(m.client)
	return h
}

func (m *Manager) gc(h *Hub) {
	// Lock order is always Manager then Hub.
	m.mu.Lock()
	h.mu.Lock()
	if len(h.subs) == 0 && !h.closed {
		h.closed = true
		h.cancel() // ends the Live Tail session -> stops billing
		delete(m.hubs, h.key)
	}
	h.mu.Unlock()
	m.mu.Unlock()
}
