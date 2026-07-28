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
	done    chan struct{} // closed by writePump when the connection is gone
	dropped uint64        // lines dropped because this connection was too slow
}

// Hub owns a single StartLiveTail session for one log group (+ filter)
// and fans its events out to every subscriber watching it.
type Hub struct {
	mgr     *Manager
	client  *cloudwatchlogs.Client // regional client for this group
	key     string                 // map key in the Manager
	groupID string                 // log group ARN
	filter  *string                // nil = no filter pattern

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
func (h *Hub) tailLoop() {
	const backoff = time.Second
	for h.ctx.Err() == nil {
		if err := h.runSession(); err != nil && h.ctx.Err() == nil {
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

func (h *Hub) runSession() error {
	out, err := h.client.StartLiveTail(h.ctx, &cloudwatchlogs.StartLiveTailInput{
		LogGroupIdentifiers:   []string{h.groupID},
		LogEventFilterPattern: h.filter,
	})
	if err != nil {
		return err // pre-stream error (e.g. missing group, no permission)
	}
	stream := out.GetStream()
	defer stream.Close() //nolint:errcheck

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

// Manager maps a "log group (+ filter)" key to its Hub and hands out a
// CloudWatch Logs client per region.
type Manager struct {
	cfg    aws.Config
	linger time.Duration

	// Used to turn a bare log group name into an ARN, which StartLiveTail
	// requires.
	partition string
	region    string // default region when a request does not specify one
	account   string

	mu      sync.Mutex
	hubs    map[string]*Hub
	clients map[string]*cloudwatchlogs.Client
}

func NewManager(cfg aws.Config, linger time.Duration, partition, region, account string) *Manager {
	return &Manager{
		cfg:       cfg,
		linger:    linger,
		partition: partition,
		region:    region,
		account:   account,
		hubs:      map[string]*Hub{},
		clients:   map[string]*cloudwatchlogs.Client{},
	}
}

// resolve turns a name-or-ARN plus a requested region into the log group
// ARN StartLiveTail needs and the region whose client must serve it. A
// bare name uses reqRegion (or the default); an ARN carries its own region.
func (m *Manager) resolve(group, reqRegion string) (arn, region string) {
	if strings.HasPrefix(group, "arn:") {
		arn = strings.TrimSuffix(group, ":*")
		if region = arnField(arn, 3); region == "" {
			region = m.region
		}
		return arn, region
	}
	if region = reqRegion; region == "" {
		region = m.region
	}
	arn = fmt.Sprintf("arn:%s:logs:%s:%s:log-group:%s", m.partition, region, m.account, group)
	return arn, region
}

func (m *Manager) subscribe(group, reqRegion string, filter *string, s *subscriber) {
	arn, region := m.resolve(group, reqRegion)
	key := arn
	if filter != nil {
		key += "\x00" + *filter
	}
	for {
		h := m.getOrCreate(key, arn, region, filter)
		s.hub = h
		if h.add(s) {
			return
		}
		// The hub was GC'd in the race window between lookup and add: retry.
	}
}

// backfillMax caps how many past events a single connection replays, so a
// wide -since window over a busy group cannot flood one viewer unbounded.
const backfillMax = 10000

// backfill replays past events over [now-since, now] to a single subscriber
// before it attaches to the live tail, so a viewer sees recent history first.
// It blocks on the subscriber's send channel (no dropping) and stops early if
// the connection closes or the cap is reached.
func (m *Manager) backfill(ctx context.Context, group, reqRegion string, filter *string, since time.Duration, s *subscriber) {
	arn, region := m.resolve(group, reqRegion)
	m.mu.Lock()
	client := m.clientForLocked(region)
	m.mu.Unlock()

	now := time.Now()
	in := &cloudwatchlogs.FilterLogEventsInput{
		LogGroupIdentifier: aws.String(arn),
		StartTime:          aws.Int64(now.Add(-since).UnixMilli()),
		EndTime:            aws.Int64(now.UnixMilli()),
		FilterPattern:      filter,
	}
	sent := 0
	pager := cloudwatchlogs.NewFilterLogEventsPaginator(client, in)
	for pager.HasMorePages() {
		out, err := pager.NextPage(ctx)
		if err != nil {
			s.push(wsMessage{Type: "notice", Message: "history unavailable: " + err.Error()})
			return
		}
		for _, ev := range out.Events {
			if sent >= backfillMax {
				s.push(wsMessage{Type: "notice",
					Message: fmt.Sprintf("history truncated at %d events", backfillMax)})
				return
			}
			if !s.push(wsMessage{
				Type:    "log",
				TS:      aws.ToInt64(ev.Timestamp),
				Stream:  aws.ToString(ev.LogStreamName),
				Message: aws.ToString(ev.Message),
			}) {
				return // connection closed
			}
			sent++
		}
	}
	if sent > 0 {
		s.push(wsMessage{Type: "session", Message: fmt.Sprintf("replayed %d past events", sent)})
	}
}

// push blocks until the message is queued for this subscriber, or the
// connection is gone. It returns false once the connection has closed.
func (s *subscriber) push(m wsMessage) bool {
	b, err := json.Marshal(m)
	if err != nil {
		return true // skip this one, keep going
	}
	select {
	case s.send <- b:
		return true
	case <-s.done:
		return false
	}
}

func (m *Manager) getOrCreate(key, arn, region string, filter *string) *Hub {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h := m.hubs[key]; h != nil && !h.closed {
		return h
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		mgr:     m,
		client:  m.clientForLocked(region),
		key:     key,
		groupID: arn,
		filter:  filter,
		subs:    map[*subscriber]struct{}{},
		ctx:     ctx,
		cancel:  cancel,
	}
	m.hubs[key] = h
	go h.tailLoop()
	return h
}

// clientForLocked returns a CloudWatch Logs client for the region,
// creating and caching it on first use. Call with m.mu held.
func (m *Manager) clientForLocked(region string) *cloudwatchlogs.Client {
	if c := m.clients[region]; c != nil {
		return c
	}
	c := cloudwatchlogs.NewFromConfig(m.cfg, func(o *cloudwatchlogs.Options) {
		o.Region = region
	})
	m.clients[region] = c
	return c
}

// arnField returns the i-th colon-separated field of an ARN, or "".
func arnField(arn string, i int) string {
	if parts := strings.Split(arn, ":"); i < len(parts) {
		return parts[i]
	}
	return ""
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
