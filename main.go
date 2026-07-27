package main

import (
	"context"
	_ "embed"
	"flag"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/gorilla/websocket"
)

//go:embed web/terminal.html
var terminalHTML []byte

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = 50 * time.Second
	sendBuffer = 256
)

var upgrader = websocket.Upgrader{
	// Authorization / origin checks are handled outside this tool.
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Server struct {
	mgr *Manager
}

// ServeHTTP routes without net/http's ServeMux so that a "//" in a log
// group path (e.g. /tail//aws/lambda/foo) is not normalized away.
func (srv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/ws/"):
		srv.handleWS(w, r)
	case strings.HasPrefix(r.URL.Path, "/tail/"):
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(terminalHTML)
	default:
		http.NotFound(w, r)
	}
}

// handleWS upgrades the connection and attaches it to the hub for the
// log group named by everything after "/ws/".
func (srv *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	group := strings.TrimPrefix(r.URL.Path, "/ws/")
	if group == "" {
		http.Error(w, "log group required", http.StatusBadRequest)
		return
	}
	var filter *string
	if f := r.URL.Query().Get("filter"); f != "" {
		filter = aws.String(f)
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s := &subscriber{send: make(chan []byte, sendBuffer)}
	srv.mgr.subscribe(group, filter, s)
	go s.writePump(conn)
	s.readPump(conn)
}

// readPump blocks until the client disconnects, keeping the read
// deadline fresh via pong replies. It receives no application messages.
func (s *subscriber) readPump(conn *websocket.Conn) {
	defer func() {
		s.hub.remove(s)
		conn.Close()
	}()
	conn.SetReadLimit(512)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (s *subscriber) writePump(conn *websocket.Conn) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		conn.Close()
	}()
	for {
		select {
		case msg, ok := <-s.send:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok { // hub closed the channel
				conn.WriteMessage(websocket.CloseMessage, nil)
				return
			}
			if d := atomic.SwapUint64(&s.dropped, 0); d > 0 {
				notice := []byte(`{"type":"notice","message":"dropped ` +
					strconv.FormatUint(d, 10) + ` lines (slow connection)"}`)
				if err := conn.WriteMessage(websocket.TextMessage, notice); err != nil {
					return
				}
			}
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	linger := flag.Duration("linger", 15*time.Second,
		"keep a Live Tail session open this long after the last viewer leaves")
	flag.Parse()

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}
	srv := &Server{mgr: NewManager(cloudwatchlogs.NewFromConfig(cfg), *linger)}

	log.Printf("clterm listening on %s", *addr)
	log.Printf("open  http://localhost%s/tail/<log-group>   (e.g. /tail//aws/lambda/your-fn)", *addr)
	log.Fatal(http.ListenAndServe(*addr, srv))
}
