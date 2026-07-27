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
	"github.com/aws/aws-sdk-go-v2/service/sts"
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

// ServeHTTP routes the two endpoints. The log group is passed as the
// ?group= query parameter, so no path parsing is involved.
func (srv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/ws":
		srv.handleWS(w, r)
	case "/tail":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(terminalHTML)
	default:
		http.NotFound(w, r)
	}
}

// handleWS upgrades the connection and attaches it to the hub for the
// log group named by the ?group= query parameter.
func (srv *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	group := r.URL.Query().Get("group")
	if group == "" {
		http.Error(w, "group required", http.StatusBadRequest)
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
	// StartLiveTail needs an ARN; accept a bare name or an ARN.
	srv.mgr.subscribe(srv.mgr.resolveARN(group), filter, s)
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
	region := flag.String("region", "", "AWS region (default: from AWS config / AWS_REGION)")
	linger := flag.Duration("linger", 15*time.Second,
		"keep a Live Tail session open this long after the last viewer leaves")
	flag.Parse()

	var opts []func(*config.LoadOptions) error
	if *region != "" {
		opts = append(opts, config.WithRegion(*region))
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}
	if cfg.Region == "" {
		log.Fatal("no AWS region configured: pass -region or set AWS_REGION")
	}

	// StartLiveTail needs log group ARNs, so resolve the account and
	// partition once to build them from bare names.
	id, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.TODO(), &sts.GetCallerIdentityInput{})
	if err != nil {
		log.Fatalf("get caller identity (needed to build log group ARNs): %v", err)
	}
	account := aws.ToString(id.Account)
	partition := partitionFromARN(aws.ToString(id.Arn))

	srv := &Server{mgr: NewManager(
		cloudwatchlogs.NewFromConfig(cfg), *linger, partition, cfg.Region, account)}

	log.Printf("clterm listening on %s (region %s, account %s)", *addr, cfg.Region, account)
	log.Printf("open  http://localhost%s/tail?group=<log-group>   (e.g. ?group=/aws/lambda/your-fn)", *addr)
	log.Fatal(http.ListenAndServe(*addr, srv))
}

// partitionFromARN extracts the partition (aws, aws-cn, aws-us-gov) from
// an ARN, defaulting to "aws".
func partitionFromARN(arn string) string {
	if parts := strings.SplitN(arn, ":", 3); len(parts) >= 2 && parts[1] != "" {
		return parts[1]
	}
	return "aws"
}
