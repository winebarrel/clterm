package main

import (
	"bytes"
	"context"
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/gorilla/websocket"
)

// version is overwritten at release time via -ldflags "-X main.version=...".
var version = "dev"

//go:embed web/terminal.html
var terminalHTML []byte

// Vendored xterm.js assets, served from /vendor/ so the page has no
// external (CDN) dependencies.
//
//go:embed web/vendor
var vendorFS embed.FS

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
	mgr     *Manager
	static  http.Handler // serves the embedded /vendor/ assets
	html    []byte       // terminal.html with the WebSocket path substituted
	wsRoute string       // the WebSocket endpoint path ("/ws" or "/ws/<value>")
}

// ServeHTTP routes the endpoints. The log group is passed as the ?group=
// query parameter, so no path parsing is involved.
func (srv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == srv.wsRoute:
		srv.handleWS(w, r)
	case r.URL.Path == "/tail":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(srv.html) //nolint:errcheck
	case strings.HasPrefix(r.URL.Path, "/vendor/"):
		srv.static.ServeHTTP(w, r)
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
	region := r.URL.Query().Get("region") // optional; blank = default region
	// filter is a raw CloudWatch Logs filter pattern; q is a plain string
	// matched literally (auto-quoted). filter wins if both are given.
	var filter *string
	if f := r.URL.Query().Get("filter"); f != "" {
		filter = aws.String(f)
	} else if q := r.URL.Query().Get("q"); q != "" {
		filter = aws.String(literalFilter(q))
	}
	var since time.Duration // optional; >0 = replay recent history first
	if v := r.URL.Query().Get("since"); v != "" {
		d, err := parseSince(v)
		if err != nil || d <= 0 {
			http.Error(w, "invalid since (use e.g. 5m, 1h, 1d, 1w)", http.StatusBadRequest)
			return
		}
		since = d
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s := &subscriber{send: make(chan []byte, sendBuffer), done: make(chan struct{})}
	go s.writePump(conn)
	// Replay recent history (if requested) before attaching to the live
	// tail, so past events appear ahead of new ones.
	if since > 0 {
		srv.mgr.backfill(r.Context(), group, region, filter, since, s)
	}
	// StartLiveTail needs an ARN; accept a bare name (resolved with the
	// requested or default region) or a full ARN.
	srv.mgr.subscribe(group, region, filter, s)
	s.readPump(conn)
}

// readPump blocks until the client disconnects, keeping the read
// deadline fresh via pong replies. It receives no application messages.
func (s *subscriber) readPump(conn *websocket.Conn) {
	defer func() {
		s.hub.remove(s)
		conn.Close() //nolint:errcheck
	}()
	conn.SetReadLimit(512)
	conn.SetReadDeadline(time.Now().Add(pongWait)) //nolint:errcheck
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
		conn.Close()  //nolint:errcheck
		close(s.done) // let a running backfill know the connection is gone
	}()
	for {
		select {
		case msg, ok := <-s.send:
			conn.SetWriteDeadline(time.Now().Add(writeWait)) //nolint:errcheck
			// hub closed the channel
			if !ok {
				conn.WriteMessage(websocket.CloseMessage, nil) //nolint:errcheck
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
			conn.SetWriteDeadline(time.Now().Add(writeWait)) //nolint:errcheck
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
	wsPath := flag.String("ws-path", "",
		"extra path segment for the WebSocket endpoint (e.g. -ws-path foo serves /ws/foo); default is /ws")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

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

	vendor, err := fs.Sub(vendorFS, "web")
	if err != nil {
		log.Fatalf("embed vendor assets: %v", err)
	}
	// The /tail page connects to the WebSocket endpoint, so bake the same
	// path into the served HTML.
	wsRoute := "/ws"
	if seg := strings.Trim(*wsPath, "/"); seg != "" {
		wsRoute = "/ws/" + seg
	}
	html := bytes.ReplaceAll(terminalHTML, []byte("__WS_BASE__"), []byte(wsRoute))
	srv := &Server{
		mgr:     NewManager(cfg, *linger, partition, cfg.Region, account),
		static:  http.FileServerFS(vendor),
		html:    html,
		wsRoute: wsRoute,
	}

	log.Printf("clterm listening on %s (region %s, account %s)", *addr, cfg.Region, account)
	log.Printf("open  http://localhost%s/tail?group=<log-group>   (e.g. ?group=/aws/lambda/your-fn)", *addr)
	log.Fatal(http.ListenAndServe(*addr, srv))
}

// literalFilter turns a plain search string into a CloudWatch Logs filter
// pattern that matches it verbatim, by quoting it and escaping the backslash
// and double-quote the quoted form treats specially. So q=aaaa- becomes
// "aaaa-" (a literal) instead of tripping the "-" exclusion operator.
func literalFilter(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	return `"` + v + `"`
}

// dayWeekPrefix matches a leading "<n>d" or "<n>w" segment, which Go's
// time.ParseDuration does not understand.
var dayWeekPrefix = regexp.MustCompile(`^(\d+)([dw])`)

// parseSince is time.ParseDuration extended with day (d) and week (w) units,
// so windows like "1d" or "1w3d12h" work. Leading day/week segments are peeled
// off and the remainder is handed to time.ParseDuration.
func parseSince(v string) (time.Duration, error) {
	var extra time.Duration
	for {
		m := dayWeekPrefix.FindStringSubmatch(v)
		if m == nil {
			break
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, err
		}
		unit := 24 * time.Hour
		if m[2] == "w" {
			unit = 7 * 24 * time.Hour
		}
		extra += time.Duration(n) * unit
		v = v[len(m[0]):]
	}
	if v == "" {
		return extra, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, err
	}
	return extra + d, nil
}

// partitionFromARN extracts the partition (aws, aws-cn, aws-us-gov) from
// an ARN, defaulting to "aws".
func partitionFromARN(arn string) string {
	if parts := strings.SplitN(arn, ":", 3); len(parts) >= 2 && parts[1] != "" {
		return parts[1]
	}
	return "aws"
}
