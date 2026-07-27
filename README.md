# clterm

Real-time, `tail -f`-style viewer for **Amazon CloudWatch Logs**, rendered in a
browser terminal. Backed by the CloudWatch Logs
[Live Tail](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CloudWatchLogs_LiveTail.html)
(`StartLiveTail`) API, with fan-out so multiple people can watch the same log
group over a single session.

## How it works

```
browser (xterm.js) --WebSocket--> clterm --StartLiveTail--> CloudWatch Logs
                                    hub: one session per log group,
                                    fanned out to every viewer
```

- **One Live Tail session per log group**, shared across all viewers (Live Tail
  is billed per minute, so we never open one session per viewer).
- Sessions are opened lazily on the first viewer and closed shortly after the
  last one leaves (`-linger`), so idle groups stop billing.
- The 3-hour Live Tail session cap is handled by automatic reconnection.

## Run

```sh
go run . # listens on :8080

# then open (note the double slash: the log group name starts with "/")
open 'http://localhost:8080/tail//aws/lambda/your-fn'
```

Flags:

| flag | default | meaning |
|------|---------|---------|
| `-addr`   | `:8080` | listen address |
| `-linger` | `15s`   | keep a session open this long after the last viewer leaves |

Standard AWS credential resolution applies (`AWS_PROFILE`, env vars, SSO,
instance role, ...) via the default config chain.

## URL scheme

```
/tail/<log-group>          the terminal page
/ws/<log-group>            the WebSocket stream
/ws/<log-group>?filter=... optional Live Tail filter pattern
```

Everything after `/tail/` (or `/ws/`) is taken verbatim as the log group name.
Because most groups start with `/`, the URL usually contains a double slash,
e.g. `/tail//aws/lambda/foo` -> group `/aws/lambda/foo`. Routing deliberately
avoids `net/http`'s `ServeMux` so the `//` is not normalized away. An ARN also
works in place of the name.

## IAM

The process needs:

```json
{ "Effect": "Allow", "Action": "logs:StartLiveTail", "Resource": "*" }
```

Scope `Resource` to the log group ARNs you want to expose.

## Notes / limits

- Live Tail samples events above ~500/sec; a notice is shown when that happens.
- On reconnect (3h cap or transient errors) a few events around the gap may be
  missed — `tail -f` semantics, not a gap-free guarantee.
- Slow WebSocket clients have their backlog dropped (with a count) rather than
  stalling the shared hub.
- **No authorization** is built in: anyone who can reach the server can tail any
  group the process's IAM role allows. Put it behind your own authz.
- The frontend loads xterm.js from a CDN. To ship a single self-contained
  binary, vendor `xterm.min.js` / `xterm.min.css` / `addon-fit.min.js` into
  `web/` and embed them too.