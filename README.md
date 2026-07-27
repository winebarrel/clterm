# clterm

[![CI](https://github.com/winebarrel/clterm/actions/workflows/ci.yml/badge.svg)](https://github.com/winebarrel/clterm/actions/workflows/ci.yml)
[![AI Generated](https://img.shields.io/badge/AI%20Generated-Claude-orange?logo=anthropic)](https://claude.ai/claude-code)

Real-time, `tail -f`-style viewer for **Amazon CloudWatch Logs**, rendered in a
browser terminal. Backed by the CloudWatch Logs
[Live Tail](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CloudWatchLogs_LiveTail.html)
(`StartLiveTail`) API, with fan-out so multiple people can watch the same log
group over a single session.

![](https://github.com/user-attachments/assets/07bc0cc5-1171-4041-801e-020fe7bf3988)

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

# then open
open 'http://localhost:8080/tail?group=/aws/lambda/your-fn'
```

Flags:

| flag | default | meaning |
|------|---------|---------|
| `-addr`   | `:8080` | listen address |
| `-region` | *(from config)* | AWS region; overrides `AWS_REGION` |
| `-linger` | `15s`   | keep a session open this long after the last viewer leaves |

Standard AWS credential and region resolution applies (`AWS_PROFILE`,
`AWS_REGION`, env vars, SSO, instance role, ...) via the default config chain.
`-region` overrides the resolved region. A region must be set one way or the
other, or startup fails.

## Docker

Multi-arch images (amd64/arm64) are published to
`ghcr.io/winebarrel/clterm` on each release.

```sh
docker run --rm -p 8080:8080 \
  -e AWS_REGION=ap-northeast-1 \
  -v ~/.aws:/home/nonroot/.aws:ro -e AWS_PROFILE=your-profile \
  ghcr.io/winebarrel/clterm:latest

# then open http://localhost:8080/tail?group=/aws/lambda/your-fn
```

Supply credentials the usual container ways: env vars
(`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`), a
mounted `~/.aws`, or an instance/task role (EC2/ECS/EKS) with no extra config.

## URL scheme

```
/tail?group=<log-group>                    the terminal page
/ws?group=<log-group>                      the WebSocket stream
/ws?group=<log-group>&region=<region>      tail a group in another region
/ws?group=<log-group>&filter=...           optional Live Tail filter pattern
```

The log group is passed as the `group` query parameter, e.g.
`/tail?group=/aws/lambda/foo`. `StartLiveTail` requires an ARN, so a bare name
is resolved to `arn:<partition>:logs:<region>:<account>:log-group:<name>` using
the caller's account and — unless `region` is given — the configured region.
Add `region=<region>` to tail a group in another region; a per-region client is
created and cached automatically. To tail a group in **another account**, pass
its full ARN as `group` instead (the ARN's own region is used).

## IAM

The process needs:

```json
[
  { "Effect": "Allow", "Action": "logs:StartLiveTail", "Resource": "*" },
  { "Effect": "Allow", "Action": "sts:GetCallerIdentity", "Resource": "*" }
]
```

`sts:GetCallerIdentity` (called once at startup) is used to build log group
ARNs from bare names. Scope the `logs:StartLiveTail` `Resource` to the log
group ARNs you want to expose.

## Notes / limits

- Live Tail samples events above ~500/sec; a notice is shown when that happens.
- On reconnect (3h cap or transient errors) a few events around the gap may be
  missed — `tail -f` semantics, not a gap-free guarantee.
- Slow WebSocket clients have their backlog dropped (with a count) rather than
  stalling the shared hub.
- **No authorization** is built in: anyone who can reach the server can tail any
  group the process's IAM role allows. Put it behind your own authz.
- The frontend (xterm.js) is vendored under `web/vendor/` and embedded into
  the binary, so there is no CDN dependency — it runs fully offline / in
  closed networks. Those assets are MIT-licensed (see `web/vendor/xterm.LICENSE`).
