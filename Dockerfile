# The binary is cross-built by goreleaser and copied in; nothing is
# compiled here. distroless/static ships CA certs (for AWS TLS) and tzdata.
FROM gcr.io/distroless/static:nonroot
COPY clterm /clterm
EXPOSE 8080
ENTRYPOINT ["/clterm"]
