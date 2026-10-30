# The lotse image (ADR 0049): a static musl build of the `lotse` binary on
# distroless, no shell, running as a non-root user. Build from the
# repository root:
#
#   docker build -f packaging/Dockerfile -t lotse .
#
# Run one node; the cluster file and certificates are mounted read-only,
# the data directory is a volume:
#
#   docker run -e LOTSE_NODE=2 \
#     -v /etc/lotse:/etc/lotse:ro -v lotse-2:/var/lib/lotse \
#     -p 7000:7000 -p 9000:9000 lotse
#
# The cluster file's data_dir must be /var/lib/lotse (or a path below it).

FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
# Alpine's Rust targets musl and links statically by default.
RUN cargo build --release --locked -p lotse-node \
    && install -D -m 0755 target/release/lotse /out/lotse \
    && install -d -o 65532 -g 65532 /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/lotse /usr/bin/lotse
COPY --from=build --chown=65532:65532 /out/data /var/lib/lotse
ENV LOTSE_CONFIG=/etc/lotse/cluster.toml
VOLUME ["/var/lib/lotse"]
# 7000 for the other nodes, 9000 for clients, as in the example cluster file.
EXPOSE 7000 9000
USER 65532:65532
ENTRYPOINT ["/usr/bin/lotse"]
CMD ["run"]
