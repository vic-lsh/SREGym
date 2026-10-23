# Astronomy Shop source deployment

The source adapter builds OpenTelemetry Demo 2.2.0 from the pinned
`source_apps/astronomy_shop` submodule. The independently pinned
`SREGym-applications/astronomy-shop` submodule contains its Helm chart
(`7d7b074714345a0c282b0be75af7a2c504b44c95`), rather than application source.
Initialize both before use:

```bash
git submodule update --init source_apps/astronomy_shop SREGym-applications
git -C SREGym-applications submodule update --init astronomy-shop
```

`astronomy_shop` is supported by the normal application-workspace API.
Unseeded workspaces copy the pinned application source and start a fresh Git
repository. Seeded workspaces retain their own accepted repository; source
builds honor `SREGYM_APP_SOURCE_DIR`. Chart resolution continues to use the
harness chart, since upstream application source does not contain that chart.

Fresh evaluation workspaces add `NU1901`–`NU1904` to .NET's
`WarningsNotAsErrors` for accounting and cart before the initial Git snapshot.
Auditing stays enabled and findings remain visible; the historical dependency
versions remain fixed. This prevents later advisory-database updates from
silently making the 2.2.0 comparison unbuildable. Seeded repositories are never
modified; prepare them explicitly with `prepare_astronomy_source` before
accepting their source if this compatibility policy is required.

Set `SREGYM_DEPLOY_FROM_SOURCE=1` and the existing
`SREGYM_KIND_CLUSTER_NAME` to enable source builds through AstronomyShop.deploy.
The adapter builds all 20 demo images, including the mock LLM, Kafka and the
flagd-ui sidecar, then loads them into that kind cluster. It uses upstream's
component Dockerfiles and pinned Java/C++ instrumentation build arguments.
`SREGYM_DOCKER_BUILDER` selects an existing buildx builder; the existing opt-in
`SREGYM_SOURCE_BUILD_CACHE` caches unchanged contexts.

The chart's `default.image.tag` changes to the source-build tag. Its component
suffix rule selects each locally built image without replacing chart lists or
container configuration. Namespace, service names, feature flags, ports,
Kafka/Valkey/PostgreSQL, load generator, and observability topology are retained.
The normal deploy settings still disable bundled Prometheus and browser traffic.
Infrastructure and helper images retain their original chart pins.

This adapter supplies local build and deployment plumbing. SDO separately
verifies the rollout, records image evidence through its commit broker, and
publishes the rendered lock and app-specific lifecycle health memory. No model
call, benchmark fault capability change, or verdict bypass is introduced.
