# Generic source-build adapter (opt in)

The version-1 build declaration is the source of truth. Select an SDO app's
`apps/<app>/build.yaml` with `SREGYM_APP_BUILD_DECLARATION` and enable
`SREGYM_DEPLOY_FROM_SOURCE=1`. Unknown/mismatched apps fail explicitly. Without
that variable, the existing per-app adapters retain their behavior.

The declaration pins source/chart revisions and specifies images (repository,
context, Dockerfile, optional ARM Dockerfile, args, target and tag suffix), the
namespace, deployment source, and Helm image/tag keys. Contexts and Dockerfiles
are relative to the selected source root; symlink escapes fail. The source path
is relative to the declaration; prepared workspaces use `SREGYM_APP_SOURCE_DIR`.
The shared tag rule remains `sregym-src-<app>-<cluster>`.

`SREGYM_SOURCE_BUILD_JOBS` accepts 1–4, default 1; available host CPU/load can
reduce the pool. The generic cache fingerprints source contents, Dockerfile,
arguments, target, and pinned download inputs. Docker/BuildKit also shares its
normal layer cache. Set `SREGYM_SOURCE_BUILD_CACHE=1` to reuse identical local
images and avoid reloading images already present on all kind nodes.

`downloads` on an image declares `{url, sha256, replace?}` archive inputs. The
verified bytes live in `SREGYM_DOWNLOAD_CACHE` (default
`~/.cache/sregym/downloads`), under SHA-256(URL + newline + expected SHA-256).
Publication is atomic and guarded by a process lock; hits are verified again.
An unavailable origin fails only on a cold cache, with URL/checksum/path context.
A corrupt cache fails explicitly. To preseed a shared cache, call
`DownloadCache.fetch(Download(...))` while the mirror is reachable.

For these declared archives the adapter uses a named BuildKit context and a
temporary Dockerfile: `curl -fSL URL -o FILE` and `wget URL` are replaced with
local copies. `replace` matches the upstream literal URL, including `${ARG}`
placeholders; `url` is the pinned mirror URL. Each declaration must match exactly
one command. Source Dockerfiles are never edited. Base image pulls, Git clones,
apt/npm/Go/NuGet/LuaRocks and similar package managers retain their native caches;
this is an explicit archive cache, not a general HTTPS proxy. Named contexts
require BuildKit. The existing `SREGYM_DOCKER_BUILDER` selects the builder.

Hotel uses its existing manifests through a temporary Kustomize overlay. Social
and astronomy retain existing Helm deployment settings and receive only image
and chart-path overrides. Namespace mismatches fail before a build. This work
does not migrate or delete legacy adapters. AS1 must land before the astronomy
legacy path is available by default; rebase GA1 onto `sdo3` after that merge.

Run the focused suite without Docker or an LLM:

```bash
SREGYM_BUILD_DECLARATIONS=/absolute/sdo/apps \
uv run python -m pytest -q tests/test_generic_source.py tests/test_source_deploy.py tests/test_app_workspace.py
```

Before AS1 lands, export its unmodified `sregym/service/source_deploy.py` at the
reviewed commit as `SREGYM_AS1_REFERENCE_MODULE=/absolute/reference.py` to exercise
all six legacy-equivalence cases (hotel, social, astronomy; amd64 and arm64).
Only the module's source-root path dependency is bound for loading it; builds
and loads are captured by the contracts. No cluster/model clients run.
The pinned social source lacks the legacy ARM Dockerfile; the command-selection
equivalence remains covered, while a real ARM build fails on that missing input.

After contracts pass, a hotel-only smoke may run detached with a PID record
while holding an idle lane's existing nonblocking lock. Never preempt AS1, SN1
or LK2. No live LLM runs are needed.
