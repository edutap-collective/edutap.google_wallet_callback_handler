# eduTAP.wallet_google_callback_handler - A Callback-Handler for Google Wallet

This eduTAP Package provides you with a reusable callback handler that conforms with the API specification from Google Wallet.
see https://developers.google.com/wallet/generic/use-cases/use-callbacks-for-saves-and-deletions

This docker compose contains:

- a google pass callback handler
    it just puts the request as is into kafka
- kafka instance storing incoming callback requests from google
- kafka-ui for viewing what happens in kafka
- traefik ingress server that handles https and automatic letsencrypt stuff

## Install Standalone Version

**Attention** Google expects a public https service at the location defined in the pass class
therefore we use traefik with letsencrypt

provide an .env file with the contents in the root of this directory, for example like so:

```env
DNS=192.168.8.1
DOMAIN=demo.edutap.eu
HTTPS_PORT=443
```

make sure that

- the domain points to your server
- your server is reachable publicly on port 80 for letsencrypt
- your HTTPS_PORT is free

then start the appliance:

```bash
docker-compose up
```

the appliance contains the following services:

- callback-handler
    reachable under /callback
- kafka
    reachable
- kafka-ui
- traefik


after startup, check:

- `https://{DOMAIN}/{HTTPS_PORT}/docs` shall show you the swagger interface
- `https://{DOMAIN}/{HTTPS_PORT}/kafka-ui` shows you the kafka user interface
- `https://{DOMAIN}/{HTTPS_PORT}/v1/callback` contains the callback url

TODO: protect the kafka user interface

### troubleshooting

- kafka complains about 'permission denied'

make sure that ./kafka_data has the necessary permissions

- permission complaints concerning the `./letsencrypt` dir

fiddle around with the permissions of this directory ;)
## Development

Everything runs through the `Makefile`, and the CI calls the same targets, so a
green checkout is a green pipeline.

```bash
make sync              # create or refresh the environment (uv)
make lint              # ruff check, ruff format --check, ty check
make reformat          # apply what ruff can fix itself
make test-local        # the fast suite: no broker, no network
make test-integration  # the suite against a real broker (starts one)
make test-matrix       # the fast suite on every supported Python version (tox)
make docker-build      # build the service image for the cluster's architecture
```

`make help` lists them.

Install the commit hooks once with `prek install`; `prek run --all-files` runs
them over the whole tree. The configuration lives in `.pre-commit-config.yaml`
and `pre-commit` reads it just as well.

### Tests

`make test-local` needs nothing but the environment. It substitutes the Kafka
producer, which is the right thing for tests about this package's own decisions
-- which topic an event goes to, what the record key is, what happens when the
broker is gone.

`make test-integration` is the one that talks to a broker. It starts the
single-node Kafka in `compose.test.yml`, runs the tests marked `integration`,
and takes the broker down again whether they passed or not. Those tests are
deselected by default, so a laptop with no Docker daemon can still run the
suite.

```bash
make test-integration

# or, with a broker of your own:
docker compose -f compose.test.yml up -d --wait
EDUTAP_KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uv run pytest -m integration
docker compose -f compose.test.yml down -v
```

### Container image

```bash
make docker-build
docker run --rm -p 8086:8086 edutap-wallet-google-callback-handler:local
```

The image is built for `linux/amd64` regardless of the machine building it:
every cluster node is x86_64, and an arm64 image fails at start with `exec
format error`. It runs as an unprivileged user and listens on `$HTTP_PORT`,
which defaults to 8086.

### Releases

The release tag is a UTC timestamp, `YYYY-MM-DD_HHmm`, and it becomes the image
tag verbatim.

Not a semantic version, and the distinction is the artefact rather than the
repository it sits in: **a service is dated, a library is versioned.** This one
is a service — it is not on an index, nothing imports it, and what it ships is a
container. The tag therefore says *which state* is deployed. A number would claim
something nobody can act on: there is no consumer to whom a major bump would mean
anything. `edutap.wallet_google`, which is a library, stays on semantic versions
for exactly the same reason read the other way round.

```bash
make release        # release-check, then tag main with the current UTC minute and push
make release-check  # the checks on their own: clean, pushed, not already tagged
make release-digest # the pin for ansible-app-server: <tag>@sha256:...
```

`make release` reads the clock when it runs, so the tag cannot be stale. Override
it only to repeat a release that failed *after* tagging:

```bash
make release RELEASE_TAG=2026-08-27_0930
```

The checks come before the tag, not after: a tag on a commit that is not on the
server points into nothing for everyone else, and the workflow that builds on the
tag would check out a commit the runner cannot fetch.

`latest` still follows the tip of `main` and means nothing more than that. A
deployment pins the dated tag together with its digest — `make release-digest`
prints that pair in the form `group_vars/lrz_cc/edutap_production.yml` expects.

### Configuration

Three environment namespaces meet in this service, and they are not
interchangeable:

| Prefix | Read by | Examples |
| --- | --- | --- |
| `EDUTAP_WALLET_GOOGLE_CALLBACK_HANDLER_` | this package | `ENVIRONMENT`, `SENTRY_DSN` |
| `EDUTAP_KAFKA_` | this package, shared across the estate | `BOOTSTRAP_SERVERS`, `GOOGLE_CALLBACK_TOPIC`, `CA_FILE`, `CERT_FILE`, `KEY_FILE` |
| `EDUTAP_WALLET_GOOGLE_` | `edutap.wallet_google` | `GOOGLE_ENVIRONMENT`, `HANDLER_CALLBACK_VERIFY_SIGNATURE` |

The Kafka prefix is deliberately not this package's own: the broker list and the
client certificates are the same for every eduTAP service on a cluster.
