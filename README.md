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
make sync          # create or refresh the environment (uv)
make lint          # ruff check, ruff format --check, ty check
make reformat      # apply what ruff can fix itself
make test-local    # the fast suite: no broker, no network
make test-matrix   # the suite on every supported Python version (tox)
make docker-build  # build the service image
```

`make help` lists them.

Install the commit hooks once with `prek install`; `prek run --all-files` runs
them over the whole tree. The configuration lives in `.pre-commit-config.yaml`
and is readable by `pre-commit` as well.
