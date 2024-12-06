# eduTAP.google_callback_handler - A Callback-Handler for Google Wallet

This eduTAP Package provides you with a reusable callback handler that conforms with the API specification from Google Wallet.


## Install Standalone Version

**Attention** Google expects a public https service at the location defined in the pass class
therefore we use traefik with letsencrypt

provide an .env file with the contents in the root of this directory

```env
DNS=192.168.66.1
DOMAIN=edutap.bluedynamics.net
HTTPS_PORT=8443
```

make sure that 

- the domain points to your server
- your server is reachable publically on port 80 for letsencrypt
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

- {DOMAIN}/{HTTPS_PORT}/docs shall show you the swagger interface
- {DOMAIN}/{HTTPS_PORT}/kafka-ui shows you the kafka user interface

TODO: protect the kafka user interface
