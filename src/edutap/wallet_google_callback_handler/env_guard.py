"""Refuse to start when the deployment still carries the pre-rename variables.

Every settings field has a default, so a deployment that still exports the old
names would not fail -- it would start with the development defaults and report
to nobody. This turns that silent misconfiguration into a loud one.

Unlike its siblings in the other VAS services, this check matches *exact names*
rather than a prefix. The retired prefix here was the bare ``EDUTAP_``, which
other packages legitimately use: ``EDUTAP_KAFKA_*`` is read by this very service,
``EDUTAP_WALLET_GOOGLE_*`` by the wallet library. A prefix check would flag those
and the service would never start again.

TEMPORARY. Remove once production and staging are on the new prefix; see
docs/superpowers/specs/2026-08-08-paket-umbenennung-runde-2-design.md in
lmu_edutap_dev_setup.
"""

from collections.abc import Mapping

# One entry per field of Settings. Whoever adds a field before this guard is
# removed has to add its retired spelling here -- test_env_guard.py enforces
# that the two stay in sync.
RETIRED_KEYS = frozenset(
    {
        "EDUTAP_ENVIRONMENT",
        "EDUTAP_GOOGLE_CALLBACK_URL",
        "EDUTAP_SENTRY_DSN",
    }
)
CURRENT_PREFIX = "EDUTAP_WALLET_GOOGLE_CALLBACK_HANDLER_"


def check_retired_env_vars(environ: Mapping[str, str]) -> None:
    """Raise if any variable still uses a retired name."""
    stale = sorted(RETIRED_KEYS & {key.upper() for key in environ})
    if not stale:
        return
    raise RuntimeError(
        f"Found {len(stale)} retired environment variable(s): {', '.join(stale)}. "
        f"This service now reads '{CURRENT_PREFIX}'. "
        f"Update the deployment configuration."
    )
