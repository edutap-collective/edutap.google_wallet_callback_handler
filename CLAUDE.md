# CLAUDE.md — edutap.wallet_google_callback_handler

Repository-specific rules. They take precedence over the global defaults.

## Language

**English only.** This repository belongs to eduTAP proper, not to any single
institution: README, changelog, documentation, docstrings, code comments, commit
messages, pull request titles and bodies, and replies to review comments.

The language follows the repository, not the conversation. A discussion held in
German still produces English artefacts here.

## What this service is

Receives the Google Wallet callback, verifies its signature against Google's root
signing keys, and writes the event to Kafka.

## Guard rails

**The distribution name is not the import path.** The repository and the
distribution follow `edutap.wallet_<vendor>[_<product>][_<service>]`; the import path
under `src/edutap/` still carries the older spelling. Three consequences that have
already caused breakage:

* `importlib.metadata.version("…")` looks up the **distribution** name. It sits at
  module level here, so getting it wrong fails at import, not on the first request.
* `[project.scripts]` and package-data keys name the **import path**. They must not
  follow a distribution rename.
* README examples using `uvicorn …` or `python -m …` name the **import path** too.

Before any rename, list which occurrences carry which of the three roles. A blanket
search-and-replace cannot tell them apart.

**A service is dated, a library is versioned.** The release tag here is a UTC
timestamp, `YYYY-MM-DD_HHmm`, and `make release` is what sets it. The dividing
line is the artefact, not the organisation the repository sits in: this package
is `edutap.*` like the library next to it, but it is not on an index, nothing
imports it, and what it ships is a container. Its tag says which state is
deployed. A semantic version would claim something nobody can act on — there is
no consumer to whom a major bump would mean anything.

Two things have to agree for that to work, and until 2026-08-27 neither did: the
publish workflow triggered on `v*.*.*`, so a dated tag built nothing at all, and
its metadata rules were `type=semver`, which cannot match a timestamp. A tag that
produces no image is invisible — there is no error anywhere.

**The callback comes from Google's servers, not from a device.** There is no device
field in the payload and there never will be — Google does not disclose how many
devices hold a pass. Do not model one.

**Signature verification is the whole security boundary.** The endpoint is publicly
reachable by construction; everything downstream trusts what this service accepted.

**The topic default here and the consumer's default are different variables.** This
service reads `EDUTAP_KAFKA_*`, the LMU consumer reads `LMU_EDUTAP_KAFKA_*`. They
have to be configured to the same value, and the deployment is what makes them agree.

## Sources and confidentiality

**No vendor internals — from any vendor, not just the ones currently in play.**
Neither in files nor in commit messages.

The standard is academic: a statement counts as reliable only where it can be
evidenced from public information, with a link. Everything else was obtained either
by our own testing or through insider knowledge, and the three are not
interchangeable:

* **Documented** — public source, linked. May be written as fact.
* **Verified, not citable** — obtained by a person from an access-protected area and
  checked there; the reference is recorded internally but must not be published; and
  the statement has been reduced to what is not confidential. May be written as fact,
  carrying this label. It is the rule journalism uses for source protection: the claim
  stands, we know where it comes from, the reader does not get the source.

  The four conditions hold together. A statement for which nobody can name the
  internal reference does not fall here — that is insider knowledge.
* **Measured** — established by our own tests. May be written down, but always marked
  as such, because it describes what a platform did on the day we looked, not what it
  guarantees. It can change with the next release, without notice and without an
  entry in any changelog.
* **Insider knowledge** — is not written down at all.

What a platform's behaviour *means for us* stays documentable even where the
mechanism does not: "the platform enforces a deadline, it is self-healing, it is
outside our control" carries the design consequence without disclosing anything.

Contract and regulatory material is wanted and citable: eduPersonAssurance, GÉANT and
eduGAIN terms, published wallet programme obligations.

## Working practice

Branch first, never commit on `main`. Push only when asked. Lint and tests green
before opening a pull request.

Design records under `docs/superpowers/` record a decision at a point in time — do
not rewrite them to match a later state; write a new one.
