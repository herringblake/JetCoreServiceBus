"""Parses the nkey seed out of a .creds file (Design.md §12 Step C6).

.creds bundles a JWT + nkey seed together (`nsc generate creds`, Step A3)
— the standard NATS client connection artifact. nats-py parses this
internally to sign the connection handshake, but doesn't expose that
parsing as public API (`Client._read_creds_user_nkey` is a private
method, confirmed by reading nats-py's source) — this is a small,
independent implementation of the same well-documented .creds block
format, so a real adapter entrypoint can reuse the SAME nkey for event
signing that its connection already authenticates with, rather than
generating an unrelated one every process start (Design.md §4.1: "the
same keypair serves both connection auth and event signing", per
crypto.py's own `generate_signing_keypair` docstring; see also B7's
finding about what goes wrong when a signing identity isn't stable).
"""

from __future__ import annotations

import re
from pathlib import Path

from jetcore.crypto import SigningKeyPair, _keypair_from_seed


class CredsParseError(ValueError):
    """`creds_path` doesn't contain a recognizable USER NKEY SEED block."""


#  nsc's own template is asymmetric — "-----BEGIN...-----" (5 dashes) but
# "------END...------" (6 dashes), confirmed by reading a real generated
# .creds file, not assumed from a PEM-style template. Matches a variable
# dash run on both markers rather than hardcoding either count.
_SEED_BLOCK = re.compile(
    rb"-+BEGIN USER NKEY SEED-+[ \t]*\r?\n"
    rb"(?P<seed>[^\r\n]+)\r?\n"
    rb"-+END USER NKEY SEED-+"
)


def load_signing_keypair(creds_path: Path) -> SigningKeyPair:
    """Extracts the nkey seed embedded in `creds_path` and derives its
    public key, returning both as a SigningKeyPair — the identity a real
    adapter should sign events with (Step C6), not a freshly generated one."""
    data = creds_path.read_bytes()
    match = _SEED_BLOCK.search(data)
    if match is None:
        raise CredsParseError(f"no USER NKEY SEED block found in {creds_path}")
    seed = match.group("seed").decode().strip()
    return _keypair_from_seed(seed)
