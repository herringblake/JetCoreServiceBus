"""Tests for creds.py (Design.md §12 Step C6)."""

from pathlib import Path

import pytest
from jetcore.creds import CredsParseError, load_signing_keypair
from jetcore.crypto import sign, verify

REAL_CREDS = Path("infra/nats/operator/creds/webhook-listener-01.creds")


def test_loads_real_seed_from_a_real_creds_file() -> None:
    """Against an actual nsc-generated .creds file (Step A3's output),
    not a hand-crafted fixture — proves the parser handles the real
    template's asymmetric dash counts (5 for BEGIN, 6 for END), not just
    a symmetric one this test invented."""
    keypair = load_signing_keypair(REAL_CREDS)

    assert keypair.seed.startswith("SU")  # nkey seed encoding for a User
    assert keypair.public_key.startswith("U")  # nkey public encoding for a User

    # The derived keypair must actually work for signing/verification —
    # not just look like plausible strings.
    signature = sign(keypair.seed, b"hello")
    assert verify(keypair.public_key, b"hello", signature)


def test_missing_seed_block_raises(tmp_path: Path) -> None:
    creds = tmp_path / "no-seed.creds"
    creds.write_text(
        "-----BEGIN NATS USER JWT-----\nnot a real jwt\n------END NATS USER JWT------\n"
    )

    with pytest.raises(CredsParseError):
        load_signing_keypair(creds)


def test_symmetric_dash_variant_also_parses(tmp_path: Path) -> None:
    """The regex is deliberately dash-count-agnostic (not hardcoded to
    the real template's asymmetry) — confirm a plain 5-dashes-both-sides
    PEM-style block works too, not just the exact real-world quirk."""
    creds = tmp_path / "symmetric.creds"
    creds.write_text(
        "-----BEGIN USER NKEY SEED-----\n"
        "SUAESYV3QZVFS46ZLC2CDJBU6BGSWSVOL2RQL7V7OIOHDJH2ZSATZL2LJQ\n"
        "-----END USER NKEY SEED-----\n"
    )

    keypair = load_signing_keypair(creds)

    assert keypair.seed == "SUAESYV3QZVFS46ZLC2CDJBU6BGSWSVOL2RQL7V7OIOHDJH2ZSATZL2LJQ"
