"""Tests for crypto.py (Design.md §4.1, §4.3) — Step B3. Fixed/freshly
generated test keys throughout; no live bus needed."""

import pyrage
import pytest
from gsb_core.crypto import (
    decrypt,
    encrypt_for_recipients,
    generate_encryption_keypair,
    generate_signing_keypair,
    sign,
    verify,
)

# --- Encryption ---------------------------------------------------------


def test_encrypt_decrypt_round_trip_single_recipient() -> None:
    recipient = generate_encryption_keypair()
    plaintext = b"a secret payload"

    ciphertext = encrypt_for_recipients(plaintext, [recipient.public_key])

    assert decrypt(ciphertext, recipient.private_key) == plaintext


def test_encrypt_for_multiple_recipients_each_can_decrypt() -> None:
    alice = generate_encryption_keypair()
    bob = generate_encryption_keypair()
    plaintext = b"shared secret"

    ciphertext = encrypt_for_recipients(plaintext, [alice.public_key, bob.public_key])

    assert decrypt(ciphertext, alice.private_key) == plaintext
    assert decrypt(ciphertext, bob.private_key) == plaintext


def test_non_recipient_cannot_decrypt() -> None:
    intended = generate_encryption_keypair()
    outsider = generate_encryption_keypair()
    ciphertext = encrypt_for_recipients(b"not for you", [intended.public_key])

    with pytest.raises(pyrage.DecryptError):
        decrypt(ciphertext, outsider.private_key)


def test_ciphertext_does_not_contain_plaintext() -> None:
    """Sanity check that this is actually doing encryption, not passing
    data through."""
    recipient = generate_encryption_keypair()
    plaintext = b"a very specific and searchable secret string"

    ciphertext = encrypt_for_recipients(plaintext, [recipient.public_key])

    assert plaintext not in ciphertext


# --- Signing --------------------------------------------------------------


def test_sign_verify_round_trip() -> None:
    signer = generate_signing_keypair()
    payload = b"an event payload"

    signature = sign(signer.seed, payload)

    assert verify(signer.public_key, payload, signature) is True


def test_verify_works_with_only_the_public_key_no_private_material() -> None:
    """The scenario Decision #5 actually depends on: a *recipient*, who
    only ever has the sender's public key, verifying a signature they did
    not create. This is the case that surfaced the real nkeys.py bug
    (KeyPair.verify() secretly requires the private key) — kept as an
    explicit, separate test from the round-trip above so a regression here
    can't hide behind a test that happens to still have the seed in scope.
    """
    signer = generate_signing_keypair()
    payload = b"an event payload"
    signature = sign(signer.seed, payload)

    # Only public_key crosses into this "verifier" logic — no `signer.seed`.
    public_key_only = signer.public_key
    assert verify(public_key_only, payload, signature) is True


def test_verify_rejects_tampered_payload() -> None:
    signer = generate_signing_keypair()
    signature = sign(signer.seed, b"original payload")

    assert verify(signer.public_key, b"tampered payload", signature) is False


def test_verify_rejects_signature_from_a_different_key() -> None:
    signer = generate_signing_keypair()
    impostor = generate_signing_keypair()
    payload = b"an event payload"
    signature = sign(impostor.seed, payload)

    assert verify(signer.public_key, payload, signature) is False


def test_verify_raises_on_malformed_public_key() -> None:
    with pytest.raises(ValueError, match="malformed nkey public key"):
        verify("not-a-real-nkey", b"payload", b"\x00" * 64)


def test_generate_signing_keypair_produces_distinct_keys_each_time() -> None:
    a = generate_signing_keypair()
    b = generate_signing_keypair()
    assert a.seed != b.seed
    assert a.public_key != b.public_key
