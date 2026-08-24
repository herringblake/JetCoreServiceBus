"""Payload encryption and event signing (Design.md §4.1, §4.3) — Step B3
(Design.md §11 Track B). Pure crypto primitives; no NATS, no envelope
awareness (that's bus_client.py, B6). Unit tested against fixed/freshly
generated test keys, no live bus needed.

Two independent mechanisms, per Design.md §4.1:
  - Encryption keypair (X25519, via `pyrage`/age): confidentiality. Multiple
    recipients' public keys can each decrypt the same ciphertext.
  - Signing keypair (Ed25519, "nkey" format, via `nkeys`): authenticity —
    lets a recipient verify who really sent an event, independent of
    transport trust, per Decision #5.

IMPORTANT — a real bug in nkeys.py 0.2.1, confirmed by reading its source
and testing, not assumed: `KeyPair.verify()` only works if `self._keys` is
already a full `nacl.signing.SigningKey` (it calls `self._keys.verify_key`
internally) — there is no working way to construct a `KeyPair` from just a
public key string and verify with it, even though that's the entire point
of asymmetric signature verification. `_decode_nkey_public()` below
reimplements nkey's public-key decoding (mirroring `KeyPair.public_key`'s
encode logic in reverse — base32, 1-byte prefix, 2-byte CRC16 checksum) so
verification can use `nacl.signing.VerifyKey` directly instead, bypassing
the broken method entirely. Confirmed correct against nkeys.py's own
encoding: the decoded bytes match `SigningKey.verify_key` exactly, and
independent verification (no private key material at all) both accepts a
genuine signature and rejects a tampered one.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
from typing import NamedTuple

import nacl.exceptions
import nacl.signing
import nkeys
import pyrage
from pyrage import x25519


class EncryptionKeyPair(NamedTuple):
    """An X25519 encryption keypair (Design.md §4.3)."""

    private_key: str
    public_key: str


class SigningKeyPair(NamedTuple):
    """An Ed25519 "nkey" signing keypair (Design.md §4.1). For tests/
    tooling — real adapters get theirs from `nsc` (Step A3), not this."""

    seed: str
    public_key: str


# --- Payload encryption (age / pyrage, Design.md §4.3) ----------------------


def generate_encryption_keypair() -> EncryptionKeyPair:
    """A fresh X25519 keypair, for tests/tooling."""
    identity = x25519.Identity.generate()
    return EncryptionKeyPair(private_key=str(identity), public_key=str(identity.to_public()))


def encrypt_for_recipients(plaintext: bytes, recipient_public_keys: list[str]) -> bytes:
    """Encrypt `plaintext` so that ANY of `recipient_public_keys` can
    decrypt it (age's native multi-recipient support — Design.md §4.3).

    Note: unlike a hand-rolled envelope, age's ciphertext bundles all
    per-recipient key-wrapping internally — there's no separate "wrapped
    key per recipient" to inspect from the outside. The envelope's
    `EncryptionMetadata.recipients` (envelope.py) is therefore just the
    list of intended recipient key strings, informational metadata for
    matching against the service-directory registry (§4.5) — not
    functional decryption data in its own right.
    """
    recipients = [x25519.Recipient.from_str(k) for k in recipient_public_keys]
    return bytes(pyrage.encrypt(plaintext, recipients))


def decrypt(ciphertext: bytes, private_key: str) -> bytes:
    """Decrypt `ciphertext` using one recipient's own private key. Raises
    `pyrage.DecryptError` if this key isn't one of the intended recipients
    (or the ciphertext is corrupt)."""
    identity = x25519.Identity.from_str(private_key)
    return bytes(pyrage.decrypt(ciphertext, [identity]))


# --- Event signing (Ed25519 / nkeys, Design.md §4.1) -------------------------


def generate_signing_keypair() -> SigningKeyPair:
    """A fresh Ed25519 nkey keypair, for tests/tooling. Real adapters use
    the nkey `nsc` (Step A3) already generated for their NATS identity —
    the same keypair serves both connection auth and event signing
    (Design.md §4.1's "two separate keypairs" note is about encryption vs.
    signing keys being different purposes, not that signing needs its own
    distinct nkey from the one used to connect)."""
    import os

    raw_seed = os.urandom(32)
    seed = nkeys.encode_seed(raw_seed, nkeys.PREFIX_BYTE_USER).decode()
    public_key = nkeys.from_seed(seed.encode()).public_key.decode()
    return SigningKeyPair(seed=seed, public_key=public_key)


def _digest(plaintext: bytes) -> bytes:
    """SHA-256 digest of the plaintext payload — what actually gets signed,
    per Design.md §4.1 ("the publisher signs a digest of the plaintext
    payload with its nkey"). Centralized here so signing and verification
    can never disagree about what "the digest" means."""
    return hashlib.sha256(plaintext).digest()


def sign(seed: str, plaintext: bytes) -> bytes:
    """Sign `plaintext`'s digest with the nkey identified by `seed`.
    Returns the raw 64-byte Ed25519 signature."""
    keypair = nkeys.from_seed(seed.encode())
    return bytes(keypair.sign(_digest(plaintext)))


def _decode_nkey_public(public_key: str) -> bytes:
    """Decode an nkey-encoded public key string to its raw 32-byte Ed25519
    key — reimplemented here because nkeys.py doesn't expose this itself
    (only `decode_seed`, for the longer seed encoding). Mirrors
    `KeyPair.public_key`'s encode logic in reverse; validates the CRC16
    checksum and prefix byte rather than trusting the input blindly."""
    padding = b"=" * (-len(public_key.encode()) % 8)
    try:
        raw = base64.b32decode(public_key.encode() + padding)
    except binascii.Error as exc:
        raise ValueError(f"malformed nkey public key: not valid base32 ({exc})") from exc
    if len(raw) != 35:  # 1 prefix + 32 key + 2 crc
        raise ValueError(f"malformed nkey public key: unexpected length {len(raw)}")
    prefix, key, crc_given = raw[0], raw[1:-2], int.from_bytes(raw[-2:], "little")
    if crc_given != nkeys.crc16(raw[:-2]):
        raise ValueError("malformed nkey public key: checksum mismatch")
    if prefix != nkeys.PREFIX_BYTE_USER:
        raise ValueError(f"expected a user nkey (prefix {nkeys.PREFIX_BYTE_USER}), got {prefix}")
    return key


def verify(public_key: str, plaintext: bytes, signature: bytes) -> bool:
    """Verify that `signature` is a valid signature over `plaintext`'s
    digest, made by the private key corresponding to `public_key`. Returns
    False for an invalid/mismatched signature; raises ValueError for a
    malformed `public_key` string (a caller bug, not a verification
    outcome)."""
    verify_key = nacl.signing.VerifyKey(_decode_nkey_public(public_key))
    try:
        verify_key.verify(_digest(plaintext), signature)
        return True
    except nacl.exceptions.BadSignatureError:
        return False
