"""webhook_sender — subscribes to configured subjects and relays each
decrypted payload to an external webhook URL, best-effort, no retry
(Design.md §8, Decision #12, §13 Track G).

Step G1 scaffold. Real modules (settings, relay handler, entrypoint) land
in Steps G2-G4 — this package is intentionally empty of application logic
until then.
"""

__version__ = "0.1.0"
