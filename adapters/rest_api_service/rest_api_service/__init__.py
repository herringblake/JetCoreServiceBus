"""rest_api_service — "front door" for external HTTP clients, translating
inbound REST calls into bus events (Design.md §8, §13 Track I).

Step I2 scaffold. Real modules (settings, pending-reply correlation, HTTP
app, entrypoint) land in Steps I3-I6 — this package is intentionally
empty of application logic until then.
"""

__version__ = "0.1.0"
