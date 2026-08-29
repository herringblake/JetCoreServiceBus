"""http_adapter — calls a configured external REST API when triggered by
a bus event, publishes the response back as a correlated event
(Design.md §8, §13 Track H).

Step H1 scaffold. Real modules (settings, trigger handler, entrypoint)
land in Steps H2-H4 — this package is intentionally empty of application
logic until then.
"""

__version__ = "0.1.0"
