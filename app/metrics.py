from prometheus_client import Counter, Histogram

REDIRECTS = Counter(
    "urlshortener_redirects_total",
    "Redirect outcomes",
    ("outcome",),
)
REDIRECT_LATENCY = Histogram(
    "urlshortener_redirect_duration_seconds",
    "Redirect request duration",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2),
)
ANALYTICS_PUBLISH_FAILURES = Counter(
    "urlshortener_analytics_publish_failures_total",
    "Redirect analytics events that could not be durably published",
)
