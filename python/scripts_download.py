import http.client
import time
import urllib.error
import urllib.request

USER_AGENT = "ikea-sales-data-pipeline-source/0.1"
CHUNK = 1 << 20  # 1 MB
RETRIES = 3
RETRYABLE_HTTP = {408, 429, 500, 502, 503, 504}


def _is_retryable(exc):
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in RETRYABLE_HTTP
    return True


def get_bytes(url, timeout=120):
    """GET a URL with an explicit User-Agent, retrying transient errors."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except (
            urllib.error.URLError,
            http.client.HTTPException,
            TimeoutError,
            OSError,
        ) as exc:
            if not _is_retryable(exc):
                raise
            if attempt == RETRIES:
                raise SystemExit(f"failed to fetch {url}: {exc}") from exc
            time.sleep(2 ** (attempt - 1))


def download(url, destination, expected_size=0, min_bytes=0, timeout=600):
    """Stream a URL to disk in 1 MB chunks via a .part file, retrying transient errors."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".part")
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response, open(
                partial, "wb"
            ) as handle:
                expected = (
                    expected_size
                    if expected_size > 0
                    else int(response.headers.get("Content-Length") or 0)
                )
                while chunk := response.read(CHUNK):
                    handle.write(chunk)
            written = partial.stat().st_size
            if expected and written != expected:
                raise OSError(
                    f"short download: {partial} is {written:,} bytes, "
                    f"source says {expected:,}"
                )
            if min_bytes and written < min_bytes:
                raise SystemExit(
                    f"unexpectedly small download: {partial} is {written:,} bytes "
                    f"(< {min_bytes:,})"
                )
            break
        except (
            urllib.error.URLError,
            http.client.HTTPException,
            TimeoutError,
            OSError,
        ) as exc:
            if not _is_retryable(exc):
                raise
            if attempt == RETRIES:
                raise SystemExit(str(exc)) from exc
            time.sleep(2 ** (attempt - 1))
    partial.replace(destination)
    print(f"downloaded {destination.name}: {written:,} bytes")
    return written
