import json
import time
from pathlib import Path

from scripts_download import download, get_bytes

DATA_DIR = Path(__file__).parent / "../data/school_census_names"
REPO = "matt40k/Names"
FILES = {
    "forenames.csv": "Forename_Autumn2014.csv",
    "surnames.csv": "Surname_Autumn2014.csv",
}
MIN_CSV_BYTES = 1_000


def main():
    start = time.perf_counter()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    commit = json.loads(
        get_bytes(f"https://api.github.com/repos/{REPO}/commits/main")
    )["sha"]
    print(
        f"source: DfE Autumn 2014 School Census names, OGL v3.0, via {REPO} @ "
        f"{commit[:12]}"
    )

    for local_name, remote_name in FILES.items():
        download(
            f"https://raw.githubusercontent.com/{REPO}/{commit}/{remote_name}",
            DATA_DIR / local_name,
            min_bytes=MIN_CSV_BYTES,
            timeout=120,
        )

    print(f"done in {time.perf_counter() - start:.1f}s -> {DATA_DIR.resolve()}")


if __name__ == "__main__":
    main()
