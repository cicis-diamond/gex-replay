"""Package the published historical SPY archive for GitHub Pages."""
import base64
from collections import defaultdict
from datetime import date, datetime
import gzip
import json
import math
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

REPO = "anahatsingh-ui/options-dataset-hist"
API = f"https://api.github.com/repos/{REPO}"
START = "2025-01-01"
ET = ZoneInfo("America/New_York")


def get_json(url):
    with urlopen(Request(url, headers={"User-Agent": "cicis-diamond-gex-replay"}), timeout=45) as response:
        return json.load(response)


def download(url, target):
    with urlopen(url, timeout=90) as response, target.open("wb") as output:
        shutil.copyfileobj(response, output)


def timestamp(day, hour, minute=0):
    return datetime.fromisoformat(day).replace(hour=hour, minute=minute, tzinfo=ET).isoformat(timespec="minutes")


def build(prices_file, option_files):
    fields = ["open", "high", "low", "close"]
    prices = pq.read_table(prices_file, columns=["date", *fields],
                           filters=[("date", ">=", START)]).to_pylist()
    candles = {row["date"]: [row[field] for field in fields] for row in prices
               if all(row[field] is not None and math.isfinite(row[field]) and row[field] > 0 for field in fields)}
    result = []
    for path in option_files:
        nearest = {}
        chains = defaultdict(dict)
        columns = ["date", "expiration", "strike", "type", "implied_volatility", "gamma", "open_interest", "contract_id"]
        for batch in pq.ParquetFile(path).iter_batches(columns=columns):
            for row in batch.to_pylist():
                day, expiry = row["date"], row["expiration"]
                if day not in candles or expiry <= day or not re.fullmatch(r"SPY\d{6}[CP]\d{8}", row["contract_id"] or ""):
                    continue
                previous = nearest.get(day)
                if previous and expiry > previous:
                    continue
                if previous != expiry:
                    nearest[day] = expiry
                    chains[day].clear()
                strike, iv, gamma, oi = (row[key] for key in ("strike", "implied_volatility", "gamma", "open_interest"))
                if (not all(value is not None and math.isfinite(value) for value in (strike, iv, gamma, oi))
                        or strike <= 0 or iv <= 0.0001 or gamma < 0 or oi < 0):
                    continue
                right = {"call": "C", "put": "P"}[row["type"]]
                chains[day][strike, right] = [strike, right, iv, gamma, oi]
        for day, chain in chains.items():
            if {contract[1] for contract in chain.values() if contract[4] > 0} == {"C", "P"}:
                result.append([timestamp(day, 16), timestamp(nearest[day], 16, 15), *candles[day], list(chain.values())])
    result.sort(key=lambda row: row[0])
    if not result or len({row[0] for row in result}) != len(result):
        raise ValueError("Expected unique historical SPY sessions.")
    return result


def main():
    revision = get_json(f"{API}/commits/main")["sha"]
    raw = f"https://raw.githubusercontent.com/{REPO}/{revision}"
    files = get_json(f"{API}/contents/spy?ref={revision}")
    names = sorted(item["name"] for item in files if re.fullmatch(r"options_\d{4}\.parquet", item["name"])
                   and 2025 <= int(item["name"][8:12]) <= date.today().year)
    with TemporaryDirectory() as directory:
        root = Path(directory)
        prices = root / "underlying_prices.parquet"
        license_path = root / "LICENSE"
        download(f"{raw}/spy/{prices.name}", prices)
        download(f"{raw}/LICENSE", license_path)

        def option_files():
            for name in names:
                path = root / name
                download(f"{raw}/spy/{name}", path)
                yield path
                path.unlink()

        sessions = build(prices, option_files())
        payload = json.dumps({"source": "Philipp D. Dubach", "revision": revision,
                              "license": license_path.read_text(), "sessions": sessions},
                             separators=(",", ":"), allow_nan=False)
    output = Path("_site")
    output.mkdir(exist_ok=True)
    archive_name = f"spy-archive-{revision[:12]}.json"
    page = Path("index.html").read_text(encoding="utf-8")
    placeholder = '"spy-history-2025.json"'
    if page.count(placeholder) != 1:
        raise ValueError("Expected one archive filename in the replay page.")
    (output / "index.html").write_text(page.replace(placeholder, json.dumps(archive_name)), encoding="utf-8")
    packed = base64.b64encode(gzip.compress(payload.encode(), mtime=0)).decode()
    (output / archive_name).write_text(json.dumps({"gzip": packed}) + "\n")
    print(f"Built {len(sessions)} SPY sessions from {sessions[0][0][:10]} through {sessions[-1][0][:10]} ({revision}).")


if __name__ == "__main__":
    main()
