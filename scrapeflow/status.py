import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

StatusData = dict[str, Any]
Params = dict[str, Any]


def _read_status_from(directory: Path, keys: Iterable[str]) -> pd.DataFrame:
    """Reads the json metadata from the given files into a DF."""
    status_map = {}
    for key in keys:
        try:
            status_data = read_one_status(directory, key)
            status_map[key] = status_data or {"params": {}}
        except Exception as e:
            print(key)
            raise e
    return pd.DataFrame.from_dict(status_map, orient="index")


def url_to_key(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def read_one_status(directory: Path, key: str) -> StatusData | None:
    status_file_name = directory / f"{key}.status.json"
    if not status_file_name.is_file():
        return None
    return json.loads(status_file_name.read_text())


def write_one_status(directory: Path, key: str, status: StatusData) -> None:
    status_file_name = directory / f"{key}.status.json"
    # Convert meta to string before opening the file to avoid overwriting in case of an
    # error during dumps.
    status_file_name.write_text(json.dumps(status, indent=2))


def read_status(directory: Path) -> pd.DataFrame:
    """Read metadata for all the blobs in the given directory."""
    keys = (
        str(x.name).removesuffix(".status.json")
        for x in directory.glob("*.status.json")
    )
    df = _read_status_from(directory, keys)
    return df


def write_status(directory: Path, df_status: pd.DataFrame):
    """Write the rows of df_status to individual .status.json files."""
    for key, row in df_status.iterrows():
        assert isinstance(key, str), key
        write_one_status(directory, key, row.to_dict())


def urls_to_tasks(urls: list[str]) -> dict[str, Params]:
    """Converts a list of urls to list of task params."""
    return {url_to_key(u): {"url": u} for u in urls}


def status_summary(directory: Path) -> pd.DataFrame:
    df_status = read_status(directory)
    if df_status.empty:
        return df_status
    df_status = (
        pd.concat(
            [
                df_status[x].value_counts().to_frame()
                for x in list(df_status)
                if x.endswith("_status")  # type: ignore
            ]
        )
        .reset_index(names=["index"])
        .groupby("index")
        .sum()
        .astype(int)
    )
    return df_status.loc[df_status.sum(axis=1).sort_values(ascending=False).index]
