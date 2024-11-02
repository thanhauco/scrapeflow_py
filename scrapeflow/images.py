#!/usr/bin/env python

import errno
import hashlib
import os

import pandas as pd
from PIL import Image

from scrapeflow.executor import Context, taskify
from scrapeflow.scrape import ScraperResponse, scrapify
from scrapeflow.status import MetaData, read_metadata


@taskify
async def parse_image(context: Context, meta: MetaData) -> MetaData:
    response_meta = {}
    name = meta["name"]
    image_file_name = f"{context.dir}/{name}.scrape"

    with Image.open(image_file_name) as img:
        response_meta["format"] = img.format
        response_meta["width"] = img.size[0]
        response_meta["height"] = img.size[1]

    return response_meta


@taskify
async def thumbnail(context: Context, meta: MetaData) -> MetaData:
    response_meta = {}
    name = meta["name"]
    image_file_name = f"{context.dir}/{name}.scrape"

    with Image.open(image_file_name) as img:
        img.thumbnail(context.params["thumbnail"])
        img.save(f"{context.dir}/{name}.thumb.webp")
        response_meta["format"] = "WEBP"
        response_meta["width"] = img.size[0]
        response_meta["height"] = img.size[1]

    return response_meta


def extract_images(row):
    if pd.isna(row["upload"]):
        return []
    return [
        {
            "url": row["upload"]["thumbnail"],
            "width": row["thumbnail"]["width"],
            "height": row["thumbnail"]["height"],
            "type": "thumbnail",
        },
        {
            "url": row["upload"]["url"],
            "width": row["parse_image"]["width"],
            "height": row["parse_image"]["height"],
            "type": "full",
        },
    ]


def read_images(store_dir: str) -> pd.Series:
    """Extends the camp data with hostedImages."""
    df_metadata = read_metadata(store_dir)
    df_metadata["url"] = df_metadata["params"].map(lambda p: p["url"])
    return df_metadata.set_index("url").apply(extract_images, axis=1)


def symlink_force(target, link_name):
    try:
        os.symlink(target, link_name)
    except OSError as e:
        if e.errno == errno.EEXIST:
            os.remove(link_name)
            os.symlink(target, link_name)
        else:
            raise e


@scrapify
def scrape_web(context: Context, meta: MetaData) -> ScraperResponse:
    url = meta["params"]["url"]
    name = meta["name"]
    return url, f"{context.dir}/{name}.scrape"


@taskify
async def scrape_local_file_or_url(
    context: Context,
    meta: MetaData,
) -> MetaData:
    url = meta["params"]["url"]
    if not url.startswith("file://"):
        return await scrape_web(context, meta)
    file_name = url.removeprefix("file://")
    name = meta["name"]

    response_meta = {}
    response_meta["size"] = os.path.getsize(file_name)
    with open(file_name, "rb") as f:
        response_meta["content"] = hashlib.md5(f.read()).hexdigest()
    symlink_force(file_name, f"{context.dir}/{name}.scrape")

    return response_meta
