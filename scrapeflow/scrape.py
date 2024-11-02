"""Scraping module for parallel scrapeflow."""

import functools
import hashlib
from pathlib import Path
from typing import Any, Callable, List, Sequence, Tuple

from scrapeflow.executor import Context, taskify
from scrapeflow.status import StatusData

ScraperResponse = Tuple[str, Path]
Scraper = Callable[[Context, str, StatusData], ScraperResponse]

# The purpose of @scrapify is to be able to easily create custom scrapers that use
# different names, not just {name}.scrape.


_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def scrapify(func: Scraper):
    """A decorator turning a function returning a url and a filename into a scraper."""

    @functools.wraps(func)
    async def wrapper(context: Context, key: str, status: StatusData) -> StatusData:
        url, file_name = func(context, key, status)
        response_status = {}
        cookies = (
            status["params"]["cookies"]
            if "params" in status and "cookies" in status["params"]
            else context.params["cookies"]
            if "cookies" in context.params
            else {}
        )

        assert context.session, "Synchronous scraping: not implemented"
        proxy = None
        if context.proxy_provider:
            proxy = context.proxy_provider.get_one_proxy()

        post_payload = None
        # request_context holds the request set up as a get or post request.
        request_context = None
        if "params" in status and "post_payload" in status["params"]:
            # Post request.
            post_payload = status["params"]["post_payload"]
            request_context = context.session.post(
                url=url,
                ssl=False,
                proxy=proxy,
                cookies=cookies,
                headers=_HEADERS,
                json=post_payload,
            )
        else:
            # Get request.
            request_context = context.session.get(
                url=url, ssl=False, proxy=proxy, cookies=cookies
            )

        async with request_context as response:
            status_code = response.status
            headers = {k: response.headers.getone(k) for k in response.headers.keys()}
            if status_code != 200:
                # TODO(zsolt): any way to pass headers?
                raise RuntimeError(f"HTTP response {status_code}")

            resp = await response.read()
            response_status["size"] = len(resp)
            response_status["content"] = hashlib.md5(resp).hexdigest()
            with open(file_name, mode="wb") as f:
                f.write(resp)
            response_status["response_headers"] = headers
        return response_status

    return wrapper


@taskify
@scrapify
def scrape(context: Context, key: str, status: StatusData) -> ScraperResponse:
    url = status["params"]["url"]
    return url, context.dir / f"{key}.scrape"


@scrapify
def scrape_helper(context: Context, key: str, status: StatusData) -> ScraperResponse:
    url = status["params"]["url"]
    return url, context.dir / f"{key}.scrape"


@taskify
async def scrape_with_validation(
    context: Context,
    key: str,
    status: StatusData,
    response_callback: Callable[[bytes], None],
) -> StatusData:
    response_status = await scrape_helper(context, key, status)
    # The purpose of this is to raise exception.
    response_callback((context.dir / "{key}.scrape").read_bytes())
    return response_status


def _ordered_unique_list(items: Sequence[Any]) -> List[Any]:
    return list(dict.fromkeys(items))


# def missing_scrapes(
#     directory: Path, task_params: Sequence[Params]
# ) -> dict[str, Params]:
#     """Returns the task params for which the scrape files are missing from the joins."""
#     existing_scrapes = {
#         s.removeprefix(f"{directory}/").removesuffix(".scrape")
#         for s in glob.glob(f"{directory}/*.scrape")
#     }
#     return _ordered_unique_list(
#         [p for p in task_params if url_to_name(p["url"]) not in existing_scrapes]
#     )


def _extract_content_type(ct: str) -> str:
    """Extracts the file type from a HTTP response content-type string."""
    # Like `application/xml; charset=utf-8` -> `xml`.
    # In a sample of ~8k urls, we've seen the following content type strings:
    # - application/pdf
    # - application/xml
    # - application/xml; charset=utf-8
    # - image/jpeg
    # - text/html
    # - text/html; charset-utf-8;charset=UTF-8
    # - text/html; charset="utf-8"
    # - text/html; charset=ISO-8859-1
    # - text/html; charset=utf-8
    # - text/html; charset=UTF-8
    # - text/html; Charset=UTF-8;charset=UTF-8
    # - text/html;charset=utf-8
    # - text/html;charset=UTF-8

    # Takes the part before the first ; and then the text after /.
    return ct.split(";")[0].split("/")[-1]


def remove_response_headers(d):
    return {k: v for k, v in d.items() if k != "response_headers"}


def extract_task_content_type(key: str, task_status: StatusData) -> str:
    if "scrape" not in task_status:
        raise RuntimeError("Missing response_headers")
    scrape_meta = task_status["scrape"]
    if "response_headers" not in scrape_meta:
        raise RuntimeError("Missing response_headers")
    response_headers = scrape_meta["response_headers"]
    if "Content-Type" not in response_headers:
        raise RuntimeError("Missing Content-Type")
    content_type = _extract_content_type(response_headers["Content-Type"])
    return content_type
