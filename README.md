- [Parallel workflow for web scraping and processing](#parallel-workflow-for-web-scraping-and-processing)
    - [Overview](#overview)
    - [Installation](#installation)
  - [`scrapeflow.executor` module](#scrapeflowexecutor-module)
    - [Basic usage](#basic-usage)
    - [API](#api)
      - [**Arguments**](#arguments)
      - [**Returns**](#returns)
      - [**Raises**](#raises)
      - [**Output**](#output)
  - [Executors](#executors)
  - [`scrapeflow.status` module](#scrapeflowstatus-module)
  - [`scrapeflow.scrape` module](#scrapeflowscrape-module)
    - [`scrape` Executor](#scrape-executor)
    - [`scrape_with_validation` Executor](#scrape_with_validation-executor)
  - [`scrapeflow.proxies` module](#scrapeflowproxies-module)
    - [The base class](#the-base-class)
    - [Implementations](#implementations)
      - [`ProxyProviderFromList` and `ProxyProviderFromDict`](#proxyproviderfromlist-and-proxyproviderfromdict)
      - [`ProxyProviderFromProxyscrape`](#proxyproviderfromproxyscrape)
      - [`ProxyProviderFromWebshare` and `ProxyProviderFromIPRoyal`](#proxyproviderfromwebshare-and-proxyproviderfromiproyal)

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>

# Parallel workflow for web scraping and processing

This library provides utilities to execute workflows in a parallel fashion, specifically
designed for web scraping. The main modules are described below.

### Overview

The library operates on **tasks**, typically URLs. Tasks are executed in parallel
fashion. For each task, multiple **`Executor`s** are run sequentially.

The library maintains a _status.json_ file in the working directory for each task which
stores the state of the task. This allows monitoring failures and hot restarting the
workflow from previous runs.

### Installation

You can install the module from github with:

```bash
pip install -U 'git+ssh://git@github.com/terek/scrapeflow.git/'
```

## `scrapeflow.executor` module

Provides the main infrastructure for running the workflow.

### Basic usage

```python
from pathlib import Path
from scrapeflow.executor import execute_async
from scrapeflow.scrape import scrape

tasks = {
    "google": {"url": "http://www.google.com"},
    "bing": {"url": "http://www.bing.com"}
}
directory = Path("/tmp/test/")

await execute_async(executors=[scrape], directory=directory, tasks=tasks)
```

### API

```python
async def execute_async(
    executors: Sequence[Executor],
    directory: Path,
    tasks: dict[str, Params] | Sequence[str],
    timeout: Optional[float] = None,
    force_executors: Optional[list[str]] = None,
    max_parallelism: Optional[int] = None,
    proxy_provider: Optional[ProxyProvider] = None,
) -> list[str]
```

#### **Arguments**

- `executors`

  A list of `Executor` functions (see below)

- `directory`

  The directory where the statusdata files (and potentially outputs of the processing) are
  stored.

- `tasks`

  Initially, this has to be a dict of `key` --> `Params`, where `key` will be the file\
  name and `Params` will be passed to each task.
  At subsequent calls, it's enough to provide a list of `key`s, and the corresponding
  `Params` will be read from the status files.
  
- `timeout`

  Task timeout in seconds. Exection of tasks are cancelled if they take longer than this
  time. The default timeout is 30 seconds.

- `max_parallelism`

  Maximum concurrent tasks. The underlying library caps this at 100.

- `force_executors`

  A list of names of `Executor`s which need to be rerun even if they were successful on
  previous iterations. To force rerunning everything you can use

  ```python
  force_executors=["all"]
  ```

- `proxy_provider`

  A `ProxyProvider` object to provide proxies for scrape. See below.

#### **Returns**

A list of `basename`s for the successful or skipped tasks where the
`basename` is basename of the output corresponding to the url in the `directory`.

#### **Raises**

The module recognizes the following list of exceptions and logs them in the statusdata
without crashing the overall execution. Unhandled exceptions crash the execution flow
so that users can debug what happened.

```python
except aiohttp.ClientConnectorError as e:
except aiohttp.ServerDisconnectedError as e:
except aiohttp.ClientOSError as e:
except aiohttp.ClientConnectionError as e:
except aiohttp.ClientPayloadError as e:
except aiohttp.ClientResponseError as e:
except TimeOutError as e:
except CancelledError as e:
except RuntimeError as e:
```

#### **Output**

The output of the run is a bunch of _\*.status.json_ files in the `directory` documenting
the workflow runs as well as containing the output of each individual `Executor` runs.

The typical statusdata file looks like this (comments inside are from me)

```jsonc
{
  # The basename corresponding to the url (task). This file is <name>.status.json
  "name": "9cbc5ee4b61e0acb335d56e96c6b2586",
  # The task defined by a set of params, url is mandatory.
  "params": {
    "url": "http://www.bing.com",
    ...
  },
  # Output of the "scraper" executor (in json format)
  "scraper": {
    "size": 95534,
    "content": "b20a97b447f1b3c6573e20d57d7fe4b8",
    # An example of emebedded json. This could even be a list as well.
    "response_headers": {
      "Cache-Control": "private",
      "Transfer-Encoding": "chunked",
      "Content-Type": "text/html; charset=utf-8",
      "Content-Encoding": "gzip",
      ...
    }
  },
  # The status of the run, could be "SKIPPED", "SUCCESS" or "FAILED" (with error message)
  "scraper_status": "SUCCESS",
  # Last actual run of the scrape. When status is "SKIPPED" this is not updated
  "scraper_last_run": "2022-08-05 16:03:52.336815",

  # Another executor without any output.
  "executor2": {},
  "executor2_status": "ERROR RunTimeError::could not compute something",
  "executor2_last_run":"2022-08-05 16:03:52.336815",
  ...
}
```

## Executors

`Executor`s are async routines that process one task. The interface for `Executor`s are
defined as:

```python
# executor.py
Executor = Callable[["Context", StatusData], Awaitable[ExecutorResponse]]
ExecutorResponse = Tuple[StatusData, bool]
```

`Context` encodes the execution context for the workflow as is defined as:

```python
# executor.py

class Context(NamedTuple):
    "Each task gets the context as a parameter."
    # The directory for the binary blobs.
    dir: str
    # To be used for async http requests.
    session: Optional[aiohttp.ClientSession]
    # List of executor names that need to be forced or 'all'
    forced_executors: list[str]
    # Limit concurrency with semaphore
    semaphore: Semaphore
    # Timeout
    timeout_seconds: float
    # Proxy provider for http requests
    proxy_provider: Optional[ProxyProvider]
```

A typical executor implementation looks like this:

```python
from scrapeflow.executor import Context, taskify
from scrapeflow.statusdata import StatusData

@taskify
async def scrape(context: Context, key: str, status: StatusData) -> StatusData:
    response_status = {}
    url = status["params"]["url"]

    # Do something potentially asynchronously.
    async with context.session.get(url=url) as response:
        status = response.status
        headers = {k: response.headers.getone(k) for k in response.headers.keys()}

        # When something goes wrong and don't want to stop the whole precossing, raise
        # a RuntimeError exception.
        if status != 200:
            raise RuntimeError(f"HTTP response {status}")

        # Wait for the async call.
        resp = await response.read()

        # Write information into the status file. These will show up under the task name,
        # which is the name of the function.
        response_status["size"] = len(resp)
        response_status["content"] = hashlib.md5(resp).hexdigest()
        response_status["response_headers"] = headers

        # Write something into the scratch directory.
        with open(context.dir / f"{key}.scrape", mode="wb") as f:
            f.write(resp)

    return response_status
```

## `scrapeflow.status` module

This module defines the `StatusData` object and provides two utility functions:

```python
# Reads all status files in directory and returns the dataframe for them.
def read_status(directory: Path) -> pd.DataFrame:

# Reads all status files in directory and counts the outputs the histogram of status
# messages for each executor.
def status_summary(directory: Path) -> pd.DataFrame:
```

## `scrapeflow.scrape` module

This module provides executors for scraping the web through `GET` or `POST` requests.

### `scrape` Executor

This is he base class that performs scraping. It scrapes the url in
`status["params"]["url"]` and writes the result of the correspondis `*.scrape` file into
the scratch directory.

By default it sends a `GET` request, but if `status["params"]["post_payload"]` is defined
(as a python dict object) it sends a `POST` request.

### `scrape_with_validation` Executor

```python
async def scrape_with_validation(
    context: Context,
    key: str,
    status: StatusData,
    response_callback: Callable[[bytes], None],
) -> StatusData:
```

This executor scrapes the url in `status["params"]["url"]` and writes the result of the
scrape into the scratch directory.

The interesting part of this implementation is the optional `response_callback`. This
function could quickly check that the scraped content is as expected (eg. real SERP
result as opposed to some _I'm not a robot_ captcha). The callback is expected to raise
a `RuntimeError` when the result is not what we expect.

An example of using this executor with callback:

```python
# We use scrape_base so that we can name our scrape function scrape; the name of the
# function will be the task name in statusdata.
from scrapeflow.scrape import scrape_with_validation

def callback(content: bytes) -> None:
    if content.find(b"ytInitialData") < 0 or content.find(b"topLevelButtons") < 0:
        raise RuntimeError("Scrape missing data.")

async def scrape(context, status):
    """This runs scrape but after it checks whether it contains ytInitialData and
    topLevelButtons. If it does not, it removes the scrape file and
    """
    # Add a bit of sleep to make sure that we don't overwhelm the backend.
    await asyncio.sleep(5 * random())
    return await scrape_with_validation(context, status, callback)
```

## `scrapeflow.proxies` module

This module defines the the base class `ProxyProvider` and implements a few ways to get
proxies.

### The base class

```python
class ProxyProvider:
    # Dictionary of proxies from country -> [] list of proxies. If the proxy provider
    # does not specify country information, the country key is "". The list of proxies
    # is fully qualified, ie. contain the protocol (and user/password if necessary.)
    proxies: dict[str, list[str]] = {}
    bad_proxies: set[str] = set()
    default_retries = 5

    # Checks which proxies are alive. This is useful for free lists.
    async def check_proxies(self, timeout=30, retries=5)-> None:

    # Returns a fully qualified proxy in 'http://[user:password@]ip:port' format or
    # None if we don't have any valid proxies.
    # The country restrict should have the same string as provided by the underlying
    # proxy list.
    def get_one_proxy(self, country: Optional[str] = None) -> str | None:
```

### Implementations

We have a couple of implementantion for the base class depending on the usecase.

#### `ProxyProviderFromList` and `ProxyProviderFromDict`

```python
from scrapeflow.proxies import ProxyProviderFromDict, ProxyProviderFromList
proxy_provider = ProxyProviderFromList(["http://user:password@ip:port"])

proxy_provider = ProxyProviderFromDict({
  "US": ["http://ip1:port1", "http://ip2:port2"],
  "DE": ["http://ip3:port3"],
  # Use this proxy when no country is specified
  "*": ["http://ip4:port4"],
})
```

#### `ProxyProviderFromProxyscrape`

This fetches free proxies from `https://api.proxyscrape.com`.

```python
from scrapeflow.proxies import ProxyProviderFromProxyscrape

proxy_provider = ProxyProviderFromProxyscrape()
# Since these are not very reliable ones, let's see which are good enough to use.
await proxy_provider.check_proxies()

```

#### `ProxyProviderFromWebshare` and `ProxyProviderFromIPRoyal`

These are paid proxy services and they require an APIKEY to get and, in case of IPRoyal,
also an order_id.

Webshare proxies have limited bandwith per month, so only use it when we need it. Our
IPRoyal only has 5 proxies for this month, but they come with unlimited bandwith.

