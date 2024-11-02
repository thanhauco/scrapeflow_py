"""Main executor module for parallel workflow processing."""

import functools
from asyncio import CancelledError, Semaphore, wait_for
from asyncio import TimeoutError as TOError
from datetime import datetime
from pathlib import Path
from typing import (
    Awaitable,
    Callable,
    Literal,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import aioftp.errors
import aiohttp
import PIL
import tqdm.asyncio

from scrapeflow.proxies import ProxyProvider
from scrapeflow.status import Params, StatusData, read_one_status, write_one_status

# Either we return a response or raise an Exception. The second bool tells
# us whether we ran (True) or was skipped.
ExecutorResponse = Tuple[StatusData, bool]
Executor = Callable[["Context", StatusData], Awaitable[ExecutorResponse]]

# Some sites return 406 with the default user agent.
_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    + "(KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"
)
_ASYNC_TIMEOUT_SECONDS = 30
# There is an internal limitation within aiohttp on the max number of
# parallel requests (100) (cf.
# https://stackoverflow.com/questions/55259755/maximize-number-of-parallel-requests-aiohttp).
# We also need to limit this to avoid too many open files.
_MAX_PARALLELISM = 100


class Context(NamedTuple):
    "Each task gets the context as a parameter."
    # The directory for the binary blobs.
    dir: Path
    # To be used for async http requests.
    session: Optional[aiohttp.ClientSession]
    # List of executor names that need to be forced or 'all'
    force_executors: list[Executor] | Literal["all"]
    # Limit concurrency with semaphore
    semaphore: Semaphore
    # Timeout
    timeout_seconds: float
    # Proxy provider for http requests
    proxy_provider: Optional[ProxyProvider]
    # Use-case specific contextual params
    params: StatusData


# Returns if this phase needs to run.
def previous_execution(
    phase: str, context: Context, task_status: StatusData
) -> Union[ExecutorResponse, None]:
    # This phase hasn't run yet.
    if phase not in task_status:
        return None
    # This phase or all phases are forced.
    forced_executor_names = (
        [f.__name__ for f in context.force_executors]
        if context.force_executors != "all"
        else []
    )
    if phase in forced_executor_names or context.force_executors == "all":
        return None
    # Return the task status for the previous executor.
    return task_status[phase]


def taskify(func):
    """Wraps a function to check previous exectution and only run if not awailable."""

    @functools.wraps(func)
    async def wrapper(
        context: Context, key: str, task_status: StatusData, *args
    ) -> ExecutorResponse:
        # Skip this phase if it has already run and is not forced.
        prev_run = previous_execution(
            func.__name__,
            context,
            task_status,
        )
        # print("prev_run", func.__name__, prev_run)
        if prev_run is not None:
            return prev_run, False  # type:ignore
        response_status = await func(context, key, task_status, *args)
        return response_status, True

    return wrapper


async def _execute_single_chain_async(
    context: Context, executors: Sequence[Executor], key: str, params: Params, timeout
) -> str:
    async with context.semaphore:
        status: StatusData = read_one_status(context.dir, key) or {"params": params}

        recognized_exception = None
        for executor in executors:
            task = executor.__name__
            status_key = f"{task}_status"
            last_run_key = f"{task}_last_run"
            did_run = False
            try:
                response_status, did_run = await wait_for(
                    executor(context, key, status), timeout  # type: ignore
                )  # type: ignore
                # Successfully completed.
                status[task] = response_status
                if did_run:
                    status[status_key] = "SUCCESS"
                    status[last_run_key] = str(datetime.now())
                else:
                    # TODO(zt): remove
                    status[status_key] = "SUCCESS"
            # Certain types of errors / exceptions we save in status.
            except PIL.UnidentifiedImageError as e:
                recognized_exception = e
            except aiohttp.InvalidURL as e:
                recognized_exception = e
            except aioftp.errors.StatusCodeError as e:
                recognized_exception = e
            except aiohttp.ClientConnectorError as e:
                recognized_exception = e
            except aiohttp.ServerDisconnectedError as e:
                recognized_exception = e
            except aiohttp.ClientOSError as e:
                recognized_exception = e
            except aiohttp.ClientConnectionError as e:
                recognized_exception = e
            except aiohttp.ClientPayloadError as e:
                recognized_exception = e
            except aiohttp.ClientResponseError as e:
                recognized_exception = e
            except TOError as e:
                recognized_exception = e
            except CancelledError as e:
                recognized_exception = TOError(e)
            except RuntimeError as e:
                # Our own exceptions for signalling task errors.
                recognized_exception = e
            except Exception as e:
                write_one_status(context.dir, key, status)
                raise e

            if recognized_exception:
                print(
                    f"ERROR: {task} {key} {params} "
                    f"{recognized_exception.__class__.__name__}"
                )
                status[status_key] = (
                    f"ERROR {recognized_exception.__class__.__name__}::"
                    + f"{recognized_exception}"
                )
                status[last_run_key] = str(datetime.now())
                if task in status:
                    del status[task]
                break

        write_one_status(context.dir, key, status)
        return key


def _is_valid_filename(filename: str) -> bool:
    return filename not in [".", ".."] and not filename.count("/") > 0


async def execute_async(
    executors: Sequence[Executor],
    directory: Path,
    tasks: dict[str, Params] | Sequence[str],
    timeout: Optional[float] = None,
    # We have to use string names because the functions are not the same at comparision.
    force_executors: Optional[list[Executor] | Literal["all"]] = None,
    max_parallelism: Optional[int] = None,
    proxy_provider: Optional[ProxyProvider] = None,
    context_params: Optional[StatusData] = None,
) -> list[str] | None:
    # validate keys in tasks as it is used as a directory name
    all_keys = list(tasks.keys() if isinstance(tasks, dict) else tasks)
    # check for duplicate keys
    if len(set(all_keys)) != len(all_keys):
        duplicate_keys = [key for key in all_keys if all_keys.count(key) > 1]
        raise ValueError(f"Duplicate keys: {duplicate_keys}")

    for key in all_keys:
        if not _is_valid_filename(key):
            raise ValueError(f"Invalid key: {key}")

    directory.mkdir(parents=True, exist_ok=True)

    if timeout is None:
        timeout = _ASYNC_TIMEOUT_SECONDS

    if max_parallelism is None:
        max_parallelism = _MAX_PARALLELISM

    async with aiohttp.ClientSession(headers={"User-Agent": _USER_AGENT}) as session:
        context = Context(
            directory,
            session,
            force_executors=force_executors or [],
            semaphore=Semaphore(max_parallelism),
            timeout_seconds=timeout,
            proxy_provider=proxy_provider,
            params=context_params or {},
        )
        try:
            ret = await tqdm.asyncio.tqdm.gather(
                *[
                    _execute_single_chain_async(
                        context, executors, key, params, timeout
                    )
                    for key, params in (
                        tasks if isinstance(tasks, dict) else {key: {} for key in tasks}
                    ).items()
                ]
            )
        except CancelledError:
            return None
        except KeyboardInterrupt:
            return None
    return ret
