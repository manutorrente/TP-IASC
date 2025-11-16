import asyncio
from typing import List, Any, Coroutine

class NotEnoughResultsError(Exception):
    """Raised when fewer than n tasks complete before timeout."""
    pass

async def gather_n(
    coroutines: List[Coroutine],
    n: int,
    timeout: float,
    *,
    count_exceptions: bool = False,
) -> List[Any]:
    """
    Run coroutines concurrently and return the results of the first n to complete.

    Args:
        coroutines: List of coroutine objects.
        n: Number of results to return.
        timeout: Max seconds to wait before raising NotEnoughResultsError.
        count_exceptions: Whether to count failed tasks toward n.

    Raises:
        NotEnoughResultsError: If fewer than n results complete before timeout.
    """
    if n > len(coroutines):
        raise ValueError("n cannot be greater than the number of coroutines")

    tasks = [asyncio.create_task(coro) for coro in coroutines]
    results = []

    try:
        for completed_task in asyncio.as_completed(tasks, timeout=timeout):
            try:
                result = await completed_task
                results.append(result)
            except Exception as e:
                if count_exceptions:
                    results.append(e)
                # Otherwise, ignore and wait for another successful one

            if len(results) >= n:
                break

        if len(results) < n:
            raise NotEnoughResultsError(
                f"Only {len(results)} tasks completed before timeout"
            )

        return results[:n]

    except asyncio.TimeoutError:
        raise NotEnoughResultsError(
            f"Timeout reached before {n} tasks completed"
        )

    finally:
        # Cancel any unfinished tasks
        for t in tasks:
            if not t.done():
                t.cancel()
