"""通用等待原语：把各模块里手写的"轮询到超时"循环收敛成一处。

原先至少有 6 处各写了一遍 ``deadline = clock() + timeout`` / ``while`` /
``sleeper`` 的组合（编排器、两种池、节点管理器等），超时语义与日志节奏
各不相同。统一后 predicate 一律"先判断再等待"，``timeout<=0`` 表示只判断
一次，不会出现某些实现先 sleep 再判断的空等。
"""
from __future__ import annotations

import time
from typing import Callable


class WaitTimeout(TimeoutError):
    """等待条件在超时前没有满足。

    继承 ``TimeoutError``：历史代码与调用方都按 TimeoutError 捕获，
    替换实现时不改变上层行为。
    """


def wait_for(
    predicate: Callable[[], bool],
    *,
    timeout: float,
    interval: float = 1.0,
    message: str = "等待超时",
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
    on_tick: Callable[[float], None] | None = None,
) -> None:
    """轮询 ``predicate`` 直到为真；超时抛 :class:`WaitTimeout`。

    on_tick 在每次休眠前收到"已等待秒数"，用于打日志或上报进度；
    不使用 on_tick 时本函数与原先各处的循环完全等价。
    """
    deadline = clock() + float(timeout)
    waited = 0.0
    while True:
        if predicate():
            return
        if clock() >= deadline:
            raise WaitTimeout(message)
        if on_tick is not None:
            on_tick(waited)
        sleeper(interval)
        waited += interval
