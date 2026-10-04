"""Module đo lường thời gian chạy (Execution Time) và tài nguyên bộ nhớ (Memory Profiling).

Tuân thủ quy chuẩn dự án Spark & Data Pipeline cho môi trường Apple Silicon (Mac M3).
"""

from contextlib import contextmanager
from functools import wraps
import os
import time
from typing import Callable, Generator
import psutil


def get_process_memory_mb() -> float:
    """Lấy lượng RAM hiện tại tiến trình Python đang sử dụng (RSS - Resident Set Size) theo MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


@contextmanager
def profile_block(block_name: str = "Khối thực thi") -> Generator[None, None, None]:
    """Context manager đo lường thời gian và biến thiên bộ nhớ RAM của một khối lệnh.

    Cách dùng:
        with profile_block("Phép toán GroupBy & Shuffle"):
            df.groupBy("category").count().show()
    """
    mem_before = get_process_memory_mb()
    time_start = time.perf_counter()

    print(f"\n🚀 [BẮT ĐẦU] {block_name}")
    print(f"   RAM trước khi chạy : {mem_before:.2f} MB")

    try:
        yield
    finally:
        time_elapsed = time.perf_counter() - time_start
        mem_after = get_process_memory_mb()
        mem_delta = mem_after - mem_before
        sign = "+" if mem_delta >= 0 else ""

        print(f"⏱️ [HOÀN TẤT] {block_name}")
        print(f"   Thời gian thực thi : {time_elapsed:.4f} giây")
        print(f"   RAM sau khi chạy   : {mem_after:.2f} MB ({sign}{mem_delta:.2f} MB)")
        print("-" * 50)


def profile_function(func: Callable) -> Callable:
    """Decorator đo lường thời gian và RAM của một hàm cụ thể."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        with profile_block(f"Hàm `{func.__name__}()`"):
            return func(*args, **kwargs)
    return wrapper
