# # src/crawler/fetcher.py
#
# from __future__ import annotations
#
# import logging
# import time
#
# import requests
# from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential
#
# from src.config import settings
# from src.pipeline.models import metrics
#
# logger = logging.getLogger("fetcher")
#
#
# class Fetcher:
#     def __init__(self) -> None:
#         self.session = requests.Session()
#         self.session.headers.update(
#             {
#                 "User-Agent": settings.user_agent,
#                 "Accept-Language": "en-US,en;q=0.9",
#             }
#         )
#
#     @retry(
#         stop=stop_after_attempt(3),
#         wait=wait_exponential(multiplier=1, min=1, max=8),
#         before_sleep=before_sleep_log(logger, logging.WARNING),
#     )
#     def get(self, url: str) -> tuple[str, str]:
#         logger.info("[GET] %s", url)
#         start = time.time()
#         metrics.inc("total_requests")
#
#         try:
#             resp = self.session.get(url, timeout=settings.timeout, allow_redirects=True)
#             metrics.inc("success_requests")
#             final_url = resp.url
#
#             logger.info("[RESP] %s -> %s", url, resp.status_code)
#
#             if resp.status_code >= 400:
#                 logger.error("[ERROR] %s -> %s", url, resp.status_code)
#
#             resp.raise_for_status()
#             return resp.text, final_url
#
#         except Exception as e:
#             logger.error("[EXCEPTION] %s -> %s", url, e)
#             metrics.inc("failed_requests")
#             raise
#         finally:
#             metrics.add_latency(time.time() - start)

# src/crawler/fetcher.py

import logging
import time
import requests

from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from src.config import settings
from src.pipeline.models import metrics

logger = logging.getLogger("fetcher")


class Fetcher:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": settings.user_agent,
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

    def _do_request(self, url: str):
        start = time.perf_counter()

        try:
            resp = self.session.get(url, timeout=settings.timeout, allow_redirects=True)

            resp.raise_for_status()
            metrics.inc("success_requests")
            return resp

        except Exception:
            metrics.inc("failed_requests")
            raise

        finally:
            metrics.inc("total_requests")  # 每次 HTTP attempt 都算
            metrics.add_latency(time.perf_counter() - start)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get(self, url: str) -> tuple[str, str]:
        logger.info("[GET] %s", url)

        resp = self._do_request(url)
        return resp.text, resp.url