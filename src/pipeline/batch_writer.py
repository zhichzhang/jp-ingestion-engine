# src/pipeline/batch_writer.py

from __future__ import annotations

import logging

from src.db.repository import Repository
from src.pipeline.models import WineryRecord, ProductRecord, MediaRecord

logger = logging.getLogger("batch")


class BatchWriter:
    def __init__(self, repository: Repository) -> None:
        self.repository = repository
        self.winery_id_map: dict[str, str] = {}

    def _single_winery_id(self) -> str | None:
        if not self.winery_id_map:
            return None
        if len(self.winery_id_map) == 1:
            return next(iter(self.winery_id_map.values()))
        return self.winery_id_map.get("joseph perrier")

    def flush(
        self,
        wineries: list[WineryRecord],
        products: list[ProductRecord],
        media: list[MediaRecord],
    ) -> None:
        logger.info(
            "[FLUSH] wineries=%d products=%d media=%d",
            len(wineries),
            len(products),
            len(media),
        )

        if wineries:
            new_winery_map = self.repository.upsert_wineries(wineries)
            self.winery_id_map.update(new_winery_map)

        winery_id = self._single_winery_id()

        if products:
            self.repository.upsert_products(products, winery_id=winery_id)

        if media:
            self.repository.upsert_media(media)

        logger.info("[FLUSH DONE] winery_ids=%d", len(self.winery_id_map))