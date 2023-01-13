from __future__ import annotations
from datetime import date
from typing import Optional

from src.allocation.domain import model
from src.allocation.adapters.repository import AbstractRepository


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def allocate(line: model.OrderLine, repo: AbstractRepository, session) -> str:
    batches = repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f"Invalid sku {line.sku}")
    batchref = model.allocate(line, batches)
    session.commit()
    return batchref


def add_batch(
    ref: str, sku: str, qty: int, eta: Optional[date], repo: AbstractRepository, session
):
    batch = model.Batch(ref, sku, qty, eta)
    repo.add(batch)
    session.commit()


def deallocate(line: model.OrderLine, repo: AbstractRepository, session):
    for batch in repo.list():
        if batch.sku == line.sku and line in batch._allocations:
            batch.deallocate(line)
            session.commit()
            return
    raise Exception("Batch not allocated")
