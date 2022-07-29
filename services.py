from __future__ import annotations
from datetime import date
from typing import Optional, List, Set

import model
from model import OrderLine, OrderLineNotAllocated
from repository import AbstractRepository


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def allocate(line: OrderLine, repo: AbstractRepository, session) -> str:
    batches = repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f"Invalid sku {line.sku}")
    batchref = model.allocate(line, batches)
    session.commit()
    return batchref


def deallocate(orderid: str, repo: AbstractRepository, session) -> bool:
    batches = repo.list()
    try:
        model.deallocate(orderid, batches)
        session.commit()
    except (OrderLineNotAllocated) as e:
        print(e)
        return False
    return True


#services.add_batch("b1", "BLUE-PLINTH", 100, None, repo, session)
def add_batch(batchref: str, sku: str, qty: int, eta: Optional[date], repo: AbstractRepository, session) -> None:
    batch = model.Batch(batchref, sku, qty, eta)
    repo.add(batch)
    session.commit()
