from __future__ import annotations
from typing import List, Dict, Callable, Type, TYPE_CHECKING
from allocation.domain import events
from . import handlers

if TYPE_CHECKING:
    from . import unit_of_work


def handle(
    event: events.Event,
    uow: unit_of_work.AbstractUnitOfWork,
):
    results = []
    queue = [event]
    while queue:
        event = queue.pop(0)
        for handler in HANDLERS[type(event)]:
            results.append(handler(event, uow=uow))
            queue.extend(uow.collect_new_events())
    return results


HANDLERS = {
    events.BatchCreated: [handlers.add_batch],
    events.BatchQuantityChanged: [handlers.change_batch_quantity],
    events.AllocationRequired: [handlers.allocate],
    events.OutOfStock: [handlers.send_out_of_stock_notification],
}  # type: Dict[Type[events.Event], List[Callable]]

class AbstractMessageBus:
    HANDLERS: Dict[Type[events.Event], List[Callable]]
    
    def __init__(self, uow):
        self._uow = uow

    def handle(self, event: events.Event):
        queue = [event]
        while queue:
            event = queue.pop(0)
            for handler in HANDLERS[type(event)]:
                handler(event, uow=self._uow)
                queue.extend(self._uow.collect_new_events())


class MessageBus(AbstractMessageBus):
    HANDLERS = HANDLERS
    
    def __init__(self, uow):
        super().__init__(uow)
