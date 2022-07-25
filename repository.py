import abc
import model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        # self.session.execute('INSERT INTO ??
        # TODO(jwd) need to commit allocations here too
        rows = list(self.session.execute('SELECT reference, sku, _purchased_quantity, eta FROM batches WHERE reference = \'{}\''.format(batch.reference)))
        if not rows:
            if batch.eta is None:
                self.session.execute('INSERT INTO batches (id, reference, sku, _purchased_quantity, eta) VALUES (NULL, \"{}\", \"{}\", {}, NULL)'.format(batch.reference, batch.sku, batch._purchased_quantity))
            else:
                self.session.execute('INSERT INTO batches (id, reference, sku, _purchased_quantity, eta) VALUES (NULL, \"{}\", \"{}\", {}, {})'.format(batch.reference, batch.sku, batch._purchased_quantity, batch.eta))
        else:
            # update existing batch
            if batch.eta is None:
                self.session.execute('UPDATE batches SET sku=\"{}\", _purchased_quantity={} WHERE reference=\"{}\"'.format(batch.sku, batch._purchased_quantity, batch.reference))
            else:
                self.session.execute('UPDATE batches SET sku=\"{}\", _purchased_quantity={}, eta={} WHERE reference=\"{}\"'.format(batch.sku, batch._purchased_quantity, batch.reference, batch.eta))
        #self.session.commit()  # XXX(jwd) are these needed here?
        [[batch_id]] = self.session.execute(
            'SELECT id FROM batches WHERE reference=\"{}\" AND sku=\"{}\"'.format(batch.reference, batch.sku)
        )
        # need to get allocations
        for allocation in batch.allocations:
            # check if orderline already in database
            try:
                [[orderline_id]] = self.session.execute(
                    "SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku",
                    dict(orderid=allocation.orderid, sku=allocation.sku),
                )
            except:
                self.session.execute(
                    "INSERT INTO order_lines (orderid, sku, qty)"
                    ' VALUES (\"{}\", \"{}\", {})'.format(allocation.orderid, allocation.sku, allocation.qty)
                )
                #self.session.commit()
            [[orderline_id]] = self.session.execute(
                "SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku",
                dict(orderid=allocation.orderid, sku=allocation.sku),
            )
            # set allocation
            self.session.execute(
                "INSERT INTO allocations (orderline_id, batch_id)"
                " VALUES (:orderline_id, :batch_id)",
                dict(orderline_id=orderline_id, batch_id=batch_id),
            )

    def get(self, reference) -> model.Batch:
        # self.session.execute('SELECT ??
        rows= list(self.session.execute('SELECT reference, sku, _purchased_quantity, eta FROM batches WHERE reference = \'{}\''.format(reference)))
        if len(rows) != 1:
            return None  # no batch exists or is ambiguous
        ref, sku, pq, eta = rows[0]
        batch = model.Batch(ref, sku, pq, eta) 
        # need to get allocations
        rows = list(
            self.session.execute(
                "SELECT orderid"
                " FROM allocations"
                " JOIN order_lines ON allocations.orderline_id = order_lines.id"
                " JOIN batches ON allocations.batch_id = batches.id"
                " WHERE batches.reference = :batchid",
                dict(batchid=ref),
            )
        )
        order_ids = {row[0] for row in rows}
        print(order_ids)
        for oid in order_ids:
            sku, qty, order_id = list(self.session.execute(
                "SELECT sku, qty, orderid"
                " FROM order_lines"
                " WHERE orderid = :orderid",
                dict(orderid=oid),
            ))[0]
            order_line = model.OrderLine(order_id, sku, qty)
            batch.allocate(order_line)
        return batch 
