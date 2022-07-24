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
        self.session.execute('INSERT INTO batches (reference, sku, _purchased_quantity, eta) VALUES ({}, {}, {}, {})'.format(batch.reference, batch.sku, batch._purchased_quantity, batch.eta))

    def get(self, reference) -> model.Batch:
        # self.session.execute('SELECT ??
        ref, sku, pq, eta = self.session.execute('SELECT reference, sku, _purchased_quantity, eta FROM "batches"') 
        return model.Batch(ref, sku, pq, eta)
