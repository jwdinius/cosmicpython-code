import abc
from allocation.domain import model


class AbstractRepository(abc.ABC):

    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference=None, sku=None) -> model.Batch:
        raise NotImplementedError



class SqlAlchemyRepository(AbstractRepository):

    def __init__(self, session):
        self.session = session

    def add(self, batch):
        self.session.add(batch)

    def get(self, reference=None, sku=None):
        assert reference is not None or sku is not None
        if reference is not None:
            return self.session.query(model.Batch).filter_by(reference=reference).one()
        else:
            return self.session.query(model.Batch).filter_by(sku=sku).one()


    def list(self):
        return self.session.query(model.Batch).all()
