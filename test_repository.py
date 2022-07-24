# pylint: disable=protected-access
import model
import repository


def test_repository_can_save_a_batch(session):
    batch = model.Batch("batch1", "RUSTY-SOAPDISH", 100, eta=None)

    repo = repository.SqlRepository(session)
    repo.add(batch)
    session.commit()

    rows = session.execute(
        'SELECT reference, sku, _purchased_quantity, eta FROM "batches"'
    )
    assert list(rows) == [("batch1", "RUSTY-SOAPDISH", 100, None)]

def insert_order_line(session, order_line=model.OrderLine("order1", "GENERIC-SOFA", 12)):
    session.execute(
        "INSERT INTO order_lines (orderid, sku, qty)"
        ' VALUES (\"{}\", \"{}\", {})'.format(order_line.orderid, order_line.sku, order_line.qty)
    )
    [[orderline_id]] = session.execute(
        "SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku",
        dict(orderid=order_line.orderid, sku=order_line.sku),
    )
    return orderline_id

def insert_batch(session, batch=model.Batch("batch1", "GENERIC-SOFA", 100, None)):
    if batch.eta is None:
        session.execute(
            "INSERT INTO batches (reference, sku, _purchased_quantity, eta)"
            ' VALUES (\"{}\", \"{}", {}, null)'.format(batch.reference, batch.sku, batch._purchased_quantity)
        )
    else:
        session.execute(
            "INSERT INTO batches (reference, sku, _purchased_quantity, eta)"
            ' VALUES (\"{}\", \"{}", {}, {})'.format(batch.reference, batch.sku, batch._purchased_quantity, batch.eta)
        )
    print(list(session.execute(
        'SELECT id FROM batches WHERE reference=\"{}\" AND sku=\"{}\"'.format(batch.reference, batch.sku)
    )))

    [[batch_id]] = session.execute(
        'SELECT id FROM batches WHERE reference=\"{}\" AND sku=\"{}\"'.format(batch.reference, batch.sku)
    )
    return batch_id

'''
def insert_order_line(session):
    session.execute(
        "INSERT INTO order_lines (orderid, sku, qty)"
        ' VALUES ("order1", "GENERIC-SOFA", 12)'
    )
    [[orderline_id]] = session.execute(
        "SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku",
        dict(orderid="order1", sku="GENERIC-SOFA"),
    )
    return orderline_id


def insert_batch(session, batch_id):
    session.execute(
        "INSERT INTO batches (reference, sku, _purchased_quantity, eta)"
        ' VALUES (:batch_id, "GENERIC-SOFA", 100, null)',
        dict(batch_id=batch_id),
    )
    [[batch_id]] = session.execute(
        'SELECT id FROM batches WHERE reference=:batch_id AND sku="GENERIC-SOFA"',
        dict(batch_id=batch_id),
    )
    return batch_id
'''

def insert_allocation(session, orderline_id, batch_id):
    session.execute(
        "INSERT INTO allocations (orderline_id, batch_id)"
        " VALUES (:orderline_id, :batch_id)",
        dict(orderline_id=orderline_id, batch_id=batch_id),
    )


def test_repository_can_retrieve_a_batch_with_allocations(session):
    orderline_id = insert_order_line(session)
    batch1_id = insert_batch(session)
    insert_batch(session, model.Batch("batch2", "GENERIC-SOFA", 100, None))
    insert_allocation(session, orderline_id, batch1_id)

    repo = repository.SqlRepository(session)
    retrieved = repo.get("batch1")

    expected = model.Batch("batch1", "GENERIC-SOFA", 100, eta=None)
    assert retrieved == expected  # Batch.__eq__ only compares reference
    assert retrieved.sku == expected.sku
    assert retrieved._purchased_quantity == expected._purchased_quantity
    assert retrieved._allocations == {
        model.OrderLine("order1", "GENERIC-SOFA", 12),
    }


def get_allocations(session, batchid):
    rows = list(
        session.execute(
            "SELECT orderid"
            " FROM allocations"
            " JOIN order_lines ON allocations.orderline_id = order_lines.id"
            " JOIN batches ON allocations.batch_id = batches.id"
            " WHERE batches.reference = :batchid",
            dict(batchid=batchid),
        )
    )
    return {row[0] for row in rows}


def test_updating_a_batch(session):
    order1 = model.OrderLine("order1", "WEATHERED-BENCH", 10)
    order2 = model.OrderLine("order2", "WEATHERED-BENCH", 20)
    batch = model.Batch("batch1", "WEATHERED-BENCH", 100, eta=None)

    repo = repository.SqlRepository(session)
    repo.add(batch)
    orderline_id = insert_order_line(session, order1)
    [[batch_id]] = session.execute(
        'SELECT id FROM batches WHERE reference=\"{}\" AND sku=\"{}\"'.format(batch.reference, batch.sku)
    )
    #batch_id = insert_batch(session, batch)
    batch.allocate(order1)
    insert_allocation(session, orderline_id, batch_id)
    session.commit()

    repo.add(batch)
    orderline2_id = insert_order_line(session, order2)
    batch.allocate(order2)
    insert_allocation(session, orderline2_id, batch_id)
    session.commit()

    assert get_allocations(session, batch.reference) == {"order1", "order2"}
