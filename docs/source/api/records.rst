Records
=======

Records can hold arbitrary information. They are quite useful when precessing data, since
information can be easily added to a record.

.. code-block:: python

    @define
    class AItem(Item):
        a: int


    @define
    class A1Item(AItem):
        a1: int


    @define
    class BItem(Item):
        b: int


    @define
    class CItem(Item):
        c: int


    @recordtypes(A1Item)
    class ARecord(Record):
        ...


    @recordtypes(BItem)
    class ABRecord(ARecord):
        ...


    record = ABRecode(AItem(1), BItem(2))
    print(record[AItem].a)  # 1


    record = record.update(BItem(3))
    print(record[BItem])  # 3


.. autoclass:: datamaestro.record.Item
    :members:

.. autoclass:: datamaestro.record.Record
    :members: update, has, get, from_types

.. autofunction:: datamaestro.record.recordtypes

.. autoclass:: datamaestro.record.RecordTypesCache
    :members: __init__, update

.. autoclass:: datamaestro.record.SingleRecordTypeCache
    :members: __init__, update
