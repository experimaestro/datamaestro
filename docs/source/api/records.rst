Records
=======

Records are flexible ways to compose information coming from various sources. For instance,
your processing chain can produce records only containing an ID. Later, you add can retrieve
the item content and add it to the record. Further in the processing, you would want to add
some transformation of the item content.

Records allow to perform this type of transformations by holding a set of **items**. Record types
form a lattice of types so that checking that some item types are present in an item is easy.

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



    record = Record(AItem(1), BItem(2))
    print(record[AItem].a)  # 1
    print(record[BItem].b)  # 1

    # records types are only defined by their item types
    other_record = Record(A1Item(1), BItem(2))

    # records can be updated
    new_record = record.update(BItem(3), CItem(4))
    print(new_record[BItem].b)  # 3
    print(new_record[CItem].c)  # 4

    # records only hold one instance of a given item
    # base type
    new_record_a1 = record.update(A1Item(3, 4))
    print(new_record[AItem].a)  # 3
    print(new_record[A1Item].a)  # 3
    print(new_record[A1Item].a1)  # 4


Working with record types
*************************

Record types form a lattice of types that can be used to check
record properties before hand.

.. code-block:: python

    ABRecord = record_type(AItem, BItem)
    AB1Record = record_type(AItem, B1Item)

    # Hierarchy-based check
    assert ABRecord.contains(AB1Record)

    # Checks for specific types
    assert ABRecord.has(AItem, BItem)

Validating
**********

To ensure that a record fills the requested property,
one can use record types

.. code-block:: python

    ABRecord = record_type(AItem, BItem)

    # OK
    ABRecord(AItem(1), BItem(2))

    # Fails: A1Item is not AItem
    ABRecord(A1Item(1), BItem(2))

    # Fails: AItem is not present
    ABRecord(BItem(2))

When updating, it is also possible to validate

.. code-block:: python

    A1BRecord = record_type(A1Item, BItem)
    record = Record(AItem(1), BItem(2))

    # Update the ABRecord into a A1/B one
    record.update(A1Item(1, 2), target=A1BRecord)


API
***

.. autoclass:: datamaestro.record.Item

.. autoclass:: datamaestro.record.RecordType
    :members: __call__, validate, sub

.. autoclass:: datamaestro.record.Record
    :members: update, has, get

.. autofunction:: datamaestro.record.record_type
