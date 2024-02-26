from datamaestro.record import Record, Item, recordtypes
from attrs import define
import pytest


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


class MyRecord(Record):
    itemtypes = [A1Item, BItem]


@recordtypes(CItem)
class MyRecord2(MyRecord):
    pass


def test_record_simple():
    a = A1Item(1, 2)
    b = BItem(4)
    r = MyRecord(a, b)
    assert r[AItem] is a
    assert r[A1Item] is a
    assert r[BItem] is b


def test_record_missing_init():
    with pytest.raises(KeyError):
        MyRecord(AItem(1), BItem(2))

    with pytest.raises(KeyError):
        MyRecord(A1Item(1, 2))


def test_record_update():
    a = A1Item(1, 2)
    b = BItem(4)
    r = MyRecord(a, b)

    r2 = r.add(BItem(3))
    assert r is not r2
    assert r2[BItem] is not b


def test_record_decorator():
    MyRecord2(A1Item(1, 2), BItem(2), CItem(3))


def test_record_newtype():
    MyRecord2 = MyRecord.from_types("MyRecord2", CItem)
    r = MyRecord2(A1Item(1, 2), BItem(2), CItem(3))

    # For a dynamic class, we should have the same MyRecord type
    assert r.__class__ is MyRecord
