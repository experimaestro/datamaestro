import pickle
from datamaestro.record import Item, record_type
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
class B1Item(BItem):
    b1: int


@define
class CItem(Item):
    c: int


ARecord = record_type(AItem)
BaseRecord = ARecord.sub(A1Item)
MyRecord = BaseRecord.sub(BItem)


def test_record_simple():
    a = A1Item(1, 2)
    b = BItem(4)
    r = MyRecord(a, b)
    assert r[AItem] is a
    assert r[A1Item] is a
    assert r[BItem] is b


def test_record_missing_init():
    with pytest.raises(KeyError):
        # A1Item is missing
        MyRecord(AItem(1), BItem(2))

    with pytest.raises(KeyError):
        MyRecord(A1Item(1, 2))


def test_record_update():
    a = A1Item(1, 2)
    b = BItem(4)
    r = MyRecord(a, b)

    r2 = r.update(BItem(3))
    assert r is not r2
    assert r2[BItem] is not b


def test_record_pickled():
    # First,
    MyRecord2 = BaseRecord.sub(BItem)
    r = MyRecord2(A1Item(1, 2), BItem(2))
    r = pickle.loads(pickle.dumps(r))

    assert r[A1Item].a == 1
    assert r[BItem].b == 2
