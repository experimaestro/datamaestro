from typing import Type, TypeVar, Dict, Union, Optional


class Item:
    """Base class for all item types"""

    @classmethod
    def __get_base__(cls: Type) -> Type:
        """Get the most generic superclass for this type of item"""
        if base := getattr(cls, "__base__cache__", None):
            return base

        base = cls
        for supercls in cls.__mro__:
            if issubclass(supercls, Item) and supercls is not Item:
                base = supercls
        setattr(cls, "__base__cache__", base)
        return base


T = TypeVar("T", bound=Item)
Items = Dict[Type[T], T]


class RecordType:
    def __init__(self, *item_types: Type[T]):
        self.item_types = frozenset(item_types)
        self.mapping = {item_type.__get_base__(): item_type for item_type in item_types}

    def __repr__(self):
        return f"""Record({",".join(item_type.__name__ for item_type in
                self.item_types)})"""

    def contains(self, other: "RecordType"):
        """Checks that each item type in other has an item type of a compatible
        type in self"""
        if len(self.item_types) != len(other.item_types):
            return False

        for item_type in other.item_types:
            if matching_type := self.mapping.get(item_type.__get_base__(), None):
                if not issubclass(matching_type, item_type):
                    return False
            else:
                return False

        return True

    def sub(self, *item_types: Type[T]):
        """Returns a new record type based on self and new item types"""
        cls_itemtypes = [x for x in self.item_types]
        mapping = {
            itemtype.__get_base__(): ix for ix, itemtype in enumerate(cls_itemtypes)
        }

        for itemtype in item_types:
            if (ix := mapping.get(itemtype.__get_base__(), -1)) >= 0:
                cls_itemtypes[ix] = itemtype
            else:
                cls_itemtypes.append(itemtype)

        return record_type(*cls_itemtypes)

    def __call__(self, *items: T):
        record = Record(*items)
        self.validate(record)
        return record

    def has(self, itemtype: Type[T]):
        return issubclass(self.mapping[itemtype.__get_base__()], itemtype)

    def validate(self, record: "Record"):
        """Creates and validate a new record of this type"""
        if self.item_types:
            for item_type in self.item_types:
                try:
                    record.__getitem__(item_type)
                except KeyError:
                    raise KeyError(f"Item of type {item_type} is missing")

        if len(record.items) != len(self.item_types):
            unregistered = [
                item
                for item in record.items.values()
                if all(
                    not issubclass(item.__get_base__(), item_type)
                    for item_type in self.item_types
                )
            ]
            raise KeyError(
                f"The record of type {self} contains unregistered items: {unregistered}"
            )

        # Creates a new record
        return record


def record_type(*item_types: Type[T]):
    """Returns a new record type"""
    return RecordType(*item_types)


class Record:
    """Associate types with entries

    A record is a composition of items; each item base class is unique.
    """

    #: Items for this record
    items: Items

    def __init__(self, *items: Union[Items, T], override=False):
        self.items = {}

        if len(items) == 1 and isinstance(items[0], dict):
            # Just copy the dictionary
            self.items = items[0]
        else:
            for entry in items:
                # Returns a new record if the item exists
                base = entry.__get_base__()
                if not override and base in self.items:
                    raise RuntimeError(
                        f"The item type {base} ({entry.__class__})"
                        " is already in the record"
                    )
                self.items[base] = entry

    def __str__(self):
        return (
            "{"
            + ", ".join(
                f"{key.__module__}.{key.__qualname__}: {value}"
                for key, value in self.items.items()
            )
            + "}"
        )

    def __repr__(self):
        return (
            "{"
            + ", ".join(
                f"{key.__module__}.{key.__qualname__}: {repr(value)}"
                for key, value in self.items.items()
            )
            + "}"
        )

    def get(self, key: Type[T]) -> Optional[T]:
        """Get a given item or None if it does not exist"""
        try:
            return self[key]
        except KeyError:
            return None

    def has(self, key: Type[T]) -> bool:
        """Returns True if the record has the given item type"""
        return key.__get_base__() in self.items

    def __getitem__(self, key: Type[T]) -> T:
        """Get an item given its type"""
        base = key.__get_base__()
        try:
            entry = self.items[base]
        except KeyError:
            raise KeyError(
                f"""No entry with type {key}: """
                f"""{",".join(str(s) for s in self.items.keys())}"""
            )

        # Check if this matches the expected class
        if not isinstance(entry, key):
            raise KeyError(
                f"""No entry with type {key}: """
                f"""{",".join(str(s) for s in self.items.keys())}"""
            )
        return entry

    def update(self, *items: T, target: RecordType = None) -> "Record":
        """Update some items"""
        # Create our new dictionary
        item_dict = {**self.items}
        for item in items:
            item_dict[item.__get_base__()] = item

        return Record(item_dict)
