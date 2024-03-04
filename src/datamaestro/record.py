import logging
from typing import ClassVar, Type, TypeVar, Dict, List, Union, Optional, Set


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


class Record:
    """Associate types with entries

    A record is a composition of items; each item base class is unique.
    """

    items: Items

    unpickled: bool = False
    """Flags unpickled records"""

    def __init__(
        self, *items: Union[Items, T], override=False, unpickled=False, cls=None
    ):
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

        if unpickled:
            self.unpickled = True
        else:
            self.validate(cls or self.__class__)

    def __new__(cls, *items: Union[Items, T], override=False, unpickled=False):
        # Without this, impossible to pickle objects
        if cls.__trueclass__ is not None:
            record = object.__new__(cls.__trueclass__)
            record.__init__(*items, cls=cls, override=override, unpickled=unpickled)
            return record

        return object.__new__(cls)

    def __str__(self):
        return (
            "{"
            + ", ".join(f"{key}: {value}" for key, value in self.items.items())
            + "}"
        )

    def __getstate__(self):
        return self.items

    def __setstate__(self, state):
        self.unpickled = True
        self.items = state

    def validate(self, cls: Type["Record"] = None):
        """Validate the record"""
        cls = cls if cls is not None else self.__class__

        if cls.itemtypes:
            for itemtype in cls.itemtypes:
                try:
                    self.__getitem__(itemtype)
                except KeyError:
                    raise KeyError(f"Item of type {itemtype} is missing")

        if len(self.items) != len(cls.itemtypes):
            unregistered = [
                item
                for item in self.items.values()
                if all(
                    not issubclass(item.__get_base__(), itemtype)
                    for itemtype in cls.itemtypes
                )
            ]
            raise RuntimeError(
                f"The record {cls} contains unregistered items: {unregistered}"
            )

    def get(self, key: Type[T]) -> Optional[T]:
        try:
            return self[key]
        except KeyError:
            return None

    def has(self, key: Type[T]) -> bool:
        return key.__get_base__() in self.items

    def __getitem__(self, key: Type[T]) -> T:
        """Get an item given its type"""
        base = key.__get_base__()
        entry = self.items[base]

        # Check if this matches the expected class
        if not isinstance(entry, key):
            raise KeyError(f"No entry with type {key}")
        return entry

    def update(self, *items: T) -> "Record":
        """Update some items"""
        # Create our new dictionary
        item_dict = {**self.items}
        for item in items:
            item_dict[item.__get_base__()] = item

        return self.__class__(item_dict)

    # --- Class methods and variables

    itemtypes: ClassVar[Set[Type[T]]] = []
    """For specific records, this is the list of types. The list is empty when
    no validation is used (e.g. pickled records created on the fly)"""

    __trueclass__: ClassVar[Optional[Type["Record"]]] = None
    """The last class in the type hierarchy corresponding to an actual type,
    i.e. not created on the fly"""

    @classmethod
    def has_type(cls, itemtype: Type[T]):
        return any(issubclass(cls_itemtype, itemtype) for cls_itemtype in cls.itemtypes)

    @classmethod
    def _subclass(cls, *itemtypes: Type[T]):
        cls_itemtypes = set((x for x in getattr(cls, "itemtypes", [])))
        mapping = {
            ix: itemtype.__get_base__() for ix, itemtype in enumerate(cls_itemtypes)
        }

        for itemtype in itemtypes:
            if ix := mapping.get(itemtype.__get_base__(), None):
                cls_itemtypes[ix] = itemtype
            else:
                cls_itemtypes.add(itemtype)
        return cls_itemtypes

    @classmethod
    def from_types(cls, name: str, *itemtypes: Type[T], module: str = None):
        extra_dict = {}
        if module:
            extra_dict["__module__"] = module

        return type(
            name,
            (cls,),
            {
                **extra_dict,
                "__trueclass__": cls.__trueclass__ or cls,
                "itemtypes": cls._subclass(*itemtypes),
            },
        )


def recordtypes(*types: List[Type[T]]):
    """Adds types for a new record class"""

    def decorate(cls: Type[Record]):
        (base_cls,) = [base for base in cls.__bases__ if issubclass(base, Record)]

        setattr(cls, "itemtypes", base_cls._subclass(*types))
        return cls

    return decorate


class RecordTypesCache:
    """Class to use when new record types need to be created on the fly by
    adding new items"""

    def __init__(self, name: str, *itemtypes: Type[T], module: str = None):
        self._module = module
        self._name = name
        self._itemtypes = itemtypes
        self._cache: Dict[Type[Record], Type[Record]] = {}
        self._unpickled_warnings = False

    def get(self, record_type: Type[Record], unpickled=False):
        if updated_type := self._cache.get(record_type, None):
            return updated_type

        updated_type = record_type.from_types(
            f"{self._name}_{record_type.__name__}",
            *self._itemtypes,
            module=self._module,
        )
        self._cache[record_type] = updated_type
        return updated_type

    def update(self, record: Record, *items: Item):
        if record.unpickled and not self._unpickled_warnings:
            # In that case, impossible to recover the hierarchy
            logging.warning(
                "Updating a pickled record is not recommended:"
                " prefer using well defined record types"
            )
            self._unpickled_warnings = True

        return self.get(record.__class__)(
            *record.items.values(), *items, override=True, unpickled=record.unpickled
        )
