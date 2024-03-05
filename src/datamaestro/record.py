import logging
from typing import ClassVar, Type, TypeVar, Dict, List, Union, Optional, FrozenSet


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

        self.validate()

    @classmethod
    def from_record(cls, record: "Record", *items: T, override=True):
        """Build from another record"""
        return cls({**record.items, **{item.__get_base__(): item for item in items}})

    def __str__(self):
        return (
            "{"
            + ", ".join(f"{key}: {value}" for key, value in self.items.items())
            + "}"
        )

    def __reduce__(self):
        cls = self.__class__
        if cls.__trueclass__ is None:
            return (cls.__new__, (cls.__trueclass__ or cls,), {"items": self.items})

        return (
            cls.__new__,
            (cls.__trueclass__ or cls,),
            {"items": self.items, "itemtypes": self.itemtypes},
        )

    def __setstate__(self, state):
        self.items = state["items"]
        self.itemtypes = None

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
            raise KeyError(
                f"The record {cls} contains unregistered items: {unregistered}"
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
        entry = self.items[base]

        # Check if this matches the expected class
        if not isinstance(entry, key):
            raise KeyError(f"No entry with type {key}")
        return entry

    def is_pickled(self):
        return self.itemtypes is None

    def update(self, *items: T) -> "Record":
        """Update some items"""
        # Create our new dictionary
        item_dict = {**self.items}
        for item in items:
            item_dict[item.__get_base__()] = item

        return self.__class__(item_dict)

    # --- Class methods and variables

    itemtypes: ClassVar[Optional[FrozenSet[Type[T]]]] = []
    """For specific records, this is the list of types. The value is null when
    no validation is used (e.g. pickled records created on the fly)"""

    __trueclass__: ClassVar[Optional[Type["Record"]]] = None
    """The last class in the type hierarchy corresponding to an actual type,
    i.e. not created on the fly (only defined when the record is pickled)"""

    @classmethod
    def has_type(cls, itemtype: Type[T]):
        return any(issubclass(cls_itemtype, itemtype) for cls_itemtype in cls.itemtypes)

    @classmethod
    def _subclass(cls, *itemtypes: Type[T]):
        cls_itemtypes = [x for x in getattr(cls, "itemtypes", [])]
        mapping = {
            itemtype.__get_base__(): ix for ix, itemtype in enumerate(cls_itemtypes)
        }

        for itemtype in itemtypes:
            if (ix := mapping.get(itemtype.__get_base__(), -1)) >= 0:
                cls_itemtypes[ix] = itemtype
            else:
                cls_itemtypes.append(itemtype)

        return frozenset(cls_itemtypes)

    @classmethod
    def from_types(cls, name: str, *itemtypes: Type[T], module: str = None):
        """Construct a new sub-record type

        :param name: The name of the subrecord
        :param module: The module name, defaults to None
        :return: A new Record type
        """
        extra_dict = {}
        if module:
            extra_dict["__module__"] = module

        return type(
            name,
            (cls,),
            {
                **extra_dict,
                "itemtypes": frozenset(cls._subclass(*itemtypes)),
                "__trueclass__": cls.__trueclass__ or cls,
            },
        )

    __RECORD_TYPES_CACHE__: Dict[frozenset, Type["Record"]] = {}

    @staticmethod
    def fromitemtypes(itemtypes: FrozenSet[T]):
        if recordtype := Record.__RECORD_TYPES_CACHE__.get(itemtypes, None):
            return recordtype

        recordtype = Record.from_types(
            "_".join(itemtype.__name__ for itemtype in itemtypes), *itemtypes
        )
        Record.__RECORD_TYPES_CACHE__[itemtypes] = recordtype
        return recordtype


def recordtypes(*types: List[Type[T]]):
    """Adds types for a new record class"""

    def decorate(cls: Type[Record]):
        (base_cls,) = [base for base in cls.__bases__ if issubclass(base, Record)]

        setattr(cls, "itemtypes", base_cls._subclass(*types))
        return cls

    return decorate


class RecordTypesCacheBase:
    def __init__(self, name: str, *itemtypes: Type[T], module: str = None):
        self._module = module
        self._name = name
        self._itemtypes = itemtypes

    def _compute(self, record_type: Type[Record]):
        updated_type = record_type.from_types(
            f"{self._name}_{record_type.__name__}",
            *self._itemtypes,
            module=self._module,
        )
        return updated_type


class RecordTypesCache(RecordTypesCacheBase):
    """Class to use when new record types need to be created on the fly by
    adding new items"""

    def __init__(self, name: str, *itemtypes: Type[T], module: str = None):
        """Creates a new cache

        :param name: Base name for new record types
        :param module: The module name for new types, defaults to None
        """
        super().__init__(name, *itemtypes, module=module)
        self._cache: Dict[Type[Record], Type[Record]] = {}
        self._warning = False

    def __call__(self, record_type: Type[Record]):
        if (updated_type := self._cache.get(record_type, None)) is None:
            self._cache[record_type] = updated_type = self._compute(record_type)
        return updated_type

    def update(self, record: Record, *items: Item, cls=None):
        """Update the record with the given items

        :param record: The record to which we add items
        :param cls: The class of the record, useful if the record has been
            pickled, defaults to None
        :return: A new record with the extra items
        """
        if cls is None:
            cls = record.__class__
            if record.is_pickled() and not self._warning:
                logging.warning(
                    "Updating unpickled records is not recommended"
                    " (speed issues): use the pickle record class as the cls input"
                )
                itemtypes = frozenset(type(item) for item in record.items.values())
                cls = Record.fromitemtypes(itemtypes)
        else:
            assert (
                record.is_pickled()
            ), "cls can be used only when the record as been pickled"

        return self(cls)(*record.items.values(), *items, override=True)


class SingleRecordTypeCache(RecordTypesCacheBase):
    """Class to use when new record types need to be created on the fly by
    adding new items

    This class supposes that the input record type is always the same (no check
    is done to ensure this)"""

    def __init__(self, name: str, *itemtypes: Type[T], module: str = None):
        """Creates a new cache

        :param name: Base name for new record types
        :param module: The module name for new types, defaults to None
        """
        super().__init__(name, *itemtypes, module=module)
        self._cache: Optional[Type[Record]] = None

    def __call__(self, record_type: Type[Record]):
        if self._cache is None:
            self._cache = self._compute(record_type)
        return self._cache

    def update(self, record: Record, *items: Item, cls=None):
        """Update the record with the given items

        :param record: The record to which we add items
        :param cls: The class of the record, useful if the record has been
            pickled, defaults to None
        :return: A new record with the extra items
        """
        if self._cache is None:
            if cls is None:
                cls = record.__class__
                itemtypes = frozenset(type(item) for item in record.items.values())
                cls = Record.fromitemtypes(itemtypes)
            else:
                assert (
                    record.is_pickled()
                ), "cls can be used only when the record as been pickled"

        return self(cls)(*record.items.values(), *items, override=True)
