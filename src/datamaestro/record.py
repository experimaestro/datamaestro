from typing import ClassVar, Type, TypeVar, Dict, List, Union, Optional


class Item:
    """Base class for all item types"""

    @classmethod
    def __get_base__(cls: Type) -> Type:
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
    """Associate types with entries"""

    items: Items

    def __init__(self, *items: Union[Items, T], no_check=False):
        self.items = {}

        if len(items) == 1 and isinstance(items[0], dict):
            self.items = items[0]
        else:
            for item in items:
                self.add(item, update_only=True)

        # Check if the record is constructured
        if not no_check:
            self.validate()

    def __new__(cls, *items: Union[Items, T], no_check=False):
        # Without this, impossible to pickle objects
        if cls.__trueclass__ is not None:
            record = object.__new__(cls.__trueclass__)
            record.__init__(*items, no_check=True)
            if not no_check:
                record.validate(cls=cls)
            return record

        return object.__new__(cls)

    def __str__(self):
        return (
            "{"
            + ", ".join(f"{key}: {value}" for key, value in self.items.items())
            + "}"
        )

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

    def __getitem__(self, key: Type[T]) -> T:
        """Get an item given its type"""
        base = key.__get_base__()
        entry = self.items[base]

        # Check if this matches the expected class
        if not isinstance(entry, key):
            raise KeyError(f"No entry with type {key}")
        return entry

    def add(self, *entries: T, update_only=False, no_check=False) -> "Record":
        """Update the record with this new entry, returns a new record if
        it exists"""

        for entry in entries:
            # Returns a new record if the item exists
            base = entry.__get_base__()
            if base in self.items:
                if update_only:
                    raise RuntimeError(
                        f"The item type {base} ({entry.__class__})"
                        " is already in the record"
                    )
                return self.__class__({**self.items, base: entry}, no_check=no_check)

            # No, just update
            self.items[base] = entry
        return self

    # --- Class methods and variables

    itemtypes: ClassVar[List[Type[T]]] = []
    """For specific records, this is the list of types"""

    __trueclass__: ClassVar[Optional[Type["Record"]]] = None
    """True when the class is defined in a module"""

    @classmethod
    def has_type(cls, itemtype: Type[T]):
        return any(issubclass(cls_itemtype, itemtype) for cls_itemtype in cls.itemtypes)

    @classmethod
    def _subclass(cls, *itemtypes: Type[T]):
        cls_itemtypes = [x for x in getattr(cls, "itemtypes", [])]
        mapping = {
            ix: itemtype.__get_base__() for ix, itemtype in enumerate(cls_itemtypes)
        }

        for itemtype in itemtypes:
            if ix := mapping.get(itemtype.__get_base__(), None):
                cls_itemtypes[ix] = itemtype
            else:
                cls_itemtypes.append(itemtype)
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
