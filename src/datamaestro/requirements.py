"""Optional Python requirements of datasets and data types.

Some datasets need Python packages that are not dependencies of the
repository that defines them (e.g. HuggingFace ``datasets``). Declaring them
lets datamaestro check they are installed *before* downloading anything, and
tell the user what to install::

    from datamaestro.definitions import Dataset, dataset

    @dataset(url="...", requires={"datasets": "datamaestro_ir[huggingface]"})
    class MyDataset(Dataset):
        ...

Requirements can also be attached to a data type with :func:`requires`; they
then apply to every dataset whose configuration is of that type (or of a
subclass)::

    @requires("datasets")
    class HuggingFaceDataset(Base):
        ...

Inside code that uses an optional module, :func:`require` raises a
:class:`MissingRequirementError` with the installation hint when it is
missing.
"""

import importlib.util
import sys
from dataclasses import dataclass
from typing import Iterable, Mapping, Optional, Sequence, TypeVar, Union

__all__ = [
    "MissingRequirementError",
    "Requirement",
    "RequirementsSpec",
    "missing_requirements",
    "parse_requirements",
    "require",
    "requires",
    "type_requirements",
]

#: Attribute (on data types) holding the requirements declared by
#: :func:`requires`
REQUIRES_ATTRIBUTE = "__datamaestro_requires__"


@dataclass(frozen=True)
class Requirement:
    """A Python module that must be importable"""

    module: str
    """The (top-level or dotted) module name, e.g. ``datasets``"""

    install: Optional[str] = None
    """What to install to get the module, as a Python requirement (e.g.
    ``datasets>=2`` or ``datamaestro_ir[huggingface]``, to be used with
    any package manager); defaults to the top-level module name"""

    @property
    def install_spec(self) -> str:
        return self.install or self.module.split(".")[0]

    def missing_reason(self) -> Optional[str]:
        """Returns why the module cannot be used (None if it can)

        The module is looked up without being imported. A namespace package
        (a directory without ``__init__.py``) is reported as missing: this is
        what an empty directory named like the module, in the current
        directory, looks like, and it shadows the real package.
        """
        loaded = sys.modules.get(self.module)
        if loaded is not None and getattr(loaded, "__file__", True) is not None:
            # Already imported (or injected, e.g. in tests)
            return None

        try:
            spec = importlib.util.find_spec(self.module)
        except (ImportError, ValueError):
            spec = None
        if spec is None:
            return "not installed"
        if spec.origin is None or spec.origin == "namespace":
            locations = list(spec.submodule_search_locations or [])
            where = ", ".join(str(p) for p in locations) or "?"
            return (
                f"only found as a namespace package ({where}), which might be "
                "a directory shadowing the installed package"
            )
        return None

    def __str__(self):
        if self.install_spec == self.module:
            return self.module
        return f"{self.module} (from {self.install_spec})"


#: Accepted ways to specify requirements: a module name, a
#: :class:`Requirement`, a sequence of these, a mapping module -> install
#: specification, or a data type (its :func:`requires` declarations)
RequirementsSpec = Union[
    type,
    str,
    Requirement,
    Sequence[Union[str, Requirement]],
    Mapping[str, Optional[str]],
    None,
]


def parse_requirements(spec: RequirementsSpec) -> tuple[Requirement, ...]:
    """Normalizes a requirement specification"""
    if spec is None:
        return ()
    if isinstance(spec, type):
        return type_requirements(spec)
    if isinstance(spec, (str, Requirement)):
        spec = [spec]
    if isinstance(spec, Mapping):
        return tuple(Requirement(module, install) for module, install in spec.items())

    result = []
    for item in spec:
        if isinstance(item, Requirement):
            result.append(item)
        elif isinstance(item, str):
            result.append(Requirement(item))
        else:
            raise TypeError(f"Invalid requirement specification: {item!r}")
    return tuple(result)


def _unique(requirements: Iterable[Requirement]) -> tuple[Requirement, ...]:
    """Removes duplicate modules (the first declaration wins)"""
    seen = {}
    for requirement in requirements:
        seen.setdefault(requirement.module, requirement)
    return tuple(seen.values())


class MissingRequirementError(ImportError):
    """Raised when required Python modules are missing"""

    def __init__(self, requirements: Sequence[Requirement], what: str = ""):
        self.requirements = tuple(requirements)
        lines = [
            f"Missing Python requirements{' for ' + what if what else ''}:",
        ]
        for requirement in self.requirements:
            lines.append(f"  - {requirement.module}: {requirement.missing_reason()}")
        installs = " ".join(sorted({r.install_spec for r in self.requirements}))
        lines.append(f"Please install: {installs}")
        super().__init__(
            "\n".join(lines),
            name=self.requirements[0].module if self.requirements else None,
        )


def missing_requirements(
    requirements: Iterable[Requirement],
) -> list[Requirement]:
    """Returns the requirements whose module cannot be used"""
    return [r for r in requirements if r.missing_reason() is not None]


def require(*specs: RequirementsSpec, what: str = "") -> None:
    """Checks that modules are available

    :param specs: The requirements (see :data:`RequirementsSpec`)
    :param what: What needs the requirements (used in the error message)
    :raises MissingRequirementError: if some modules cannot be used
    """
    requirements = _unique(r for spec in specs for r in parse_requirements(spec))
    if missing := missing_requirements(requirements):
        raise MissingRequirementError(missing, what)


T = TypeVar("T", bound=type)


def requires(*specs: RequirementsSpec):
    """Class decorator declaring the requirements of a data type

    Requirements are inherited: a dataset whose configuration type derives
    from a decorated class has these requirements too.
    """
    requirements = _unique(r for spec in specs for r in parse_requirements(spec))

    def decorate(cls: T) -> T:
        own = cls.__dict__.get(REQUIRES_ATTRIBUTE, ())
        setattr(cls, REQUIRES_ATTRIBUTE, _unique((*own, *requirements)))
        return cls

    return decorate


def type_requirements(cls: Optional[type]) -> tuple[Requirement, ...]:
    """Returns the requirements of a data type (including inherited ones)"""
    if cls is None:
        return ()
    return _unique(
        r
        for c in getattr(cls, "__mro__", (cls,))
        for r in c.__dict__.get(REQUIRES_ATTRIBUTE, ())
    )
