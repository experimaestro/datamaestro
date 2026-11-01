"""Tests for :mod:`datamaestro.requirements`."""

import sys
import types

import pytest

from datamaestro.data import Base
from datamaestro.definitions import Dataset, dataset
from datamaestro.requirements import (
    MissingRequirementError,
    Requirement,
    parse_requirements,
    require,
    requires,
    type_requirements,
)

MISSING = "datamaestro_test_surely_missing_module"


class TestParse:
    def test_string(self):
        assert parse_requirements("json") == (Requirement("json"),)

    def test_list(self):
        assert parse_requirements(["json", Requirement("os", "x")]) == (
            Requirement("json"),
            Requirement("os", "x"),
        )

    def test_mapping(self):
        assert parse_requirements({"datasets": "pkg[hf]"}) == (
            Requirement("datasets", "pkg[hf]"),
        )

    def test_none(self):
        assert parse_requirements(None) == ()

    def test_invalid(self):
        with pytest.raises(TypeError):
            parse_requirements([1])


class TestCheck:
    def test_available(self):
        assert Requirement("json").missing_reason() is None
        require("json")

    def test_missing(self):
        assert Requirement(MISSING).missing_reason() == "not installed"

    def test_error_message(self):
        with pytest.raises(MissingRequirementError) as excinfo:
            require({MISSING: "some-package[extra]"}, what="my dataset")
        message = str(excinfo.value)
        assert "my dataset" in message
        assert MISSING in message
        assert "some-package[extra]" in message
        assert "pip" not in message
        assert isinstance(excinfo.value, ImportError)

    def test_dotted_missing_parent(self):
        assert Requirement(f"{MISSING}.sub").missing_reason() == "not installed"

    def test_namespace_shadowing(self, tmp_path, monkeypatch):
        """An empty directory named like the module is not the module"""
        name = "datamaestro_test_shadowed"
        (tmp_path / name).mkdir()
        monkeypatch.syspath_prepend(str(tmp_path))
        reason = Requirement(name).missing_reason()
        assert reason is not None and "namespace" in reason

    def test_injected_module(self, monkeypatch):
        """Modules already in sys.modules (e.g. test fakes) are available"""
        monkeypatch.setitem(sys.modules, MISSING, types.ModuleType(MISSING))
        assert Requirement(MISSING).missing_reason() is None


class TestTypes:
    def test_inherited(self):
        @requires("json")
        class A(Base):
            pass

        @requires({"os": "os-package"})
        class B(A):
            pass

        assert type_requirements(B) == (
            Requirement("os", "os-package"),
            Requirement("json"),
        )
        assert type_requirements(A) == (Requirement("json"),)

    def test_require_type(self):
        @requires(MISSING)
        class C(Base):
            pass

        with pytest.raises(MissingRequirementError):
            require(C)


@requires(MISSING)
class NeedsMissing(Base):
    pass


class Plain(Base):
    pass


class TestDataset:
    def test_declared_and_inherited(self, context):
        @dataset(NeedsMissing, id="org.example.req.one", requires="json")
        class MyDataset(Dataset):
            """A dataset"""

            def config(self) -> NeedsMissing:
                return NeedsMissing.C()

        ds = MyDataset.__dataset__
        assert ds.requirements == (Requirement("json"), Requirement(MISSING))
        assert ds.missing_requirements() == [Requirement(MISSING)]

        # Download fails before doing anything
        assert ds.download() is False
        with pytest.raises(MissingRequirementError):
            ds.prepare(download=True)

    def test_dataset_install_overrides_type(self, context):
        @dataset(
            NeedsMissing, id="org.example.req.two", requires={MISSING: "repo[extra]"}
        )
        class Other(Dataset):
            """A dataset"""

            def config(self) -> NeedsMissing:
                return NeedsMissing.C()

        assert Other.__dataset__.requirements == (Requirement(MISSING, "repo[extra]"),)

    def test_no_requirements(self, context):
        @dataset(Plain, id="org.example.req.three")
        class PlainDataset(Dataset):
            """A dataset"""

            def config(self) -> Plain:
                return Plain.C()

        assert PlainDataset.__dataset__.requirements == ()
        assert PlainDataset.__dataset__.download() is True
