from .definitions import AbstractDataset
import re


class Condition:
    PATTERNS = [
        (re.compile(r"^tag:(.*)"), lambda m: TagCondition(m.group(1))),
        (re.compile(r"^task:(.*)"), lambda m: TaskCondition(m.group(1))),
        (re.compile(r"^type:(.*)"), lambda m: TypeCondition(m.group(1))),
        (
            re.compile(r"^repo(?:sitory)?:(.*)"),
            lambda m: RepositoryCondition(m.group(1)),
        ),
    ]

    def match(self, dataset: AbstractDataset):
        raise Exception("Match not implemented in %s" % type(self))

    @staticmethod
    def parse(searchterm):
        for regex, callback in Condition.PATTERNS:
            m = regex.match(searchterm)
            if m:
                return callback(m)

        return IDCondition(searchterm)


class AndCondition(Condition):
    def __init__(self):
        self.conditions = []

    def append(self, condition: Condition):
        self.conditions.append(condition)

    def match(self, dataset: AbstractDataset):
        for condition in self.conditions:
            if not condition.match(dataset):
                return False
        return True

    def __repr__(self):
        return " AND ".join(self.conditions)


class OrCondition(Condition):
    def __init__(self):
        self.conditions = []

    def append(self, condition: Condition):
        self.conditions.append(condition)

    def match(self, dataset: AbstractDataset):
        for condition in self.conditions:
            if condition.match(dataset):
                return True
        return False

    def __repr__(self):
        return " OR ".join(repr(s) for s in self.conditions)


class ReCondition(Condition):
    def __init__(self, regex):
        self.regex = re.compile(regex)


class TagCondition(ReCondition):
    def match(self, dataset: AbstractDataset):
        for tag in dataset.tags:
            if self.regex.search(tag):
                return True
        return False


class TaskCondition(ReCondition):
    def match(self, dataset: AbstractDataset):
        for task in dataset.tasks:
            if self.regex.search(task):
                return True
        return False


class RepositoryCondition(ReCondition):
    def match(self, dataset: AbstractDataset):
        return self.regex.search(dataset.repository.id) is not None

    def __repr__(self):
        return f"repository ~ {self.regex}"


class IDCondition(ReCondition):
    def match(self, dataset: AbstractDataset):
        return self.regex.search(dataset.id)

    def __repr__(self):
        return f"dataset ~ {self.regex}"


class TypeCondition(Condition):
    def __init__(self, typename: str):
        self.typename = typename

    def match(self, dataset: AbstractDataset):
        for ds in dataset.ancestors():
            if str(ds.__xpmtype__.identifier) == self.typename:
                return True

        return False
