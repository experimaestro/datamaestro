from typing import Callable
from .definitions import DatasetDefinition
import re


class Condition:
    PATTERNS = [
        (re.compile(r"^tag:(.*)"), lambda m: TagCondition(m.group(1))),
        (re.compile(r"^task:(.*)"), lambda m: TaskCondition(m.group(1))),
        (re.compile(r"^type:(.*)"), lambda m: TypeCondition(m.group(1))),
    ]

    def match(self, dataset: DatasetDefinition):
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

    def match(self, dataset: DatasetDefinition):
        for condition in self.conditions:
            if not condition.match(dataset):
                return False
        return True


class ReCondition(Condition):
    def __init__(self, regex):
        self.regex = re.compile(regex)


class TagCondition(ReCondition):
    def match(self, dataset: DatasetDefinition):
        for tag in dataset.tags:
            if self.regex.search(tag):
                return True
        return False


class TaskCondition(ReCondition):
    def match(self, dataset: DatasetDefinition):
        for task in dataset.tasks:
            if self.regex.search(task):
                return True
        return False


class IDCondition(ReCondition):
    def match(self, dataset: DatasetDefinition):
        return self.regex.search(dataset.id)


class TypeCondition(Condition):
    def __init__(self, typename: str):
        self.typename = typename

    def match(self, dataset: DatasetDefinition):
        for ds in dataset.ancestors():
            if str(ds.__xpmtype__.identifier) == self.typename:
                return True

        return False
