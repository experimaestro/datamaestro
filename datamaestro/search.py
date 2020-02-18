from .definitions import DatasetDefinition
import re

RE_TAG = re.compile(r"^tag:(.*)")
RE_TASK = re.compile(r"^task:(.*)")


class Condition:
    def match(self, dataset: DatasetDefinition):
        raise Exception("Match not implemented in %s" % type(self))

    @staticmethod
    def parse(searchterm):
        m = RE_TAG.match(searchterm)
        if m:
            return TagCondition(m.group(1))

        m = RE_TASK.match(searchterm)
        if m:
            return TaskCondition(m.group(1))

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
