from json import JSONEncoder
from pathlib import PosixPath

class ExperimaestroEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, PosixPath):
            return { "$type": "path", "$value": str(o.resolve()) }
        return o.__dict__    
