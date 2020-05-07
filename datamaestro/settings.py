"""Global and user settings utility classes"""
import marshmallow as mm
from pathlib import Path

class JsonSettings():   
    @classmethod
    def load(cls, path: Path):
        if path.is_file():
            settings = cls.SCHEMA().loads(path.read_text())
        else:
            settings = cls()
        settings.path = path
        return settings

    def save(self):
        """Save the preferences"""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(self.SCHEMA().dumps(self))


class PathField(mm.fields.Field):
    """Field that serializes to a title case string and deserializes
    to a lower case string.
    """

    def _serialize(self, value, attr, obj, **kwargs):
        return Path(value)

    def _deserialize(self, value, attr, data, **kwargs):
        return str(value.absolute())


# --- Global settings

class SettingsSchema(mm.Schema):
    keys = mm.fields.Dict(keys=mm.fields.Str(), values=mm.fields.Str())
    datafolders = mm.fields.Dict(keys=mm.fields.Str(), values=mm.fields.Str())

    @mm.post_load
    def make_settings(self, data, **kwargs):
        settings = Settings()
        for key, value in data.items():
            setattr(settings, key, value)
        return settings

class Settings(JsonSettings):
    SCHEMA = SettingsSchema

    """Global settings"""
    def __init__(self):
        self.keys: Dict[str, Any] = {}
        self.datafolders: Dict[str, Path] = {}


# --- User settings

class UserSettingsSchema(mm.Schema):
    agreements = mm.fields.Dict(keys=mm.fields.Str(), values=mm.fields.Str())

    @mm.post_load
    def make_settings(self, data, **kwargs):
        settings = UserSettings()
        for key, value in data.items():
            setattr(settings, key, value)
        return settings

class UserSettings(JsonSettings):
    """User settings"""

    SCHEMA = UserSettingsSchema

    def __init__(self):
        self.agreements: Dict[str, str] = {}
