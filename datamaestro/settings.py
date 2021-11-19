"""Global and user settings utility classes"""
import marshmallow as mm
from typing import Dict, Any
from experimaestro.utils.settings import JsonSettings, PathField
from pathlib import Path

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
