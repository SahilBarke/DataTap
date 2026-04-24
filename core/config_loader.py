"""
Config loader module
Reads and validates YAML config files using Pydantic.
Each YAML file describes one API data source.
"""

from __future__ import annotations
from typing import Optional, Literal, Any
from pydantic import BaseModel, Field, field_validator
import yaml
from pathlib import Path
