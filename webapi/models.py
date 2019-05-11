import base64
import binascii
from dataclasses import dataclass, field
from typing import Dict

SERDE_CHOICES = [
    'org.apache.hadoop.hive.serde2.OpenCSVSerde',
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
    'org.apache.hadoop.hive.serde2.RegexSerDe',
]

STORED_CHOICES = [
    "TEXTFILE",
    "SEQUENCEFILE",
    "ORC",
    "RCFILE",
    "PARQUET",
    "AVRO",
]


class SimpleObject(object):
    def __init__(self, name, age):
        self.name = name
        self.age = age


@dataclass
class DCSimpleObject(object):
    name: str
    age: int


@dataclass
class AssetObject(object):
    filename: str
    attachement: str


@dataclass
class AnalyzerObject(object):
    schema: str = "default_schema"
    table: str = "default_table"
    data_location: str = "s3://path/to/files/"
    serde: str = SERDE_CHOICES[0]
    serde_properties: Dict[str, str] = field(default_factory=dict)
    stored: str = STORED_CHOICES[0]
    table_properties: Dict[str, str] = field(default_factory=dict)
    csv_file: str = None
    encoding: str = field(default='base64', repr=False)
    create_table: str = None

    def __post_init__(self) -> None:
        try:
            if self.csv_file is not None:
                raw_contents = base64.b64decode(self.csv_file).decode()
                self.csv_file = raw_contents
        except binascii.Error as e:
            pass


@dataclass(frozen=True)
class AnalyzerResultObject(object):
    ddl: str


@dataclass(frozen=True)
class PointObject(object):
    point: int
    plusing: int = 0
    method: str = 'GET'
