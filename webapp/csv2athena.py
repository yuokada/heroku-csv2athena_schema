# -*- coding: utf-8 -*-
"""
csv2schema - A python script to build athena create table from csv file.

Require
-  csvfile with header
"""
import argparse
import logging
import os
import textwrap

import jinja2
from messytables import CSVTableSet, offset_processor, headers_guess, type_guess, types_processor, headers_processor
from messytables.types import IntegerType, CellType, BoolType, StringType, DateType, DateUtilType, FloatType, \
    DecimalType

logging.basicConfig(format='%(levelname)s:%(lineno)d:%(funcName)s: %(message)s', level=logging.WARN)
logger = logging.getLogger(__name__)


class StoreDictKeyPair(argparse.Action):
    """This class work for Argument parser."""

    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        """NOTE: TBD."""
        self._nargs = nargs
        super(StoreDictKeyPair, self).__init__(option_strings, dest, nargs=nargs, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        """Parse document."""
        properties = {}
        "values: {}".format(values)
        for kv in values:
            k, v = kv.split("=")
            properties[k] = v
        setattr(namespace, self.dest, properties)


def guess_csv_datatype(filename):
    with open(filename, 'rb') as fh:
        d = _guess_csv_datatype(fh)
    return d


def _guess_csv_datatype(fh):
    table_set = CSVTableSet(fh)
    row_set = table_set.tables[0]
    offset, headers = headers_guess(row_set.sample)
    logger.info("(offset, headers) = ({}, {})".format(offset, headers))

    row_set.register_processor(headers_processor(headers))
    row_set.register_processor(offset_processor(offset + 1))
    types = type_guess(row_set.sample, strict=True)
    row_set.register_processor(types_processor(types))

    counter = 0
    for row in row_set:
        logger.info(row)
        counter += 1
        if counter >= 32:
            break

    d = {h: t for h, t in zip(headers, types)}
    logger.info(d)
    return d


def convert_presto_data_type(datatype: CellType) -> str:
    if isinstance(datatype, BoolType):
        return 'BOOLEAN'
    elif isinstance(datatype, IntegerType):
        return 'INT'
    elif isinstance(datatype, FloatType):
        return 'DOUBLE'
    elif isinstance(datatype, DecimalType):
        return 'DECIMAL'
    elif isinstance(datatype, StringType):
        return 'STRING'
        # return 'VARCHAR'
    elif isinstance(datatype, DateType) or isinstance(datatype, DateUtilType):
        return 'TIMESTAMP'
    else:
        # NOTE: or raise Exception
        return 'UNDEFINED'


def convert_fields(guess_data, serde):
    fields = []
    for k, v in guess_data.items():
        # NOTE: https://docs.aws.amazon.com/ja_jp/athena/latest/ug/serde-reference.html
        if serde == 'org.apache.hadoop.hive.serde2.OpenCSVSerde':
            # NOTE: OpenCSVSerde treat with STRING only.
            x = 'STRING'
        else:
            x = convert_presto_data_type(v)
        fields.append('`{}` {}'.format(k, x))
    return fields


def get_filename(filename):
    return os.path.splitext(os.path.basename(filename))[0]


def build_ct(guess_data, arguments) -> str:
    t = textwrap.dedent("""
    CREATE EXTERNAL TABLE {{ schema }} (
      {{table_fields|join(",\n      ")}}
    )
    ROW FORMAT SERDE '{{ serde }}'
    WITH SERDEPROPERTIES (
    {%- if serde_properties -%}
    {% for k, v in serde_properties.items() %}
        {{ "'%s' = '%s'" |format(k, v) }} {%- if not loop.last -%} , {%- endif -%}
    {% endfor %}
    {% else %}
      -- default
      'separatorChar' = ',',
      'quoteChar' = '\\"',
      'escapeChar' = '\\\\'
    {% endif -%}
    )
    STORED AS {{ stored_as }}
    LOCATION '{{ location }}'
    TBLPROPERTIES (
    {%- if table_properties -%}
    {% for k, v in table_properties.items() %}
        {{ "'%s'='%s'" |format(k, v) }} {%- if not loop.last -%} , {%- endif -%}
    {% endfor %}
    {% else %}
    -- default
      'skip.header.line.count'='1',
      'has_encrypted_data'='false'
    {% endif -%}
    );
    """)
    # 1. csv -> Athena field type
    fields = convert_fields(guess_data, arguments.serde)

    # 2. tablename
    if arguments.schema:
        tablename = arguments.schema + "." + get_filename(arguments.csvfile)
    else:
        tablename = get_filename(arguments.csvfile)

    template = jinja2.Template(t)
    ct = template.render(
        schema=tablename,
        table_fields=fields,
        serde=arguments.serde,
        serde_properties=arguments.serde_properties,
        location=arguments.location,
        stored_as=arguments.stored_as,
        table_properties=arguments.table_properties,
    )
    return ct


def main():
    parser = argparse.ArgumentParser('csv2schema')
    parser.add_argument('--schema', type=str, required=False)
    parser.add_argument('--serde-properties', dest="serde_properties", action=StoreDictKeyPair, nargs='+', metavar="KEY=VAL")
    parser.add_argument('--location', type=str, default='s3://your_bucket/path/to/files', help='path to s3 url')
    parser.add_argument('--table-properties', dest="table_properties", action=StoreDictKeyPair, nargs='+', metavar="KEY=VAL")

    parser.add_argument('csvfile', type=str, metavar='FILE')

    arguments = parser.parse_args()
    guess_result = guess_csv_datatype(arguments.csvfile)

    ct = build_ct(guess_result, arguments)
    print(ct)
