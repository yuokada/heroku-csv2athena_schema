# -*- coding: utf-8 -*-
"""
csv2schema - A python script to build athena create table from csv file.

Require
-  csvfile with header
"""
import argparse
import logging
import os

from django.template import loader
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
    """
    # NOTE: OpenCSVSerde treat with STRING only.
    # NOTE: https://docs.aws.amazon.com/ja_jp/athena/latest/ug/serde-reference.html

    :param guess_data:
    :param serde:
    :return:
    """
    fields = []
    for k, v in guess_data.items():
        if serde == 'org.apache.hadoop.hive.serde2.OpenCSVSerde':
            x = 'STRING'
        else:
            x = convert_presto_data_type(v)
        fields.append('`{}` {}'.format(k, x))
    return fields


def get_filename(filename):
    return os.path.splitext(os.path.basename(filename))[0]


def build_ct(guess_data, post_form) -> str:
    # 1. csv -> Athena field type
    fields = convert_fields(guess_data, post_form['serde'].data)

    # 2. tablename
    if post_form['schema'].data is not None:
        tablename = post_form['schema'].data + "." + get_filename(post_form['csvfile'].data.name)
    else:
        tablename = get_filename(post_form['csvfile'].data.name)

    arguments = dict(
        schema=tablename,
        table_fields=fields,
        serde=post_form['serde'].data,
        # serde_properties=post_form['serde_properties'].data,
        serde_properties=dict(),
        location=post_form['location'].data,
        stored_as=post_form['stored_as'].data,
        # table_properties=post_form['table_properties'].data,
        table_properties=dict(),
    )
    return loader.render_to_string('core/ct.sql', arguments)
