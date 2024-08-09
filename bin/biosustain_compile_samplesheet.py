#!/usr/bin/env python

import argparse
import logging
import os
import sys
from pathlib import Path
from string import Template

import pyarrow.compute
import pyarrow.csv
import pyarrow.parquet

from check_samplesheet import check_samplesheet


def parse_args(args=None):
    Description = "Compile nf-core/nanoseq samplesheet file."
    Epilog = "Example usage: python biosustain_check_samplesheet.py.py <FILE_IN> <UPDATED_PATCH> <INPUT_FILE_TEMPLATE> <FILE_OUT>"

    parser = argparse.ArgumentParser(description=Description, epilog=Epilog)
    parser.add_argument("FILE_IN", help="Input benchling samplesheet file.")
    parser.add_argument("UPDATED_PATH", help="Input path for test_nobc_nodx_rnamod")
    parser.add_argument("INPUT_FILE_TEMPLATE", help="Input file template")
    parser.add_argument("FILE_OUT", help="Output samplesheet file.")
    return parser.parse_args(args)


def _rows(table):
    columns_dict = table.to_pydict()
    for i in range(table.num_rows):
        yield {col: columns_dict[col][i] for col in columns_dict}


def compile_samplesheet(file_in, updated_path, input_file_template, file_out):
    print("processing", file_in, updated_path, input_file_template, file_out)
    if updated_path == "not_changed":
        updated_path = None
    else:
        updated_path = Path(updated_path)
    input_file_template = Template(input_file_template)

    def resolve_input_file(**kwargs):
        input_file = input_file_template.substitute(**kwargs)
        input_file = Path(input_file)
        if updated_path:
            return updated_path / input_file.name
        return input_file

    sample_sheet = pyarrow.parquet.read_table(
        file_in,
        columns=["group", "replicate", "sample_barcode", ]
    )
    # pipeline group cannot contain spaces
    sample_sheet = sample_sheet.set_column(
        0,
        'group',
        pyarrow.compute.replace_substring(sample_sheet["group"], ' ', '_')
    )
    # barcode must be init - deleteting prefix (will be readded in next step...)
    sample_sheet = sample_sheet.set_column(
        2,
        'barcode',
        pyarrow.compute.replace_substring(sample_sheet["sample_barcode"], 'barecode',
                                          '')
    )
    # temporary fix replicate should be uniq in group
    sample_sheet = sample_sheet.set_column(
        1,
        'replicate',
        [[1, 1, 2, 2, 3, 3]]
    )
    sample_sheet = sample_sheet.append_column(
        'input_file',
        [
            [
                str(resolve_input_file(**row)) for row in _rows(sample_sheet)
            ]
        ]
    )

    def is_file_filter(path):
        is_file = os.path.isfile(str(path))
        if not is_file:
            logging.warning(f"File {path} is not found, skipping")
        return is_file

    sample_sheet = sample_sheet.filter(
        [is_file_filter(str(input_file)) for input_file in sample_sheet["input_file"]]
    )
    sample_sheet = sample_sheet.append_column('fasta', [[''] * len(sample_sheet)])
    sample_sheet = sample_sheet.append_column('gtf', [[''] * len(sample_sheet)])

    pyarrow.csv.write_csv(
        sample_sheet,
        file_out
    )

    # Pyarrow is not able to write the csv file without quotes...
    with open(file_out, "r") as f:
        sample_sheet_str = f.read().replace('"', '')
    # and nanoseq cannot read the csv file with quotes...
    with open(file_out, "w") as f:
        f.write(sample_sheet_str)


def main(args=None):
    args = parse_args(args)
    compile_samplesheet(args.FILE_IN, args.UPDATED_PATH, args.INPUT_FILE_TEMPLATE,
                        args.FILE_OUT)
    check_samplesheet(args.FILE_OUT, args.UPDATED_PATH, args.FILE_OUT + ".checked")


if __name__ == "__main__":
    sys.exit(main())
