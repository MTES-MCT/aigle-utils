#!/usr/bin/env python3
"""
Extract rows from Sitadel CSV file based on COMM or DEP_CODE value.
"""

import csv


def extract_by_comm(input_file, output_file, comm_value):
    """
    Extract rows from CSV where COMM matches the specified value.

    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        comm_value: COMM value to filter by
    """
    rows_written = 0

    with open(input_file, "r", encoding="utf-8") as infile, open(
        output_file, "w", encoding="utf-8", newline=""
    ) as outfile:
        reader = csv.DictReader(infile, delimiter=";", quotechar='"')

        # Write header
        writer = csv.DictWriter(
            outfile,
            fieldnames=reader.fieldnames,
            delimiter=";",
            quotechar='"',
            quoting=csv.QUOTE_NONNUMERIC,
        )
        writer.writeheader()

        # Filter and write rows
        for row in reader:
            if row["COMM"] == comm_value:
                writer.writerow(row)
                rows_written += 1

    return rows_written


def extract_by_dpt(input_file, output_file, dpt_code):
    """
    Extract rows from CSV where DEP_CODE matches the specified value.

    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        dpt_code: DEP_CODE value to filter by
    """
    rows_written = 0

    with open(input_file, "r", encoding="utf-8") as infile, open(
        output_file, "w", encoding="utf-8", newline=""
    ) as outfile:
        reader = csv.DictReader(infile, delimiter=";", quotechar='"')

        writer = csv.DictWriter(
            outfile,
            fieldnames=reader.fieldnames,
            delimiter=";",
            quotechar='"',
            quoting=csv.QUOTE_NONNUMERIC,
        )
        writer.writeheader()

        for row in reader:
            if row["DEP_CODE"] == dpt_code:
                writer.writerow(row)
                rows_written += 1

    return rows_written


if __name__ == "__main__":
    dpt_code = "31"

    input_file = "/Volumes/data/Downloads/Liste-des-autorisations-durbanisme-creant-des-logements.2026-05.csv"
    output_file = f"/Volumes/data/Downloads/Liste-des-autorisations-durbanisme-creant-des-logements.2026-05_{dpt_code}.csv"

    print(f"Filtering CSV for DPT='{dpt_code}'...")
    rows = extract_by_dpt(input_file, output_file, dpt_code)
    print(f"Done! Extracted {rows} rows to {output_file}")

    input_file = "/Volumes/data/Downloads/Liste-des-autorisations-durbanisme-creant-des-locaux-non-residentiels.2026-05.csv"
    output_file = f"/Volumes/data/Downloads/Liste-des-autorisations-durbanisme-creant-des-locaux-non-residentiels.2026-05_{dpt_code}.csv"

    print(f"Filtering CSV for DPT='{dpt_code}'...")
    rows = extract_by_dpt(input_file, output_file, dpt_code)
    print(f"Done! Extracted {rows} rows to {output_file}")
