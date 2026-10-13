#!/usr/bin/env python3
"""
Convert CSV file with WKT geometry to GeoPackage format.

This script reads a CSV file containing polygon geometries in WKT format
and converts it to a GeoPackage (.gpkg) file.
"""

import pandas as pd
import geopandas as gpd
from shapely import wkt
import sys


def csv_to_geopackage(input_csv, output_gpkg, crs="EPSG:4326"):
    """
    Convert a CSV file with WKT geometry to GeoPackage format.

    Parameters:
    -----------
    input_csv : str
        Path to the input CSV file
    output_gpkg : str
        Path to the output GeoPackage file
    crs : str, optional
        Coordinate Reference System (default: 'EPSG:4326' for WGS84)
    """

    print(f"Reading CSV file: {input_csv}")

    # Read the CSV file
    df = pd.read_csv(input_csv)

    print(f"Found {len(df)} records")
    print(f"Columns: {list(df.columns)}")

    # Convert the 'geometry' column from WKT string to geometry objects
    print("Converting WKT geometries...")
    df["geometry"] = df["geometry"].apply(wkt.loads)

    # Create a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=crs)

    print(f"Writing to GeoPackage: {output_gpkg}")

    # Save to GeoPackage
    gdf.to_file(output_gpkg, driver="GPKG", layer="objects")

    print(f"Successfully converted {len(gdf)} features to GeoPackage!")
    print(f"Output file: {output_gpkg}")

    # Print summary statistics
    print("\nSummary:")
    print(f"  - Total features: {len(gdf)}")
    print(f"  - Geometry type: {gdf.geometry.geom_type.unique()}")
    print(f"  - CRS: {gdf.crs}")
    print(f"  - Bounds: {gdf.total_bounds}")


if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python csv_to_geopackage.py <input.csv> [output.gpkg] [CRS]")
        print("\nExample:")
        print("  python csv_to_geopackage.py data.csv output.gpkg EPSG:4326")
        print(
            "\nIf output file is not specified, it will use the input filename with .gpkg extension"
        )
        print("If CRS is not specified, it will use EPSG:4326 (WGS84)")
        sys.exit(1)

    # Get input file
    input_csv = sys.argv[1]

    # Get output file (default: same name as input with .gpkg extension)
    if len(sys.argv) >= 3:
        output_gpkg = sys.argv[2]
    else:
        output_gpkg = input_csv.rsplit(".", 1)[0] + ".gpkg"

    # Get CRS (default: EPSG:4326)
    if len(sys.argv) >= 4:
        crs = sys.argv[3]
    else:
        crs = "EPSG:4326"

    # Convert the file
    try:
        csv_to_geopackage(input_csv, output_gpkg, crs)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
