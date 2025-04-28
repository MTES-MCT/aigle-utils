#!/bin/bash

if [ "$1" == "" ]
then
    echo "You must specify source folder path as argument"
    exit 1
fi

SCHEMA=custom_zones
INTERMEDIATE_GEOMETRIES_TABLE=intermediate_geometries
RESULT_TABLE=custom_zones
PGPASSWORD=$SQL_PASSWORD

# Temporary tables for processing
TEMP_DB_PREFIX="temp_shp_"
COUNTER=0

# Most French data is in either 2154 (Lambert 93)
SOURCE_SRID=2154  # This is a guess - adjust if you know the actual source SRID

# Create the intermediate table
psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -c "
    DROP TABLE IF EXISTS $SCHEMA.$INTERMEDIATE_GEOMETRIES_TABLE;
    CREATE TABLE $SCHEMA.$INTERMEDIATE_GEOMETRIES_TABLE (id serial primary key, geometry geometry(Geometry, 4326));
"

# Process each shapefile
find $1 -name "*.shp" | while read shp_file; do
    # Create a unique temporary table name
    TEMP_TABLE="${TEMP_DB_PREFIX}${COUNTER}"
    COUNTER=$((COUNTER+1))
    
    echo "Processing: $shp_file into $TEMP_TABLE"
    
    # Import shapefile to a temporary table WITHOUT specifying SRID conversion
    # Let's see what SRID is in the data
    shp2pgsql -c $shp_file $SCHEMA.$TEMP_TABLE | psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE
    
    # Detect the SRID of the imported shapefile
    DETECTED_SRID=$(psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -t -c "SELECT ST_SRID(geom) FROM $SCHEMA.$TEMP_TABLE LIMIT 1;" | xargs)
    
    echo "Detected SRID for $shp_file: $DETECTED_SRID"
    
    # If the detected SRID is 0 or NULL, use our guessed SOURCE_SRID
    if [ "$DETECTED_SRID" = "0" ] || [ -z "$DETECTED_SRID" ]; then
        echo "Setting SRID to $SOURCE_SRID for $shp_file"
        psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -c "
            UPDATE $SCHEMA.$TEMP_TABLE SET geom = ST_SetSRID(geom, $SOURCE_SRID);
        "
        DETECTED_SRID=$SOURCE_SRID
    fi
    
    # Extract just the geometry and add it to our intermediate table, properly transforming to 4326
    psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -c "
        INSERT INTO $SCHEMA.$INTERMEDIATE_GEOMETRIES_TABLE (geometry)
        SELECT ST_Transform(ST_MakeValid(geom), 4326) FROM $SCHEMA.$TEMP_TABLE;
        DROP TABLE $SCHEMA.$TEMP_TABLE;
    "
done

CUSTOM_ZONE_NAME=$(basename "$1")

# Create the final union table with the combined geometry
psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -c "
    -- Create final union table with properly typed geometry column
    CREATE TABLE IF NOT EXISTS $SCHEMA.$RESULT_TABLE (
        id serial primary key,
        geometry geometry(Geometry, 4326),
        name varchar
    );

    -- Remove existing geometry with same name if exists
    DELETE FROM $SCHEMA.$RESULT_TABLE WHERE \"name\" = '$CUSTOM_ZONE_NAME';

    -- Insert the unioned geometry
    INSERT INTO $SCHEMA.$RESULT_TABLE (name, geometry)
    SELECT 
        '$CUSTOM_ZONE_NAME' AS name, 
        ST_Buffer(ST_Union(ST_Buffer(geometry, 0)), 0) AS geometry
    FROM $SCHEMA.$INTERMEDIATE_GEOMETRIES_TABLE;

    -- Clean temporary table
    DROP TABLE $SCHEMA.$INTERMEDIATE_GEOMETRIES_TABLE;
"

echo "All geometries have been combined into the '$SCHEMA.$RESULT_TABLE' table."
