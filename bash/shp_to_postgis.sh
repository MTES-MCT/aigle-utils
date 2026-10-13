#!/bin/bash

if [ "$1" == "" ]
then
    echo "You must specify source folder path as argument"
    exit 1
fi

if [ "$2" == "" ]
then
    SCHEMA=custom_zones
else
    SCHEMA=$2
fi

if [ "$3" == "" ]
then
    RESULT_TABLE=custom_zones
else
    RESULT_TABLE=$3
fi

echo "Inserting following shp to '$SCHEMA.$RESULT_TABLE' table"
echo "$1"

INTERMEDIATE_GEOMETRIES_TABLE=intermediate_geometries
PGPASSWORD=$SQL_PASSWORD

# Temporary tables for processing
TEMP_DB_PREFIX="temp_shp_"
COUNTER=0

# Most French data is in either 2154 (Lambert 93)
SOURCE_SRID=2154  # This is a guess - adjust if you know the actual source SRID
SOURCE_SRID=4559  # This is a guess - adjust if you know the actual source SRID

# Create the intermediate table
psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -c "
    DROP TABLE IF EXISTS $SCHEMA.$INTERMEDIATE_GEOMETRIES_TABLE;
    CREATE TABLE $SCHEMA.$INTERMEDIATE_GEOMETRIES_TABLE (id serial primary key, geometry geometry(MultiPolygon, 4326));
"

# Process each shapefile and geopackage
find $1 -name "*.shp" -o -name "*.gpkg" | while read geo_file; do
    # Create a unique temporary table name
    TEMP_TABLE="${TEMP_DB_PREFIX}${COUNTER}"
    COUNTER=$((COUNTER+1))
    
    echo "Processing: $geo_file into $TEMP_TABLE"
    
    # Check file extension and use appropriate tool
    if [[ "$geo_file" == *.shp ]]; then
        # Import shapefile to a temporary table WITHOUT specifying SRID conversion
        shp2pgsql -W "UTF-8" -c -s 0 $geo_file $SCHEMA.$TEMP_TABLE | psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE
    elif [[ "$geo_file" == *.gpkg ]]; then
        # Import geopackage using ogr2ogr
        ogr2ogr -f "PostgreSQL" "PG:host='$SQL_HOST' port='$SQL_PORT' user='$SQL_USER' dbname='$SQL_DATABASE' password='$SQL_PASSWORD'" \
            -nln $SCHEMA.$TEMP_TABLE -overwrite $geo_file
    fi
    
    # Detect the SRID of the imported geometry (column name might vary)
    GEOM_COLUMN=$(psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -t -c "SELECT column_name FROM information_schema.columns WHERE table_name='$TEMP_TABLE' AND table_schema='$SCHEMA' AND udt_name='geometry';" | xargs)
    
    if [ -z "$GEOM_COLUMN" ]; then
        echo "No geometry column found in $TEMP_TABLE"
        continue
    fi
    
    DETECTED_SRID=$(psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -t -c "SELECT ST_SRID($GEOM_COLUMN) FROM $SCHEMA.$TEMP_TABLE LIMIT 1;" | xargs)
    
    echo "Detected SRID for $geo_file: $DETECTED_SRID (geometry column: $GEOM_COLUMN)"
    
    # If the detected SRID is 0 or NULL, use our guessed SOURCE_SRID
    if [ "$DETECTED_SRID" = "0" ] || [ -z "$DETECTED_SRID" ]; then
        echo "Setting SRID to $SOURCE_SRID for $geo_file"
        psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -c "
            UPDATE $SCHEMA.$TEMP_TABLE SET $GEOM_COLUMN = ST_SetSRID($GEOM_COLUMN, $SOURCE_SRID);
        "
        DETECTED_SRID=$SOURCE_SRID
    fi
    
    # Verify SRID after setting
    FINAL_SRID=$(psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -t -c "SELECT ST_SRID($GEOM_COLUMN) FROM $SCHEMA.$TEMP_TABLE LIMIT 1;" | xargs)
    echo "Final SRID for $geo_file: $FINAL_SRID"
    
    # Check geometry validity before processing
    TOTAL_ROWS=$(psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -t -c "SELECT COUNT(*) FROM $SCHEMA.$TEMP_TABLE;" | xargs)
    VALID_ROWS=$(psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -t -c "SELECT COUNT(*) FROM $SCHEMA.$TEMP_TABLE WHERE ST_IsValid($GEOM_COLUMN) AND $GEOM_COLUMN IS NOT NULL;" | xargs)
    
    echo "Total rows in $geo_file: $TOTAL_ROWS"
    echo "Valid geometries in $geo_file: $VALID_ROWS"
    
    # Extract just the geometry and add it to our intermediate table, properly transforming to 4326
    psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -c "
        INSERT INTO $SCHEMA.$INTERMEDIATE_GEOMETRIES_TABLE (geometry)
        SELECT ST_Multi(ST_Transform(ST_MakeValid($GEOM_COLUMN), 4326)) 
        FROM $SCHEMA.$TEMP_TABLE 
        WHERE ST_IsValid($GEOM_COLUMN) AND $GEOM_COLUMN IS NOT NULL;
        DROP TABLE $SCHEMA.$TEMP_TABLE;
    "
done

CUSTOM_ZONE_NAME=$(basename "$1")

# Check intermediate table before final union
INTERMEDIATE_COUNT=$(psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -t -c "SELECT COUNT(*) FROM $SCHEMA.$INTERMEDIATE_GEOMETRIES_TABLE WHERE geometry IS NOT NULL;" | xargs)
echo "Intermediate table has $INTERMEDIATE_COUNT valid geometries"

if [ "$INTERMEDIATE_COUNT" -eq "0" ]; then
    echo "ERROR: No valid geometries found in intermediate table"
    exit 1
fi

# Create the final union table with the combined geometry
psql -h $SQL_HOST -p $SQL_PORT -U $SQL_USER -d $SQL_DATABASE -c "
    -- Create final union table with properly typed geometry column
    CREATE TABLE IF NOT EXISTS $SCHEMA.$RESULT_TABLE (
        id serial primary key,
        geometry geometry(MultiPolygon, 4326),
        name varchar
    );

    -- Remove existing geometry with same name if exists
    DELETE FROM $SCHEMA.$RESULT_TABLE WHERE \"name\" = '$CUSTOM_ZONE_NAME';

    -- Insert the unioned geometry with better error handling
    INSERT INTO $SCHEMA.$RESULT_TABLE (name, geometry)
    SELECT 
        '$CUSTOM_ZONE_NAME' AS name, 
        CASE 
            WHEN ST_Union(ST_Buffer(geometry, 0)) IS NULL THEN NULL
            ELSE ST_Multi(ST_Buffer(ST_Union(ST_Buffer(geometry, 0)), 0))
        END AS geometry
    FROM $SCHEMA.$INTERMEDIATE_GEOMETRIES_TABLE
    WHERE geometry IS NOT NULL;

    -- Clean temporary table
    DROP TABLE $SCHEMA.$INTERMEDIATE_GEOMETRIES_TABLE;
"

echo "All geometries have been combined into the '$SCHEMA.$RESULT_TABLE' table."
