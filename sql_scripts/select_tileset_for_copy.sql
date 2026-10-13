-- query to select and import from preprod to prod
SELECT
    "uuid",
    created_at,
    updated_at,
    deleted,
    monochrome,
    "name",
    url,
    tile_set_status,
    "date",
    tile_set_scheme,
    tile_set_type,
    max_zoom,
    min_zoom
FROM
    public.core_tileset
WHERE
    url ILIKE '%bouche%'
