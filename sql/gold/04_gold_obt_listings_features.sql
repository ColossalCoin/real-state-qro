/* ==========================================================================
   LAYER: GOLD (Data Mart)
   TABLE: obt_listings_valuation_features
   DESCRIPTION:
     This is the One Big Table (OBT) used for the Valuation Machine Learning Model.

   DATA FLOW STRATEGY:
     1. CRIME: Aggregate raw crime logs into statistics per municipality.
     2. GEO-LOCATION: Spatially map each listing (Point) to an Official Municipality (Polygon).
     3. MERGE: Combine physical house features with the calculated environmental context.
========================================================================== */

CREATE OR REPLACE TABLE `real-estate-qro.obt_listing_features.obt_listings_valuation_features` AS

WITH

/* --------------------------------------------------------------------------
   CTE 1: CRIME CONTEXT AGGREGATION
   Goal: Convert raw crime logs into features (columns) per municipality.
   Logic: We use STRICT EQUALITY based on the official crime catalog.
-------------------------------------------------------------------------- */
crime_features AS (
  SELECT
    -- 1. JOIN KEY NORMALIZATION
    -- We strip accents and force uppercase to ensure a robust join with the map data later.
    -- (e.g., "Querétaro" -> "QUERETARO")
    UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(
        municipio_name), 'á', 'a'), 'é', 'e'), 'í', 'i'), 'ó', 'o'), 'ú', 'u')) AS municipality_key,

    -- 2. TOTAL CRIME VOLUME
    COUNT(*) AS total_crimes_period,

    -- 3. SPECIFIC CRIME FEATURES (Using Strict Equality as requested)
    -- We assume the data source has clean names with accents.
    -- We add TRIM() to handle any accidental leading/trailing whitespace.
    COUNTIF(UPPER(TRIM(crime_type)) = 'ROBO A CASA HABITACIÓN') AS feat_crime_residential,
    COUNTIF(UPPER(TRIM(crime_type)) = 'ROBO DE VEHÍCULO') AS feat_crime_vehicle,
    COUNTIF(UPPER(TRIM(crime_type)) = 'ROBO A TRANSEÚNTE TOTAL') AS feat_crime_street,
    COUNTIF(UPPER(TRIM(crime_type)) = 'HOMICIDIO DOLOSO') AS feat_crime_homicide,
    COUNTIF(UPPER(TRIM(crime_type)) = 'LESIONES DOLOSAS') AS feat_crime_domestic,
    COUNTIF(UPPER(TRIM(crime_type)) = 'NARCOMENUDEO') AS feat_crime_drug_dealing

  FROM `real-estate-qro.queretaro_data_warehouse.fact_context_crime`
  GROUP BY 1
),

/* --------------------------------------------------------------------------
   CTE 2: SPATIAL MAPPING (The "Point-in-Polygon" Step)
   Goal: Identify the official municipality for every listing.
   Logic: Use BigQuery GIS functions to check which polygon contains the house.
-------------------------------------------------------------------------- */
listings_spatially_mapped AS (
  SELECT
    l.*,

    -- Extract the official Municipality Name from the Map Layer (dim_geo_grid_polygons)
    poly.municipality_name AS official_municipality_name,

    -- Generate the Join Key (Normalized)
    -- Must match the logic used in 'crime_features' CTE (Uppercase + No Accents)
    UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        poly.municipality_name, 'á', 'a'), 'é', 'e'), 'í', 'i'), 'ó', 'o'), 'ú', 'u')) AS join_key_municipality

  FROM `real-estate-qro.queretaro_data_warehouse.fact_listings_cleaned` l

  -- SPATIAL JOIN:
  -- Join the Listing (l) with the Map Polygon (poly)
  -- ONLY IF the Polygon geometry contains the Listing point.
  INNER JOIN `real-estate-qro.queretaro_data_warehouse.dim_geo_grid_polygons` poly
    ON ST_CONTAINS(poly.polygon_geom, l.listing_geom)
)

/* --------------------------------------------------------------------------
   FINAL SELECT: ASSEMBLY
   Goal: Bring everything together into the final OBT.
-------------------------------------------------------------------------- */
SELECT
  -- 1. IDENTIFIERS & TARGET
  l.listing_id,
  l.price_mxn AS target_price,

  -- 2. PHYSICAL PROPERTY FEATURES
  l.m2_constructed AS feat_m2_constructed,
  l.m2_terrain AS feat_m2_terrain,
  l.bedrooms AS feat_bedrooms,
  l.bathrooms AS feat_bathrooms,
  l.parking_spots AS feat_parking_spots,
  l.is_new_property AS feat_is_new,

  -- 3. AMENITIES (Boolean Flags)
  l.has_security AS feat_has_security,
  l.has_garden AS feat_has_garden,
  l.has_pool AS feat_has_pool,
  l.has_gym AS feat_has_gym,
  l.has_kitchen AS feat_has_kitchen,
  l.has_terrace AS feat_has_terrace,

  -- 4. GEOGRAPHIC CONTEXT (Derived from Spatial Join)
  l.official_municipality AS feat_municipality,
  l.official_neighborhood_name AS feat_neighborhood,

  -- 5. SAFETY CONTEXT (Derived from Crime Join)
  -- We use COALESCE(x, 0) because if a municipality has NO crime records,
  -- the Join returns NULL. We treat this as 0 crimes (safest).
  COALESCE(c.total_crimes_period, 0) AS feat_crime_total,
  COALESCE(c.feat_crime_residential, 0) AS feat_crime_residential,
  COALESCE(c.feat_crime_vehicle, 0) AS feat_crime_vehicle,
  COALESCE(c.feat_crime_street, 0) AS feat_crime_street,
  COALESCE(c.feat_crime_homicide, 0) AS feat_crime_homicide,
  COALESCE(c.feat_crime_domestic, 0) AS feat_crime_domestic,
  COALESCE(c.feat_crime_drug_dealing, 0) AS feat_crime_drug_dealing

FROM listings_spatially_mapped l

-- LEFT JOIN: We keep the house even if crime data is missing
LEFT JOIN crime_features c
  ON l.join_key_municipality = c.municipality_key;