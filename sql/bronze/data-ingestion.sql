/* ==========================================================================
   DATA INGESTION
   --------------------------------------------------------------------------
   Source: GCS Bucket
   Target: BigQuery Staging Table
   Strategy: ELT (Extract-Load-Transform) with Schema Auto-detection.
   ========================================================================== 

Schema Strategy:
We use auto-detect for the Staging layer to ensure raw data ingestion.
Strict typing and constraints will be applied in the Silver layer.
CSV Parsing Configuration (Optimized for Pandas/Excel outputs) */

LOAD DATA OVERWRITE queretaro_staging.listings_enriched
FROM FILES (
  format = 'CSV',
  uris = ['gs://real-estate-qro-datalake/processed/real_estate_enriched.csv'],

  skip_leading_rows = 1,        -- Skips the header row to avoid type mismatches
  quote = '"',                  -- Handles commas inside text fields (e.g., "Kitchen, Garden")
  allow_jagged_rows = true,     -- Handles rows with missing trailing columns
  ignore_unknown_values = true  -- Prevents crashes if extra columns appear
);


LOAD DATA OVERWRITE queretaro_staging.geo_neighborhoods
FROM FILES (
  format = 'CSV',
  uris = ['gs://real-estate-qro-datalake/processed/geo_catalog.csv'],

  skip_leading_rows = 1,        
  quote = '"',                  
  allow_jagged_rows = true,     
  ignore_unknown_values = true
);


LOAD DATA OVERWRITE queretaro_staging.context_amenities
FROM FILES (
  format = 'CSV',
  uris = ['gs://real-estate-qro-datalake/external/amenities.csv'],

  skip_leading_rows = 1,        
  quote = '"',                  
  allow_jagged_rows = true,     
  ignore_unknown_values = true
);


LOAD DATA OVERWRITE queretaro_staging.context_crime
FROM FILES (
  format = 'CSV',
  uris = ['gs://real-estate-qro-datalake/external/onc-datos-abiertos.csv'],

  skip_leading_rows = 1,        
  quote = '"',                  
  allow_jagged_rows = true,     
  ignore_unknown_values = true
);


LOAD DATA OVERWRITE queretaro_staging.geo_grid_mosaics
(
  geo_raw STRING  -- We manually define a single column to hold the raw JSON
)
FROM FILES (
  format = 'CSV', -- Trick: Treat as CSV to bypass JSON parser limitations
  uris = ['gs://real-estate-qro-datalake/processed/municipios_bq.jsonl'],
  
  -- Configuration to ingest the whole line as one string
  field_delimiter = '\x01', -- Use a null character (impossible char) as separator
  quote = '',               -- Disable quoting to avoid parsing issues
  max_bad_records = 0
);