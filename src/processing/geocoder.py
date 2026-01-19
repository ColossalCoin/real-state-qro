import pandas as pd
import os
import sys
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

# Add project root to path to ensure imports work if run as script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# IMPORTING THE CLEANER CLASS
from src.utils.clean_text import AddressCleaner

# --- CONFIGURATION ---
INPUT_FILE = 'data/raw/real_estate_data.csv'
OUTPUT_FILE = 'data/processed/geocoded_data.csv'
CACHE_FILE = 'data/processed/geocoding_cache.csv'  # Dimension Table (Unique Locations)
USER_AGENT = "portfolio_project_qro_analysis_v4_modular"
SAVE_BATCH_SIZE = 10


def load_cache(cache_path):
    """Loads existing unique geocoded addresses (Dimension Table)."""
    if os.path.exists(cache_path):
        return pd.read_csv(cache_path)
    return pd.DataFrame(columns=['clean_address', 'latitude', 'longitude', 'precision'])


def process_geocoding(input_path, output_path, cache_path):
    print(f"--- STARTING MODULAR GEOCODER (FALLBACK STRATEGY ENABLED) ---")

    # 1. LOAD AND CLEAN
    print(f"Loading data from {input_path}...")
    try:
        df = pd.read_csv(input_path)
    except FileNotFoundError:
        print(f"Error: File not found at {input_path}")
        return

    # Auto-detect location column
    target_col = 'location'
    if target_col not in df.columns:
        candidates = [c for c in df.columns if 'location' in c or 'address' in c]
        if candidates: target_col = candidates[0]

    print("Step 1: Cleaning addresses using 'src.utils.clean_text'...")
    # Using the imported class
    df['clean_address'] = df[target_col].apply(AddressCleaner.clean)

    # Filter valid rows (>3 chars)
    df_valid = df[df['clean_address'].str.len() > 3].copy()

    # 2. OPTIMIZATION: IDENTIFY UNIQUES
    unique_addresses = df_valid['clean_address'].unique()
    print(f"Total rows: {len(df_valid)}")
    print(f"Unique locations to geocode: {len(unique_addresses)}")
    print(f"Optimization Factor: {len(df_valid) / len(unique_addresses):.2f}x faster")

    # 3. LOAD CACHE (RESUME LOGIC)
    df_cache = load_cache(cache_path)
    cached_addresses = set(df_cache['clean_address'].tolist())

    # Identify pending uniques
    pending_addresses = [addr for addr in unique_addresses if addr not in cached_addresses]

    if len(pending_addresses) == 0:
        print("All unique addresses are already in cache!")
    else:
        print(f"Addresses pending geocoding: {len(pending_addresses)}")

        # 4. GEOCODING LOOP
        print("Step 2: Querying API (with Fallback Strategy for robustness)...")

        # Increase timeout to 10s to prevent ReadTimeoutError
        geolocator = Nominatim(user_agent=USER_AGENT, timeout=10)
        geocode_service = RateLimiter(geolocator.geocode, min_delay_seconds=1.1)

        temp_results = []

        for address in tqdm(pending_addresses, desc="Geocoding"):
            result = {
                'clean_address': address,
                'latitude': None,
                'longitude': None,
                'precision': 'exact'  # Metadata for quality control
            }

            try:
                # ATTEMPT 1: Exact Match
                # e.g. "Zikura, Zibatá, El Marqués, Querétaro, México"
                query = f"{address}, Querétaro, México"
                location = geocode_service(query)

                # ATTEMPT 2: Fallback Strategy (If exact match fails)
                # If "Cluster, Neighborhood" fails, try just "Neighborhood"
                if location is None and ',' in address:
                    parts = address.split(',')
                    # Remove the first part (most specific) and join the rest
                    broader_address = ",".join(parts[1:]).strip()

                    if len(broader_address) > 3:
                        query_retry = f"{broader_address}, Querétaro, México"
                        location = geocode_service(query_retry)
                        if location:
                            result['precision'] = 'approximate'  # Flag as broader area

                if location:
                    result['latitude'] = location.latitude
                    result['longitude'] = location.longitude

            except Exception as e:
                # Log error silently to continue processing
                pass

            temp_results.append(result)

            # Incremental Save
            if len(temp_results) >= SAVE_BATCH_SIZE:
                batch_df = pd.DataFrame(temp_results)
                header = not os.path.exists(cache_path)
                batch_df.to_csv(cache_path, mode='a', header=header, index=False)
                temp_results = []

        # Save remaining
        if temp_results:
            pd.DataFrame(temp_results).to_csv(cache_path, mode='a', header=False, index=False)

    # 5. DATA MODELING EXPORT (BIGQUERY READY)
    print("\nStep 3: Generating Star Schema Files...")

    # A) Fact Table: Listings with Foreign Key (clean_address)
    fact_table_path = output_path.replace('.csv', '_fact_listings.csv')
    # Save original columns + clean_address
    df_valid.to_csv(fact_table_path, index=False)

    # B) Dimension Table: Geography (Unique Locations)
    # Reload full cache to ensure completeness
    final_dim_table = pd.read_csv(cache_path)

    # C) Flat Table (Optional, for local analysis/debugging)
    print("Merging for local flat file...")
    df_final = df_valid.merge(final_dim_table, on='clean_address', how='left')
    df_final.to_csv(output_path, index=False)

    hit_rate = final_dim_table['latitude'].notnull().mean() * 100

    print(f"\n--- SUCCESS ---")
    print(f"1. [Fact Table]  Listings:   {fact_table_path}")
    print(f"2. [Dim Table]   Geography:  {cache_path}")
    print(f"3. [Flat Table]  Combined:   {output_path}")
    print(f"Geocoding Success Rate (on unique locations): {hit_rate:.2f}%")


if __name__ == "__main__":
    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    process_geocoding(INPUT_FILE, OUTPUT_FILE, CACHE_FILE)