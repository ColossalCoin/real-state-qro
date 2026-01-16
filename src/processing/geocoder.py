import pandas as pd
import re
import os
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

# --- CONFIGURATION ---
INPUT_FILE = 'data/raw/real_estate_queretaro_dataset.csv'
OUTPUT_FILE = 'data/processed/geocoded_data.csv'
CACHE_FILE = 'data/processed/geocoding_cache.csv'  # Temporary storage for unique locations
USER_AGENT = "portfolio_project_qro_analysis_v3_optimized"
SAVE_BATCH_SIZE = 10


class AddressCleaner:
    """
    Service class for normalizing real estate address strings.
    """
    NOISE_PATTERNS = [
        r"venta de casa en", r"casa en venta", r"en venta",
        r"venta", r"preventa", r"remate", r"oportunidad",
        r"fraccionamiento", r"residencial", r"condominio",
        r"lotes?", r"terrenos?", r"departamentos?", r"casas?"
    ]

    @staticmethod
    def clean(raw_address: str) -> str:
        if not isinstance(raw_address, str) or len(raw_address) < 3:
            return ""
        cleaned = raw_address.lower()
        for pattern in AddressCleaner.NOISE_PATTERNS:
            cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\b(quer[ée]taro|m[ée]xico|qro)\b', '', cleaned)
        cleaned = re.sub(r'\b(.+?)(?:[\s,]+)\1\b', r'\1', cleaned)
        cleaned = re.sub(r'^\s*(?:en|de)\b\s*', '', cleaned)
        cleaned = re.sub(r'[^a-z0-9\s,áéíóúñ]', '', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        cleaned = re.sub(r'^,+,*|,*,$', '', cleaned)
        return cleaned


def load_cache(cache_path):
    """Loads existing unique geocoded addresses."""
    if os.path.exists(cache_path):
        return pd.read_csv(cache_path)
    return pd.DataFrame(columns=['clean_address', 'latitude', 'longitude'])


def process_geocoding_optimized(input_path, output_path, cache_path):
    print(f"--- STARTING OPTIMIZED GEOCODER (UNIQUE STRATEGY) ---")

    # 1. Load and Clean Data
    print(f"Loading data from {input_path}...")
    df = pd.read_csv(input_path)

    # Auto-detect column
    target_col = 'location'
    if target_col not in df.columns:
        candidates = [c for c in df.columns if 'location' in c or 'address' in c]
        if candidates: target_col = candidates[0]

    print("Cleaning addresses...")
    df['clean_address'] = df[target_col].apply(AddressCleaner.clean)

    # Filter valid
    df_valid = df[df['clean_address'].str.len() > 3].copy()

    # 2. Identify UNIQUES
    # This is the optimization step: Get only unique strings
    unique_addresses = df_valid['clean_address'].unique()
    print(f"Total rows: {len(df_valid)}")
    print(f"Unique locations to geocode: {len(unique_addresses)}")
    print(f"Optimization Factor: {len(df_valid) / len(unique_addresses):.2f}x faster")

    # 3. Load Cache (Resume Logic)
    df_cache = load_cache(cache_path)
    cached_addresses = set(df_cache['clean_address'].tolist())

    # Identify which uniques are NOT in cache
    pending_addresses = [addr for addr in unique_addresses if addr not in cached_addresses]

    if len(pending_addresses) == 0:
        print("All unique addresses are already in cache!")
    else:
        print(f"Addresses pending geocoding: {len(pending_addresses)}")

        # 4. Geocoding Loop (Only for pending uniques)
        geolocator = Nominatim(user_agent=USER_AGENT, timeout=10)
        geocode_service = RateLimiter(geolocator.geocode, min_delay_seconds=1.1)

        temp_results = []

        print("Geocoding unique locations...")
        for address in tqdm(pending_addresses):
            result = {
                'clean_address': address,
                'latitude': None,
                'longitude': None
            }

            try:
                query = f"{address}, Querétaro, México"
                location = geocode_service(query)
                if location:
                    result['latitude'] = location.latitude
                    result['longitude'] = location.longitude
            except Exception:
                pass  # Skip on error

            temp_results.append(result)

            # Incremental Save to Cache
            if len(temp_results) >= SAVE_BATCH_SIZE:
                batch_df = pd.DataFrame(temp_results)
                header = not os.path.exists(cache_path)
                batch_df.to_csv(cache_path, mode='a', header=header, index=False)
                temp_results = []

        # Save remaining
        if temp_results:
            pd.DataFrame(temp_results).to_csv(cache_path, mode='a', header=False, index=False)

    # 5. MERGE AND SAVE FINAL OUTPUT
    print("Merging coordinates back to original dataset...")

    # Reload full cache to ensure we have everything
    final_cache = pd.read_csv(cache_path)

    # Merge: SQL Left Join concept
    # We take original valid DF and attach coords based on 'clean_address'
    df_final = df_valid.merge(final_cache, on='clean_address', how='left')

    # Save final result
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_final.to_csv(output_path, index=False)

    hit_rate = df_final['latitude'].notnull().mean() * 100
    print(f"\n--- SUCCESS ---")
    print(f"Final dataset saved to: {output_path}")
    print(f"Total Rows: {len(df_final)}")
    print(f"Geocoding Hit Rate: {hit_rate:.2f}%")


if __name__ == "__main__":
    process_geocoding_optimized(INPUT_FILE, OUTPUT_FILE, CACHE_FILE)