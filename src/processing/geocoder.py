import time
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

# Importamos tu clase de limpieza robusta
from src.utils.clean_text import AddressCleaner


class GeocoderService:
    """
    Handles interactions with the OpenStreetMap API (Nominatim).
    Includes logic for:
    - Text cleaning (via AddressCleaner)
    - Rate limiting (to avoid bans)
    - Fallback strategies (handling private clusters like 'Zikura, Zibata')
    """

    def __init__(self, app_name="qro_real_estate_analyzer_v1"):
        # 1. Setup Client: Nominatim requires a unique user_agent
        self.geolocator = Nominatim(user_agent=app_name)

        # 2. Setup Rate Limiter: Crucial to prevent 429 Errors (Too Many Requests)
        # We wait 1 second between requests to be polite to the free API.
        self.geocode_api = RateLimiter(self.geolocator.geocode, min_delay_seconds=1.0)

        # 3. Context
        self.macro_context = ", Querétaro, México"

    def _query_api(self, query_string):
        """
        Internal helper to execute the safe API call.
        """
        try:
            # We add context back to ensure we don't find "Jurica" in Spain or Colombia
            full_query = f"{query_string}{self.macro_context}"
            return self.geocode_api(full_query)
        except (GeocoderTimedOut, GeocoderUnavailable):
            time.sleep(2)  # Wait a bit more if API is struggling
            return None
        except Exception as e:
            print(f"API Error on '{query_string}': {e}")
            return None

    def get_coordinates(self, raw_address):
        """
        Main logic: Tries to find coordinates implementing a Fallback Strategy.
        Returns: (latitude, longitude) or (None, None)
        """
        # Step 1: Clean the text using your robust cleaner
        clean_address = AddressCleaner.clean(raw_address)

        if not clean_address:
            return None, None

        # Step 2: Primary Attempt (Exact Match)
        location = self._query_api(clean_address)

        # Step 3: Fallback Strategy (The fix for 'Meseta, Sonterra' or 'Zikura, Zibata')
        # If exact match fails AND there is a comma, try searching for the parent neighborhood.
        if not location and ',' in clean_address:
            # Logic: Split "zikura, zibatá" -> take "zibatá"
            # We assume the last part is the most recognized macro-location.
            parts = clean_address.split(',')
            if len(parts) > 1:
                broader_address = parts[-1].strip()  # Take the part after the comma
                if len(broader_address) > 3:  # Safety check
                    # print(f"   ↳ Retrying with broader term: '{broader_address}'") # Debug
                    location = self._query_api(broader_address)

        if location:
            return location.latitude, location.longitude
        return None, None

    def process_batch(self, df: pd.DataFrame, address_col: str) -> pd.DataFrame:
        """
        Applies geocoding to a DataFrame with progress tracking.
        """
        print(f"Starting Geocoding for {len(df)} rows...")
        print("   This process includes rate limiting (1s delay). Please wait.")

        # Create copies to avoid SettingWithCopy warnings
        df = df.copy()

        # Apply logic row by row
        # Using a simple loop or apply. For large datasets, tqdm is recommended but optional.
        results = df[address_col].apply(self.get_coordinates)

        # Unpack results into new columns
        df['latitude'] = results.apply(lambda x: x[0] if x else None)
        df['longitude'] = results.apply(lambda x: x[1] if x else None)

        success_rate = df['latitude'].notnull().mean()
        print(f"Geocoding Complete. Success Rate: {success_rate:.1%}")

        return df


# Entry point for testing the script directly
if __name__ == "__main__":
    # Test with the hard cases we identified earlier
    test_data = [
        "meseta, sonterra",  # Should work via Fallback (finding Sonterra)
        "moderna con excelente jurica",  # Should work via Cleaning (finding Jurica)
        "zikura, zibatá, el marqués"  # Should work via Fallback (finding Zibatá)
    ]

    df_test = pd.DataFrame(test_data, columns=['location'])

    geocoder = GeocoderService()
    df_result = geocoder.process_batch(df_test, 'location')

    print("\n--- TEST RESULTS ---")
    print(df_result[['location', 'latitude', 'longitude']])