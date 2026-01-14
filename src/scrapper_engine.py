import asyncio
import re
import os
import random
import pandas as pd
from datetime import datetime
from playwright.async_api import async_playwright

# --- CONFIGURATION CONSTANTS ---
# Update this path if your Chrome installation is different
CHROME_PATH = r"C:/chrome-win64/chrome.exe"
BASE_URL_ROOT = "https://www.vivanuncios.com.mx/s-casas-en-venta/queretaro/"
URL_SUFFIX = "v1c1293l1021p"
OUTPUT_DIR = "data/raw"
OUTPUT_FILENAME = "real_estate_queretaro_dataset.csv"


class RealEstateScraper:
    """
    A professional-grade scraper for Real Estate data designed for Vivanuncios.

    Features:
    - Waterfall Strategy for Price Extraction (Metadata -> Title -> CSS -> Body).
    - Deep Attribute Parsing (Land vs. Construction Area, Parking spots).
    - Hidden Geolocation Mining.
    - Incremental Batch Saving (Fault tolerance).
    """

    def __init__(self, headless=True, max_pages=1):
        self.headless = headless
        self.max_pages = max_pages
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None
        # In-memory buffer for final statistics, though data is saved incrementally.
        self.data_buffer = []

    @staticmethod
    def extract_number(text: str) -> float:
        """
        Utility to extract the first valid float number from a string.
        Handles commas and currency symbols.
        """
        if not text:
            return None
        # Regex to find numbers like 5,000,000 or 5000000.00
        match = re.search(r"(\d[\d,\.]*)", text)
        if match:
            clean_str = match.group(1).replace(",", "")
            try:
                return float(clean_str)
            except ValueError:
                return None
        return None

    async def start_browser(self):
        """Initializes the Playwright engine and browser context."""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [SYSTEM] Initializing Browser Engine...")

        # Validate Chrome Path (Optional: Remove if using default Playwright browser)
        if not os.path.exists(CHROME_PATH):
            print(f"[WARNING] Custom Chrome path not found at {CHROME_PATH}. Attempting default launch.")
            executable_path = None
        else:
            executable_path = CHROME_PATH

        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            executable_path=executable_path,
            headless=self.headless,
            # Arguments to mimic real user behavior and avoid bot detection
            args=["--disable-blink-features=AutomationControlled", "--start-maximized"]
        )
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        self.page = await self.context.new_page()

    async def close_browser(self):
        """Performs a clean shutdown of browser resources."""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [SYSTEM] Browser session closed.")

    async def parse_listing(self, url: str) -> dict:
        """
        Core logic to extract details from a single listing page.
        Applies Regex heuristics to separate Land Area, Construction Area, and Parking.
        """
        try:
            await self.page.goto(url, timeout=45000)
            await self.page.wait_for_load_state("domcontentloaded")

            # 1. Raw Content Extraction
            body_handle = await self.page.locator("body").element_handle()
            full_text = await body_handle.inner_text()
            text_lower = full_text.lower()
            title = await self.page.title()
            html_content = await self.page.content()

            # 2. Price Extraction (Waterfall Strategy)
            final_price = None

            def is_valid_price(val):
                return val is not None and val > 100000

            # LEVEL 0: Meta Tags & JSON (Invisible Layer - Highest Accuracy)
            if not final_price:
                meta_patterns = [
                    r'property="product:price:amount"\s+content="(\d+[\d\.]*)"',
                    r'name="price"\s+content="(\d+[\d\.]*)"',
                    r'"price":\s*(\d+[\d\.]*)',
                    r'"priceAmount":\s*"?(\d+[\d\.]*)"?'
                ]
                for pattern in meta_patterns:
                    match = re.search(pattern, html_content)
                    if match:
                        val = self.extract_number(match.group(1))
                        if is_valid_price(val):
                            final_price = val
                            break

            # LEVEL 1: Title Search (High Confidence)
            if not final_price:
                title_matches = re.findall(r"\$\s?([\d,]+)", title)
                for match in title_matches:
                    val = self.extract_number(match)
                    if is_valid_price(val):
                        final_price = val
                        break

            # LEVEL 2: CSS Selectors (Medium Confidence)
            if not final_price:
                selectors = ['span[class*="ad-price"]', 'div[class*="price"]', 'h3[class*="price"]',
                             '[itemprop="price"]']
                for sel in selectors:
                    if await self.page.locator(sel).count() > 0:
                        elements = await self.page.locator(sel).all()
                        for el in elements:
                            raw_text = await el.inner_text()
                            val = self.extract_number(raw_text)
                            if is_valid_price(val):
                                final_price = val
                                break
                    if final_price: break

            # LEVEL 3: Body Fallback (Low Confidence)
            if not final_price:
                body_matches = re.findall(r"(?:\$|MN)\s?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)", full_text)
                for match in body_matches:
                    val = self.extract_number(match)
                    if is_valid_price(val):
                        final_price = val
                        break

            # 3. Structural Attributes Extraction

            # Initialize variables
            bedrooms, bathrooms, parking = None, None, None
            m2_constructed, m2_terrain = None, None

            # STRATEGY A: "Summary Strip" (Priority)
            # Extracts data from the standardized header: e.g., "80 m² lote 200 m² constr 2 estac"

            # Land / Terrain
            match_lote = re.search(r"(\d+)\s?m²\s?lote", text_lower)
            if match_lote: m2_terrain = self.extract_number(match_lote.group(1))

            # Construction
            match_constr = re.search(r"(\d+)\s?m²\s?constr", text_lower)
            if match_constr: m2_constructed = self.extract_number(match_constr.group(1))

            # Parking Spots
            match_park = re.search(r"(\d+)\s?estac", text_lower)
            if match_park: parking = self.extract_number(match_park.group(1))

            # Bedrooms (from strip)
            match_rec_strip = re.search(r"(\d+)\s?rec", text_lower)
            if match_rec_strip: bedrooms = self.extract_number(match_rec_strip.group(1))

            # Bathrooms (from strip)
            match_bath_strip = re.search(r"(\d+)\s?baños", text_lower)
            if match_bath_strip: bathrooms = self.extract_number(match_bath_strip.group(1))

            # STRATEGY B: Generic Fallback (If Summary Strip is missing)
            if not bedrooms:
                rec_match = re.search(r"(\d+)\s?(rec|hab|bed)", text_lower)
                bedrooms = float(rec_match.group(1)) if rec_match else None

            if not bathrooms:
                bath_match = re.search(r"(\d+\.?\d*)\s?(baño|bath)", text_lower)
                bathrooms = float(bath_match.group(1)) if bath_match else None

            if not m2_constructed and not m2_terrain:
                # Generic m2 search, defaulting to construction area
                generic_match = re.search(r"(\d{2,5})[\s,]*?(?:m²|m2)", text_lower)
                if generic_match:
                    m2_constructed = self.extract_number(generic_match.group(1))

            # 4. Geolocation (Deep Extraction)
            lat, lon = None, None
            geo_patterns = [
                r"q=(-?\d+\.\d+),(-?\d+\.\d+)",
                r"&ll=(-?\d+\.\d+),(-?\d+\.\d+)",
                r'"latitude":\s*(-?\d+\.\d+).*?"longitude":\s*(-?\d+\.\d+)',
                r"center=(-?\d+\.\d+)%2C(-?\d+\.\d+)"
            ]
            for pattern in geo_patterns:
                match = re.search(pattern, html_content)
                if match:
                    try:
                        lat_c, lon_c = float(match.group(1)), float(match.group(2))
                        # Bounding box filter for Queretaro to avoid bad matches
                        if 19.0 < lat_c < 22.0 and -101.5 < lon_c < -99.0:
                            lat, lon = lat_c, lon_c
                            break
                    except:
                        continue

            # 5. Location Text Fallback
            location_text = None
            try:
                loc_el = self.page.locator('span[class*="location"], div[class*="location"], [itemprop="address"]')
                if await loc_el.count() > 0:
                    location_text = await loc_el.first.inner_text()
            except:
                pass

            if not location_text and title:
                location_text = title

            return {
                "title": title,
                "price_numeric": final_price,
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "parking_spots": parking,
                "m2_constructed": m2_constructed,
                "m2_terrain": m2_terrain,
                "latitude": lat,
                "longitude": lon,
                "location_text": location_text,
                "url": url,
                "extraction_date": datetime.now().isoformat()
            }

        except Exception as e:
            print(f"[WARNING] Extraction failed for URL {url}: {str(e)}")
            return None

    def save_batch(self, new_data: list):
        """
        Saves a batch of data to the CSV file immediately.
        Uses 'append' mode to ensure data persistence if the script stops.
        """
        if not new_data:
            return

        df = pd.DataFrame(new_data)

        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        csv_path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

        # Check if file exists to determine if headers are needed
        file_exists = os.path.isfile(csv_path)

        try:
            # Mode 'a' = Append. Encoding 'utf-8-sig' ensures correct Spanish character rendering in Excel.
            df.to_csv(csv_path, index=False, mode='a', header=not file_exists, encoding='utf-8-sig')
            print(f"   [SYSTEM] 💾 Batch of {len(new_data)} records saved to disk.")
        except Exception as e:
            print(f"   [ERROR] Incremental save failed: {e}")

    async def run(self):
        """
        Main execution flow:
        1. Navigates pagination.
        2. Extracts listing URLs.
        3. Parses details deeply.
        4. Saves data incrementally.
        """
        await self.start_browser()

        try:
            for current_page in range(1, self.max_pages + 1):
                if current_page == 1:
                    target_url = f"{BASE_URL_ROOT}{URL_SUFFIX}{current_page}"
                else:
                    target_url = f"{BASE_URL_ROOT}page-{current_page}/{URL_SUFFIX}{current_page}"

                print(f"\n--> [Page {current_page}/{self.max_pages}] Navegando: {target_url}")

                batch_data = []  # Temporary buffer for the current page

                try:
                    await self.page.goto(target_url, timeout=60000)

                    # Wait for listing cards to load
                    card_selector = 'div[class*="postingCard-module__posting-container"]'
                    try:
                        await self.page.wait_for_selector(card_selector, timeout=20000)
                    except:
                        print(f"   [INFO] No more listings found or pagination ended at page {current_page}.")
                        break

                    cards = await self.page.locator(card_selector).all()

                    # Collect unique URLs from the current page
                    page_urls = []
                    for card in cards:
                        link_locator = card.locator("a")
                        if await link_locator.count() > 0:
                            raw = await link_locator.first.get_attribute("href")
                            if raw:
                                full_link = f"https://www.vivanuncios.com.mx{raw}" if raw.startswith("/") else raw
                                if full_link not in page_urls:
                                    page_urls.append(full_link)

                    print(f"    Found {len(page_urls)} listings. Starting deep mining...")

                    # Process each listing URL
                    for i, link in enumerate(page_urls):
                        # Progress log
                        if i % 5 == 0:
                            print(f"       Processing {i + 1}/{len(page_urls)}...")

                        data = await self.parse_listing(link)
                        if data:
                            data['source_page'] = current_page
                            batch_data.append(data)
                            self.data_buffer.append(data)

                        # Random delay to mimic human behavior
                        await self.page.wait_for_timeout(random.uniform(400, 900))

                    # --- BATCH SAVE POINT ---
                    # Save data to disk after every page completes
                    self.save_batch(batch_data)
                    # ------------------------

                except Exception as e:
                    print(f"[ERROR] Failed processing Page {current_page}: {e}")
                    # Continue to next page even if this one fails
                    continue

        finally:
            await self.close_browser()
            print(f"\n[FINISH] Extraction process completed.")
            print(f"Total records processed in session: {len(self.data_buffer)}")
            print(f"Data saved at: {os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)}")


# --- ENTRY POINT ---
if __name__ == "__main__":
    # CONFIGURATION FOR MASS EXTRACTION
    # headless=True: Runs browser in background (allows you to use the PC).
    # max_pages=409: Covers the estimated total pagination for Querétaro.

    print("--- Starting Real Estate Scraper Engine ---")
    scraper = RealEstateScraper(headless=True, max_pages=409)

    # Run the asynchronous loop
    asyncio.run(scraper.run())