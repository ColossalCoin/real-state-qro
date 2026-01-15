import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from pathlib import Path
import time
import logging
import sys
import os

# Configuración de logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class EstateGeocoder:
    def __init__(self, user_agent: str = "portfolio_real_estate_project", delay_seconds: int = 1.2):
        self.geolocator = Nominatim(user_agent=user_agent)
        self.geocode_service = RateLimiter(self.geolocator.geocode, min_delay_seconds=delay_seconds)

    def _fetch_coordinates(self, location_name: str):
        if not location_name or pd.isna(location_name):
            return None, None
        try:
            # Añadimos contexto
            search_query = f"{location_name}, Queretaro, Mexico"
            location = self.geocode_service(search_query)
            if location:
                return location.latitude, location.longitude
            return None, None
        except Exception as e:
            logging.error(f"Error buscando {location_name}: {e}")
            time.sleep(2)
            return None, None

    def process_new_locations(self, raw_df: pd.DataFrame, output_path: Path, location_col: str):
        """
        Identifica ubicaciones nuevas, las geocodifica y las anexa al archivo existente.
        """
        # 1. Obtener lista total de ubicaciones necesarias
        all_required_locations = set(raw_df[location_col].dropna().unique())

        # 2. Detectar qué ya tenemos procesado
        known_locations = set()
        if output_path.exists():
            try:
                existing_df = pd.read_csv(output_path)
                # Asumimos que la columna se llama 'location_name' en el output
                known_locations = set(existing_df['location_name'].unique())
                logging.info(f"Se encontraron {len(known_locations)} ubicaciones ya procesadas previamente.")
            except Exception as e:
                logging.warning(f"No se pudo leer el archivo existente (se rehará): {e}")

        # 3. Calcular el DELTA (Lo que falta)
        # Operación de conjuntos: A - B
        missing_locations = list(all_required_locations - known_locations)

        if not missing_locations:
            logging.info("¡Todo está al día! No hay nuevas ubicaciones para geocodificar.")
            return

        logging.info(f"Iniciando procesamiento de {len(missing_locations)} NUEVAS ubicaciones...")

        # 4. Procesar en BATCH (Iterar y Guardar)
        new_data = []
        batch_size = 5  # Guardamos en disco cada 5 ubicaciones para no perder datos

        for i, loc in enumerate(missing_locations):
            lat, lon = self._fetch_coordinates(loc)

            record = {
                'location_name': loc,
                'latitude': lat,
                'longitude': lon
            }
            new_data.append(record)

            # Log visual
            status = "✅ Found" if lat else "❌ Not Found"
            logging.info(f"[{i + 1}/{len(missing_locations)}] {loc} -> {status}")

            # 5. Guardado Incremental (Batch Save)
            if len(new_data) >= batch_size or i == len(missing_locations) - 1:
                self._append_to_csv(new_data, output_path)
                new_data = []  # Limpiar buffer

    def _append_to_csv(self, data: list, filepath: Path):
        """Helper para guardar una lista de dicts en CSV modo Append"""
        if not data:
            return

        df_chunk = pd.DataFrame(data)

        # Si el archivo no existe, escribimos con encabezados (header=True)
        # Si ya existe, escribimos SIN encabezados (header=False) y modo 'a' (append)
        file_exists = filepath.exists()

        df_chunk.to_csv(
            filepath,
            mode='a',
            index=False,
            header=not file_exists
        )
        logging.info(f"--> Guardado batch de {len(df_chunk)} registros en disco.")


def run_pipeline():
    # --- Configuración de Rutas ---
    try:
        BASE_DIR = Path(__file__).resolve().parent.parent.parent
    except NameError:
        BASE_DIR = Path(os.getcwd())

    INPUT_FILE = BASE_DIR / "data" / "raw" / "real_estate_queretaro_dataset.csv"
    OUTPUT_FILE = BASE_DIR / "data" / "processed" / "dim_locations.csv"

    # --- Validación ---
    if not INPUT_FILE.exists():
        logging.error(f"Falta archivo de entrada: {INPUT_FILE}")
        return

    # Asegurar que la carpeta de destino exista
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    # --- Ejecución ---
    df_raw = pd.read_csv(INPUT_FILE)
    geocoder = EstateGeocoder(delay_seconds=1.1)

    # Llamamos a la nueva función inteligente
    geocoder.process_new_locations(df_raw, OUTPUT_FILE, location_col='location')


if __name__ == "__main__":
    run_pipeline()