"""
ETL Script para Descarga de Datos de Socios Comerciales
========================================================

Descarga datos bilaterales de comercio (importaciones y exportaciones) para:
- 31 países europeos
- 40 socios comerciales principales (20 UE + 20 extra-UE)
- 10 sectores SITC nivel 1 (0-9)
- Periodo: 2020-2025

Fuente: Eurostat API DS-059331
"""

import requests
import pandas as pd
from pathlib import Path
import time
from io import StringIO

# URL base de la API de Eurostat
BASE_URL = "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/3.0/data/dataflow/ESTAT/ds-059331/1.0/*.*.*.*.*.*"

# Directorio de cache
CACHE_DIR = Path('data/partners')
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# 31 países europeos (reporters)
REPORTERS = [
    'AT',  # Austria
    'BE',  # Bélgica
    'BG',  # Bulgaria
    'HR',  # Croacia
    'CY',  # Chipre
    'CZ',  # Chequia
    'DK',  # Dinamarca
    'EE',  # Estonia
    'FI',  # Finlandia
    'FR',  # Francia
    'DE',  # Alemania
    'GR',  # Grecia
    'HU',  # Hungría
    'IE',  # Irlanda
    'IT',  # Italia
    'LV',  # Letonia
    'LT',  # Lituania
    'LU',  # Luxemburgo
    'MT',  # Malta
    'NL',  # Países Bajos
    'PL',  # Polonia
    'PT',  # Portugal
    'RO',  # Rumanía
    'SK',  # Eslovaquia
    'SI',  # Eslovenia
    'ES',  # España
    'SE',  # Suecia
    'GB',  # Reino Unido
    'CH',  # Suiza
    'NO',  # Noruega
    'IS',  # Islandia
]

# 40 socios comerciales principales (20 UE-27 + 20 extra-UE)
PARTNERS = [
    # UE-27 (20 principales)
    'FR', 'DE', 'IT', 'NL', 'BE', 'ES', 'PL', 'AT', 'CZ', 'SE',
    'DK', 'PT', 'RO', 'HU', 'FI', 'IE', 'GR', 'SK', 'BG', 'HR',
    # Extra-UE (20 principales)
    'GB',  # Reino Unido
    'CH',  # Suiza
    'NO',  # Noruega
    'CN',  # China
    'US',  # Estados Unidos
    'TR',  # Turquía
    'RU',  # Rusia
    'JP',  # Japón
    'IN',  # India
    'KR',  # Corea del Sur
    'BR',  # Brasil
    'MX',  # México
    'CA',  # Canadá
    'AU',  # Australia
    'SA',  # Arabia Saudita
    'AE',  # Emiratos Árabes Unidos
    'ZA',  # Sudáfrica
    'SG',  # Singapur
    'TH',  # Tailandia
    'MY',  # Malasia
]

# 10 sectores SITC nivel 1 (0-9)
# NOTA: TOTAL se calculará sumando estos 10 sectores
SECTORES_SITC = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

# Mapeo de códigos SITC a nombres descriptivos
SECTORES_NOMBRES = {
    '0': 'Alimentos y animales vivos',
    '1': 'Bebidas y tabaco',
    '2': 'Materiales crudos',
    '3': 'Combustibles minerales',
    '4': 'Aceites y grasas',
    '5': 'Productos químicos',
    '6': 'Manufacturas por material',
    '7': 'Maquinaria y transporte',
    '8': 'Manufacturas diversas',
    '9': 'Otros'
}


def download_partner_data(reporter, flow, start_period='2002-01', end_period='2025-12'):
    """
    Descarga datos de socios comerciales para un país y flujo específico.

    Args:
        reporter (str): Código ISO del país reporter (e.g., 'ES')
        flow (str): '1' para importaciones, '2' para exportaciones
        start_period (str): Periodo inicio en formato YYYY-MM
        end_period (str): Periodo fin en formato YYYY-MM

    Returns:
        pd.DataFrame: DataFrame con los datos descargados, o DataFrame vacío si error
    """
    flow_name = 'imports' if flow == '1' else 'exports'
    cache_file = CACHE_DIR / f"partners_{reporter}_{flow_name}.csv"

    # Verificar si existe cache válido (menos de 7 días)
    if cache_file.exists():
        age_days = (time.time() - cache_file.stat().st_mtime) / 86400
        if age_days < 7:
            print(f"✓ Cache válido para {reporter} {flow_name}: {cache_file.name}")
            return pd.read_csv(cache_file)

    print(f"📥 Descargando {reporter} {flow_name}...")

    # Parámetros de la petición
    # 10 sectores SITC (0-9) en una sola petición
    products_str = ','.join(SECTORES_SITC)

    params = {
        'c[freq]': 'M',                              # Frecuencia mensual
        'c[reporter]': reporter,                      # País reporter
        'c[partner]': ','.join(PARTNERS),            # 40 socios comerciales
        'c[product]': products_str,                  # 10 sectores SITC
        'c[flow]': flow,                             # 1=imports, 2=exports
        'c[indicators]': 'VALUE_EUR',                # Valor en euros
        'c[TIME_PERIOD]': f'ge:{start_period}+le:{end_period}',
        'compress': 'false',
        'format': 'csvdata',
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=120)
        response.raise_for_status()

        # Parsear CSV
        df = pd.read_csv(StringIO(response.text))

        # Guardar en cache
        df.to_csv(cache_file, index=False)
        print(f"   ✓ {len(df):,} registros guardados en {cache_file.name}")

        return df

    except requests.exceptions.Timeout:
        print(f"   ✗ Error: Timeout después de 120 segundos")
        return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"   ✗ Error en la petición: {e}")
        return pd.DataFrame()

    except Exception as e:
        print(f"   ✗ Error inesperado: {e}")
        return pd.DataFrame()


def update_all_partners_data():
    """
    Descarga datos de socios comerciales para todos los países y flujos.

    Total: 31 países × 2 flujos = 62 archivos CSV
    Tiempo estimado: 10-15 minutos
    """
    print("=" * 80)
    print("DESCARGA DE DATOS DE SOCIOS COMERCIALES")
    print("=" * 80)
    print(f"📊 Configuración:")
    print(f"   - Países: {len(REPORTERS)} reporters")
    print(f"   - Socios: {len(PARTNERS)} partners")
    print(f"   - Sectores: {len(SECTORES_SITC)} (SITC 0-9)")
    print(f"   - Periodo: 2002-01 a 2025-12")
    print(f"   - Directorio: {CACHE_DIR.absolute()}")
    print("=" * 80)
    print()

    total_files = len(REPORTERS) * 2
    completed = 0
    errors = 0

    start_time = time.time()

    for reporter in REPORTERS:
        for flow in ['1', '2']:  # 1=imports, 2=exports
            result = download_partner_data(reporter, flow)

            completed += 1
            if result.empty:
                errors += 1

            progress_pct = (completed / total_files) * 100
            print(f"Progreso: {completed}/{total_files} ({progress_pct:.1f}%)")
            print()

            # Rate limiting para no saturar la API
            time.sleep(1)

    elapsed_time = time.time() - start_time
    elapsed_minutes = elapsed_time / 60

    print("=" * 80)
    print("✅ DESCARGA COMPLETADA")
    print("=" * 80)
    print(f"📁 Archivos guardados en: {CACHE_DIR.absolute()}")
    print(f"📊 Estadísticas:")
    print(f"   - Total archivos: {completed}")
    print(f"   - Exitosos: {completed - errors}")
    print(f"   - Errores: {errors}")
    print(f"   - Tiempo total: {elapsed_minutes:.1f} minutos")
    print()

    # Calcular tamaño total
    total_size = sum(f.stat().st_size for f in CACHE_DIR.glob('*.csv'))
    total_size_mb = total_size / (1024 * 1024)
    print(f"💾 Tamaño total: {total_size_mb:.1f} MB")
    print()

    if errors > 0:
        print(f"⚠️  Advertencia: {errors} archivos no se pudieron descargar")
        print("   Vuelve a ejecutar el script para reintentar")


if __name__ == "__main__":
    update_all_partners_data()
