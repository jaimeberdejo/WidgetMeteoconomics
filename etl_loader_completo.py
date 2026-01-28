"""
ETL Loader para Balanza Comercial COMPLETA (Mercancías + Servicios)
Datasets:
  - DS-059331: International trade of goods (Comext) - MENSUAL
  - BOP_C6_Q: Balance of payments - Services (including Travel/Tourism) - TRIMESTRAL

Este script descarga datos de mercancías (mensuales) y servicios (trimestrales).
Los datos trimestrales de servicios se interpolan automáticamente a mensuales
dividiendo cada valor trimestral entre 3 para los 3 meses correspondientes.

Estrategia de interpolación:
  Q1 (Ene-Mar) → 3 meses con valor/3
  Q2 (Abr-Jun) → 3 meses con valor/3
  Q3 (Jul-Sep) → 3 meses con valor/3
  Q4 (Oct-Dic) → 3 meses con valor/3

Autor: ETL Balanza Completa
Fuente: Eurostat Comext + Eurostat BOP Database - API SDMX
"""

import requests
from datetime import datetime, timedelta
from typing import List
import io

# Configuración
from pathlib import Path
Path('data/goods').mkdir(parents=True, exist_ok=True)
Path('data/services').mkdir(parents=True, exist_ok=True)

CSV_CACHE_FILE_GOODS = 'data/goods/datos_mercancias_cache.csv'
CSV_CACHE_FILE_SERVICES = 'data/services/datos_servicios_cache.csv'
CSV_CACHE_FILE_COMBINED = 'data/datos_balanza_completa_cache.csv'

# Países a descargar (códigos Eurostat) - EXACTAMENTE COMO EN LA URL DE REFERENCIA
# Basado en: c[reporter]=AL,AT,BA,BE,BG,CH,CY,CZ,DE,DK,EE,ES,EU27_2020,FI,FR,GB,GE,GR,HR,HU,IE,IS,IT,LI,LT,LU,LV,MD,ME,MK,MT,NL,NO,PL,PT,RO,SE,SI,SK,TR,UA,XI,XK,XM,XS
PAISES_CODES = {
    # Unión Europea (27)
    'AT': 'Austria',
    'BE': 'Bélgica',
    'BG': 'Bulgaria',
    'HR': 'Croacia',
    'CY': 'Chipre',
    'CZ': 'República Checa',
    'DK': 'Dinamarca',
    'EE': 'Estonia',
    'FI': 'Finlandia',
    'FR': 'Francia',
    'DE': 'Alemania',
    'GR': 'Grecia',
    'HU': 'Hungría',
    'IE': 'Irlanda',
    'IT': 'Italia',
    'LV': 'Letonia',
    'LT': 'Lituania',
    'LU': 'Luxemburgo',
    'MT': 'Malta',
    'NL': 'Países Bajos',
    'PL': 'Polonia',
    'PT': 'Portugal',
    'RO': 'Rumanía',
    'SK': 'Eslovaquia',
    'SI': 'Eslovenia',
    'ES': 'España',
    'SE': 'Suecia',

    # Países importantes fuera de la UE
    'GB': 'Reino Unido',
    'NO': 'Noruega',
    'CH': 'Suiza',

    # Agregados
    'EU27_2020': 'Unión Europea (27)',
}

# Mapeo de nombres completos de Eurostat a nuestros nombres
PAISES_NOMBRES = {
    # UE-27
    'Austria': 'Austria',
    'Belgium': 'Bélgica',
    'Bulgaria': 'Bulgaria',
    'Croatia': 'Croacia',
    'Cyprus': 'Chipre',
    'Czech Republic': 'República Checa',
    'Czechia': 'República Checa',
    'Denmark': 'Dinamarca',
    'Estonia': 'Estonia',
    'Finland': 'Finlandia',
    'France': 'Francia',
    'Germany': 'Alemania',
    "Germany (incl. German Democratic Republic 'DD' from 1991)": 'Alemania',
    'Greece': 'Grecia',
    'Hungary': 'Hungría',
    'Ireland': 'Irlanda',
    'Italy': 'Italia',
    'Latvia': 'Letonia',
    'Lithuania': 'Lituania',
    'Luxembourg': 'Luxemburgo',
    'Malta': 'Malta',
    'Netherlands': 'Países Bajos',
    'Poland': 'Polonia',
    'Portugal': 'Portugal',
    'Romania': 'Rumanía',
    'Slovakia': 'Eslovaquia',
    'Slovenia': 'Eslovenia',
    'Spain': 'España',
    'Sweden': 'Suecia',

    # Países importantes fuera de la UE
    'United Kingdom': 'Reino Unido',
    'Norway': 'Noruega',
    'Switzerland': 'Suiza',

    # Agregados
    'Euro area': 'Eurozona',
    'European Union - 27 countries (from 2020)': 'Unión Europea (27)',
    'EU27_2020': 'Unión Europea (27)',
}

# Mapeo de códigos SITC a nombres
SECTORES_SITC = {
    'TOTAL': 'Total Comercio',
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

# Mapeo de nombres de productos de Eurostat a nuestros sectores
SECTORES_NOMBRES = {
    'Food and live animals': 'Alimentos y animales vivos',
    'Beverages and tobacco': 'Bebidas y tabaco',
    'Crude materials, inedible, except fuels': 'Materiales crudos',
    'Mineral fuels, lubricants and related materials': 'Combustibles minerales',
    'Animal and vegetable oils, fats and waxes': 'Aceites y grasas',
    'Chemicals and related products, n.e.s.': 'Productos químicos',
    'Manufactured goods classified chiefly by material': 'Manufacturas por material',
    'Machinery and transport equipment': 'Maquinaria y transporte',
    'Miscellaneous manufactured articles': 'Manufacturas diversas',
    'Commodities and transactions not classified elsewhere': 'Otros',
    'Total all products': 'Total Comercio'
}


def build_eurostat_api_url(reporters: List[str], start_year: int = 2002, end_year: int = None) -> str:
    """
    Construye la URL de la API de Eurostat Comext con los parámetros correctos.

    Formato correcto descubierto:
    /sdmx/3.0/data/dataflow/ESTAT/ds-059331/1.0/*.*.*.*.*.*?c[param]=value&format=csvdata
    """
    if end_year is None:
        end_year = datetime.now().year

    # Construir lista de reporters
    reporters_str = ','.join(reporters)

    # Productos SITC
    products = ','.join(SECTORES_SITC.keys())

    # Base URL (SDMX 3.0)
    base_url = "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/3.0/data/dataflow/ESTAT/ds-059331/1.0/*.*.*.*.*.*"

    # Parámetros de consulta (usar urllib.parse para codificar correctamente)
    from urllib.parse import urlencode, quote

    params = {
        'c[freq]': 'M',
        'c[reporter]': reporters_str,
        'c[partner]': 'WORLD',
        'c[product]': products,
        'c[flow]': '1,2',
        'c[indicators]': 'VALUE_EUR',
        'c[TIME_PERIOD]': f'ge:{start_year}-01+le:{end_year}-12',
        'compress': 'false',
        'format': 'csvdata',
        'formatVersion': '1.0',
        'lang': 'en',
        'labels': 'label_only',
        'returnData': 'ALL'
    }

    # Construir URL completa con encoding apropiado
    params_str = urlencode(params, safe=':,[]')
    url = f"{base_url}?{params_str}"

    return url


def download_from_eurostat_api(reporters: List[str], start_year: int = 2002) -> str:
    """
    Descarga datos desde la API de Eurostat Comext.
    Retorna el contenido CSV como string.
    """
    url = build_eurostat_api_url(reporters, start_year)

    print(f"\n📥 Descargando desde Eurostat API...")
    print(f"   Países: {len(reporters)} países europeos + Eurozona + UE-27")
    print(f"   Período: {start_year}-01 a {datetime.now().year}-12")
    print(f"   Productos: {len(SECTORES_SITC)} sectores SITC")
    print(f"   ⚠️  Descarga completa puede tardar 2-3 minutos...")

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/csv, text/plain, application/csv, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }

        print(f"\n🔄 Realizando solicitud HTTP...")
        print(f"   URL: {url[:100]}...")

        response = requests.get(url, headers=headers, timeout=300)  # 5 minutos para descarga completa

        print(f"   Status Code: {response.status_code}")
        print(f"   Content-Type: {response.headers.get('Content-Type', 'N/A')}")
        print(f"   Content-Length: {len(response.content):,} bytes")

        if response.status_code == 200:
            print(f"   ✓ Descarga exitosa")
            return response.text
        else:
            print(f"   ✗ Error HTTP {response.status_code}")
            print(f"   Response: {response.text[:500]}")
            return None

    except requests.exceptions.Timeout:
        print(f"   ✗ Timeout: La solicitud tardó más de 120 segundos")
        return None
    except requests.exceptions.RequestException as e:
        print(f"   ✗ Error de conexión: {e}")
        return None
    except Exception as e:
        print(f"   ✗ Error inesperado: {e}")
        return None


def build_bop_services_api_url(reporters: List[str], start_year: int = 2002, end_year: int = None) -> str:
    """
    Construye la URL de la API de Eurostat BOP para datos de servicios (incluye turismo).

    Dataset: BOP_C6_Q - Balance of payments by country - QUARTERLY data
    Incluye: SC (Services), SCG (Travel/Turismo), etc.

    IMPORTANTE: Usamos dataset trimestral porque los códigos detallados de servicios
    (SC, SCG) no están disponibles en el dataset mensual (BOP_C6_M).
    Los datos trimestrales se interpolarán a mensuales posteriormente.

    Formato SDMX 3.0
    """
    if end_year is None:
        end_year = datetime.now().year

    # Filtrar solo países válidos para BOP
    # EL = Grecia (no GR), y excluir agregados como EU27_2020
    valid_bop_countries = [
        'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE',
        'EL',  # Grecia (no GR)
        'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO',
        'SK', 'SI', 'ES', 'SE', 'UK'  # UK para datos históricos
    ]

    # Mapeo de códigos: GR -> EL para BOP
    reporters_mapped = []
    for r in reporters:
        if r == 'GR':
            reporters_mapped.append('EL')
        elif r == 'GB':
            reporters_mapped.append('UK')
        elif r in valid_bop_countries:
            reporters_mapped.append(r)

    if not reporters_mapped:
        reporters_mapped = ['ES']  # Al menos España

    # Base URL - TRIMESTRAL (bop_c6_q) - mayor cobertura de países
    # Nota: BOP_C6_M (mensual) existe pero NO incluye España ni otros países clave
    base_url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/bop_c6_q/1.0/*.*.*.*.*.*.*.*"

    from urllib.parse import urlencode

    reporters_str = ','.join(reporters_mapped)

    # Usar datos TRIMESTRALES (se interpolan a mensuales en post-procesamiento)
    # IMPORTANTE: Descargar CRE (créditos/exportaciones) y DEB (débitos/importaciones)
    # en lugar de BAL (balance) para evitar tener que estimar los flujos
    params = {
        'c[freq]': 'Q',  # QUARTERLY (trimestral)
        'c[currency]': 'MIO_EUR',  # Millones de euros
        'c[bop_item]': 'S',  # S=Services totales (incluye transporte, turismo, etc.)
                             # NOTA: No incluir SC para evitar doble conteo
        'c[sector10]': 'S1',  # Total economy
        'c[sectpart]': 'S1',  # Total economy partner
        'c[stk_flow]': 'CRE,DEB',  # CRE=Créditos/Exportaciones, DEB=Débitos/Importaciones
        'c[partner]': 'WRL_REST',  # Resto del mundo
        'c[geo]': reporters_str,  # Países válidos
        'c[TIME_PERIOD]': f'ge:{start_year}-Q1+le:{end_year}-Q4',  # Formato trimestral
        'compress': 'false',
        'format': 'csvdata',
        'formatVersion': '1.0',
        'lang': 'en',
        'labels': 'label_only',
        'returnData': 'ALL'
    }

    params_str = urlencode(params, safe=':,+[]')
    url = f"{base_url}?{params_str}"

    return url


def download_bop_services(reporters: List[str], start_year: int = 2002) -> str:
    """
    Descarga datos trimestrales de servicios desde Eurostat BOP_C6_Q.
    Se interpolan a mensuales en post-procesamiento.

    Nota: BOP_C6_M (mensual) no incluye España y tiene cobertura limitada.
    """
    url = build_bop_services_api_url(reporters, start_year)

    print(f"\n📥 Descargando SERVICIOS desde Eurostat BOP API (TRIMESTRAL)...")
    print(f"   Países: {len(reporters)} países")
    print(f"   Período: {start_year}-Q1 a {datetime.now().year}-Q4")
    print(f"   Categorías: S (Servicios totales, incluye turismo)")
    print(f"   ⚠️  Datos trimestrales se interpolarán a mensuales")

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/csv, */*',
            'Accept-Language': 'en-US,en;q=0.9',
        }

        print(f"\n🔄 Realizando solicitud HTTP...")
        print(f"   URL: {url[:120]}...")

        response = requests.get(url, headers=headers, timeout=180)

        print(f"   Status Code: {response.status_code}")
        print(f"   Content-Length: {len(response.content):,} bytes")

        if response.status_code == 200:
            print(f"   ✓ Descarga exitosa")
            return response.text
        else:
            print(f"   ✗ Error HTTP {response.status_code}")
            print(f"   Response: {response.text[:500]}")
            return None

    except Exception as e:
        print(f"   ✗ Error: {e}")
        return None


def interpolate_quarterly_to_monthly(csv_content: str) -> str:
    """
    Convierte datos trimestrales (Q1, Q2, Q3, Q4) a mensuales.

    Estrategia de interpolación:
    - Q1 (Ene-Mar): Divide valor/3 para cada mes (Ene, Feb, Mar)
    - Q2 (Abr-Jun): Divide valor/3 para cada mes (Abr, May, Jun)
    - Q3 (Jul-Sep): Divide valor/3 para cada mes (Jul, Ago, Sep)
    - Q4 (Oct-Dic): Divide valor/3 para cada mes (Oct, Nov, Dic)

    Retorna CSV con TIME_PERIOD en formato mensual (YYYY-MM).
    """
    print(f"\n🔄 Interpolando datos trimestrales a mensuales...")

    import csv
    from io import StringIO

    # Leer CSV
    reader = csv.DictReader(StringIO(csv_content))
    header = reader.fieldnames

    if not header or 'TIME_PERIOD' not in header or 'OBS_VALUE' not in header:
        print(f"   ⚠️  CSV no tiene columnas esperadas")
        return csv_content

    rows_interpolated = []
    count_quarters = 0
    count_months = 0

    # Mapeo Q1->meses, Q2->meses, etc.
    quarter_to_months = {
        'Q1': ['01', '02', '03'],
        'Q2': ['04', '05', '06'],
        'Q3': ['07', '08', '09'],
        'Q4': ['10', '11', '12']
    }

    for row in reader:
        time_period = row.get('TIME_PERIOD', '')
        obs_value = row.get('OBS_VALUE', '')

        # Si es trimestral (formato: 2024-Q1)
        if '-Q' in time_period:
            try:
                year, quarter = time_period.split('-')
                quarter_num = quarter  # Q1, Q2, Q3, Q4

                if quarter_num in quarter_to_months:
                    # Si tiene valor, dividirlo entre 3
                    if obs_value and obs_value != ':' and obs_value != '':
                        count_quarters += 1
                        value_float = float(obs_value)
                        monthly_value = value_float / 3.0

                        # Crear 3 filas mensuales con valor interpolado
                        for month in quarter_to_months[quarter_num]:
                            new_row = row.copy()
                            new_row['TIME_PERIOD'] = f"{year}-{month}"
                            new_row['OBS_VALUE'] = f"{monthly_value:.2f}"
                            rows_interpolated.append(new_row)
                            count_months += 1
                    else:
                        # Sin valor, solo convertir formato de fecha (no contar)
                        for month in quarter_to_months[quarter_num]:
                            new_row = row.copy()
                            new_row['TIME_PERIOD'] = f"{year}-{month}"
                            # OBS_VALUE se mantiene vacío
                            rows_interpolated.append(new_row)
                else:
                    # Quarter no reconocido, mantener original
                    rows_interpolated.append(row)
            except (ValueError, AttributeError):
                # Si hay error, mantener fila original
                rows_interpolated.append(row)
        else:
            # Si ya es mensual, mantener original
            rows_interpolated.append(row)

    # Escribir CSV interpolado
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=header)
    writer.writeheader()
    writer.writerows(rows_interpolated)

    interpolated_csv = output.getvalue()

    print(f"   ✓ Trimestres procesados: {count_quarters}")
    print(f"   ✓ Meses generados: {count_months}")
    print(f"   ✓ Total filas: {len(rows_interpolated)}")

    return interpolated_csv


def parse_eurostat_csv(csv_content: str) -> str:
    """
    Valida el CSV de Eurostat y lo retorna sin transformaciones.
    """
    print(f"\n⚙️  Validando CSV...")

    import csv as csv_module

    # Contar filas para diagnóstico
    reader = csv_module.DictReader(io.StringIO(csv_content))
    rows_count = sum(1 for _ in reader)

    print(f"   ✓ Total de filas: {rows_count:,}")

    return csv_content


def validate_csv(csv_content: str, csv_type: str = "mercancías") -> bool:
    """
    Valida que el CSV descargado tenga contenido.
    """
    print(f"\n🔍 Validando CSV de {csv_type}...")

    if not csv_content or len(csv_content) < 100:
        print(f"   ✗ Error: CSV vacío o demasiado pequeño")
        return False

    # Verificar que tenga columnas básicas
    first_line = csv_content.split('\n')[0]

    if 'TIME_PERIOD' not in first_line or 'OBS_VALUE' not in first_line:
        print(f"   ✗ Error: Faltan columnas esenciales")
        return False

    print(f"   ✓ CSV de {csv_type} válido")
    print(f"   Tamaño: {len(csv_content) / 1024:.1f} KB")
    return True


def save_csv_cache(csv_goods: str, csv_services: str):
    """
    Guarda los CSVs de mercancías y servicios en caché.
    """
    # Guardar mercancías
    with open(CSV_CACHE_FILE_GOODS, 'w', encoding='utf-8') as f:
        f.write(csv_goods)
    print(f"\n💾 CSV guardado: {CSV_CACHE_FILE_GOODS}")
    print(f"   Tamaño: {len(csv_goods) / 1024:.1f} KB")

    # Guardar servicios
    with open(CSV_CACHE_FILE_SERVICES, 'w', encoding='utf-8') as f:
        f.write(csv_services)
    print(f"   CSV guardado: {CSV_CACHE_FILE_SERVICES}")
    print(f"   Tamaño: {len(csv_services) / 1024:.1f} KB")

    # Nota: El CSV combinado se genera en el widget al cargar los datos
    print(f"\n   ℹ️  Los datos se combinarán al cargar el widget")


def main():
    """
    Función principal - descarga datos de mercancías y servicios de Eurostat.

    Datasets:
    - DS-059331: Mercancías (mensual)
    - BOP_C6_Q: Servicios (trimestral - se interpola a mensual)

    Nota: BOP_C6_M existe pero no incluye España ni otros países clave.
    """
    print("="*70)
    print("ETL LOADER - BALANZA COMPLETA (MERCANCÍAS + SERVICIOS)")
    print(f"Datasets: DS-059331 (Goods) + BOP_C6_Q (Services trimestral)")
    print("="*70)

    reporters = list(PAISES_CODES.keys())

    # ===== DESCARGAR MERCANCÍAS =====
    print("\n" + "="*70)
    print("PASO 1: DESCARGAR DATOS DE MERCANCÍAS")
    print("="*70)

    csv_goods = download_from_eurostat_api(reporters, start_year=2002)

    if not csv_goods:
        print("\n✗ ERROR: No se pudieron descargar datos de mercancías")
        return

    csv_goods = parse_eurostat_csv(csv_goods)

    if not validate_csv(csv_goods, "mercancías"):
        print("\n✗ Error: CSV de mercancías no válido")
        return

    # ===== DESCARGAR SERVICIOS =====
    print("\n" + "="*70)
    print("PASO 2: DESCARGAR DATOS DE SERVICIOS")
    print("="*70)

    csv_services = download_bop_services(reporters, start_year=2002)

    if not csv_services:
        print("\n✗ ERROR: No se pudieron descargar datos de servicios")
        print("   Continuando solo con mercancías...")
        csv_services = ""  # CSV vacío si falla

    if csv_services:
        csv_services = parse_eurostat_csv(csv_services)
        if not validate_csv(csv_services, "servicios"):
            print("\n⚠️  Advertencia: CSV de servicios no válido, continuando sin servicios")
        else:
            # Interpolar datos trimestrales a mensuales
            csv_services = interpolate_quarterly_to_monthly(csv_services)

    # ===== GUARDAR ARCHIVOS =====
    save_csv_cache(csv_goods, csv_services)

    print("\n" + "="*70)
    print("✓ PROCESO COMPLETADO EXITOSAMENTE")
    print("="*70)
    print(f"\nArchivos generados:")
    print(f"  - {CSV_CACHE_FILE_GOODS}")
    if csv_services:
        print(f"  - {CSV_CACHE_FILE_SERVICES}")
    print("\nPuedes ejecutar el widget con:")
    print("  streamlit run widget_balanza_completa.py")
    print("="*70)


def update_data_if_needed() -> bool:
    """
    Actualiza los datos si el caché no existe, está corrupto, o es antiguo (>7 días).

    Verifica:
    1. Existencia del archivo
    2. Tamaño mínimo (>1KB para detectar archivos vacíos/corruptos)
    3. Antigüedad (<7 días)
    """
    import os

    # 1. Verificar existencia
    if not os.path.exists(CSV_CACHE_FILE_GOODS):
        print("📥 Caché no existe, descargando datos...")
        main()
        return True

    # 2. Verificar tamaño (detectar archivos corruptos/vacíos)
    file_size = os.path.getsize(CSV_CACHE_FILE_GOODS)
    if file_size < 1024:  # Menos de 1KB = probablemente corrupto
        print(f"🔄 Caché corrupto o vacío ({file_size} bytes), re-descargando...")
        main()
        return True

    # 3. Verificar antigüedad
    cache_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(CSV_CACHE_FILE_GOODS))
    if cache_age > timedelta(days=7):
        print(f"🔄 Caché antiguo ({cache_age.days} días), actualizando...")
        main()
        return True

    print(f"✓ Usando caché existente ({cache_age.days} días, {file_size / 1024 / 1024:.1f} MB)")
    return False


if __name__ == "__main__":
    main()
