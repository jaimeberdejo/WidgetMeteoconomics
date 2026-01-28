"""
ETL Completo para Socios Comerciales de SERVICIOS
Descarga iterativa + Procesamiento en un solo script
Descarga ROBUSTA: Usa curl iterativo por país y une todo en un solo CSV.
CORRECCIÓN FINAL: Usa labels=id para obtener códigos ISO (BE, FR...) validos para el proceso.

Fuente: Eurostat BOP_C6_Q (Balance of Payments - Quarterly)
Rango: 2002-Presente
Ejecutar: python3 etl_partners_services.py
"""

import subprocess
import sys
import os
import pandas as pd
from pathlib import Path
import time

CACHE_DIR = Path('data/partners_services')
CACHE_DIR.mkdir(parents=True, exist_ok=True)
FINAL_OUTPUT = CACHE_DIR / 'all_bop_services.csv'
OUTPUT_DIR = CACHE_DIR  # Directorio para archivos procesados

# --- MAPEO DE CÓDIGOS EUROSTAT -> ISO ---
EUROSTAT_TO_ISO = {
    'EL': 'GR',       # Grecia
    'UK': 'UK',       # Reino Unido
    'CN_X_HK': 'CN',  # China (excl. HK) -> Lo tratamos como el país China
    'XK': 'XK'        # Kosovo
}

# Países reporteros a procesar (31 países UE + EFTA)
TARGET_REPORTERS = [
    'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR',
    'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL',
    'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE', 'UK', 'NO', 'CH', 'IS'
]

# --- CONFIGURACIÓN DE URL ---
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/bop_c6_q/1.0/*.*.*.*.*.*.*.*"

# Países reporteros
REPORTERS = [
    "BE", "BG", "CZ", "DK", "DE", "EE", "IE", "EL", "ES", "FR", "HR", "IT", "CY", 
    "LV", "LT", "LU", "HU", "MT", "NL", "AT", "PL", "PT", "RO", "SI", "SK", "FI", 
    "SE", "IS", "NO", "CH", "UK", "BA", "ME", "MD", "MK", "AL", "RS", "TR", "XK"
]

# Socios comerciales
PARTNERS = (
    "BE,BG,CZ,DK,DE,EE,IE,EL,ES,FR,HR,IT,CY,LV,LT,LU,HU,MT,NL,AT,PL,PT,RO,SI,SK,FI,SE,"
    "CH,UK,RU,CA,US,BR,CN_X_HK,HK,JP,IN"
)

def get_curl_url(geo_code):
    """
    Genera la URL corregida.
    CAMBIO CRÍTICO: labels=id (Eurostat no acepta 'code').
    """
    params = [
        "c%5Bfreq%5D=Q",
        "c%5Bcurrency%5D=MIO_EUR",
        "c%5Bbop_item%5D=S",
        "c%5Bsector10%5D=S1",
        "c%5Bsectpart%5D=S1",
        "c%5Bstk_flow%5D=CRE,DEB",
        f"c%5Bpartner%5D={PARTNERS}",
        f"c%5Bgeo%5D={geo_code}",
        "startPeriod=2002-Q1",    
        "compress=false",
        "format=csvdata",
        "formatVersion=1.0",      # Mantiene cabecera 'geo'
        "lang=en",
        "labels=id",              # CORRECCIÓN: 'id' devuelve el código ISO (BE, ES, etc.)
        "returnData=ALL"
    ]
    return f"{BASE_URL}?{'&'.join(params)}"

def download_services_data():
    """
    FASE 1: Descarga datos de BOP iterativamente por país
    Retorna: número de registros descargados
    """
    print("\n" + "=" * 80)
    print("FASE 1: DESCARGA DATOS BOP SERVICIOS")
    print("=" * 80)
    
    # 1. Limpiar archivo final previo
    if FINAL_OUTPUT.exists():
        FINAL_OUTPUT.unlink()
        
    temp_file = CACHE_DIR / "temp_chunk.csv"
    first_chunk = True
    total_countries = len(REPORTERS)
    rows_saved = 0

    print(f"Paso 1: Descargando datos iterativamente ({total_countries} países)...")

    for i, country in enumerate(REPORTERS, 1):
        print(f"[{i}/{total_countries}] Descargando {country}...", end=" ", flush=True)
        
        url = get_curl_url(country)
        
        # Ejecutar CURL
        result = subprocess.run(
            ['curl', '-s', '-o', str(temp_file), url], 
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"✗ Error curl: {result.stderr}")
            continue

        # Verificar datos
        if not temp_file.exists() or temp_file.stat().st_size < 10:
            print("⚠️ Error API.")
            continue

        try:
            with open(temp_file, 'r', encoding='utf-8') as f_in:
                lines = f_in.readlines()
            
            # Chequeo de error XML en el contenido
            if len(lines) > 0 and "S:Fault" in lines[0]:
                print(f"✗ Error Eurostat: {lines[0][:100]}...") # Muestra el inicio del error
                continue

            # Si hay pocas líneas, puede ser solo cabecera
            if len(lines) < 2:
                print(f"⚠️ Sin datos (Solo cabecera).")
                continue
                
            with open(FINAL_OUTPUT, 'a', encoding='utf-8') as f_out:
                count = len(lines) - 1
                if first_chunk:
                    f_out.writelines(lines)
                    first_chunk = False
                    print(f"✓ Guardado ({count} registros).")
                else:
                    # Escribir solo datos (saltar header)
                    f_out.writelines(lines[1:])
                    print(f"✓ Añadido ({count} registros).")
                
                rows_saved += count

        except Exception as e:
            print(f"✗ Error IO: {e}")

        # Pausa amable
        time.sleep(0.5)

    if temp_file.exists():
        temp_file.unlink()

    print()
    print(f"✓ Descarga consolidada en: {FINAL_OUTPUT}")
    print(f"📊 Total registros descargados: {rows_saved}")

    return rows_saved

def process_services_data():
    """
    FASE 2: Procesa all_bop_services.csv y genera archivos por país
    Retorna: número de países procesados exitosamente
    """
    print("\n" + "=" * 80)
    print("FASE 2: PROCESAMIENTO DATOS")
    print("=" * 80)

    if not FINAL_OUTPUT.exists():
        print(f"✗ Error: No se encuentra el archivo {FINAL_OUTPUT}")
        sys.exit(1)

    print(f"📂 Leyendo: {FINAL_OUTPUT} ...")

    try:
        df = pd.read_csv(FINAL_OUTPUT, low_memory=False, dtype=str)
    except Exception as e:
        print(f"✗ Error leyendo CSV: {e}")
        sys.exit(1)

    print(f"✓ {len(df):,} registros cargados.")

    # 1. Normalización y Limpieza
    df.columns = df.columns.str.strip()

    print("🧹 Limpiando códigos...")
    for col in ['geo', 'partner', 'stk_flow']:
        if col in df.columns:
            df[col] = df[col].str.strip()

    # 2. Aplicar Traducción (Eurostat -> ISO)
    df['geo'] = df['geo'].replace(EUROSTAT_TO_ISO)
    df['partner'] = df['partner'].replace(EUROSTAT_TO_ISO)

    # 3. Convertir valor a numérico
    df['OBS_VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')
    df = df.dropna(subset=['OBS_VALUE'])

    # 5. Procesamiento por país
    if 'TIME_PERIOD' not in df.columns:
        print("✗ Error: Columna TIME_PERIOD no encontrada.")
        sys.exit(1)

    success_count = 0

    for reporter_code in TARGET_REPORTERS:
        print(f"📊 Procesando {reporter_code}...", end=" ", flush=True)

        subset = df[df['geo'] == reporter_code].copy()

        if subset.empty:
            print(f"⚠️ Sin datos")
            continue

        try:
            # Interpolación Trimestral -> Mensual
            subset['date'] = pd.PeriodIndex(subset['TIME_PERIOD'], freq='Q').to_timestamp()

            df_pivot = subset.pivot_table(
                values='OBS_VALUE',
                index='date',
                columns=['partner', 'stk_flow'],
                aggfunc='sum'
            )

            if df_pivot.empty:
                print("⚠️ Vacío tras pivotar")
                continue

            # Rango mensual
            idx = pd.date_range(
                df_pivot.index.min(),
                df_pivot.index.max() + pd.offsets.QuarterEnd(0),
                freq='MS'
            )

            # Reindexar e interpolar (/ 3)
            df_monthly = df_pivot.reindex(idx).interpolate(method='linear') / 3

            # Aplanar
            df_flat = df_monthly.stack([0, 1]).reset_index()
            df_flat.columns = ['date', 'partner', 'stk_flow', 'value']

            # Formatos
            df_flat['TIME_PERIOD'] = df_flat['date'].dt.strftime('%Y-%m')
            df_flat['reporter'] = reporter_code

            # Guardar Archivos
            generated = False
            flow_map = {'DEB': 'imports', 'CRE': 'exports'}

            for flow_code, flow_name in flow_map.items():
                flow_data = df_flat[df_flat['stk_flow'] == flow_code].copy()

                if not flow_data.empty:
                    # Millones -> Unidades
                    flow_data['OBS_VALUE'] = flow_data['value'] * 1_000_000

                    final_df = flow_data[['reporter', 'partner', 'TIME_PERIOD', 'OBS_VALUE']]

                    outfile = OUTPUT_DIR / f'services_partners_{reporter_code}_{flow_name}.csv'
                    final_df.to_csv(outfile, index=False)
                    generated = True

            if generated:
                print("✓ OK")
                success_count += 1
            else:
                print("⚠️ Sin flujos")

        except Exception as e:
            print(f"✗ Error: {e}")

    print()
    print(f"📊 Países procesados: {success_count}/{len(TARGET_REPORTERS)}")
    return success_count

def main():
    print("=" * 80)
    print("ETL SERVICIOS COMPLETO - SOCIOS COMERCIALES")
    print("=" * 80)

    # Fase 1: Descarga
    rows_saved = download_services_data()

    if rows_saved == 0:
        print("✗ Error CRÍTICO: No se han descargado datos válidos.")
        sys.exit(1)

    # Fase 2: Procesamiento
    success_count = process_services_data()

    # Fase 3: Limpieza de archivo temporal
    if FINAL_OUTPUT.exists():
        file_size_mb = FINAL_OUTPUT.stat().st_size / (1024 * 1024)
        FINAL_OUTPUT.unlink()
        print(f"\n🗑️  Archivo temporal eliminado: {FINAL_OUTPUT} ({file_size_mb:.1f} MB)")

    print()
    print("=" * 80)
    print("✅ ETL COMPLETADO")
    print(f"✓ Datos procesados: {success_count} países")
    print(f"📁 Directorio: {CACHE_DIR.absolute()}")
    print("=" * 80)

if __name__ == "__main__":
    main()