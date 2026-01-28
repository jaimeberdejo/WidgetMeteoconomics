import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
from datetime import datetime

# Importar desde etl_loader_completo
try:
    from etl_loader_completo import update_data_if_needed, CSV_CACHE_FILE_GOODS, CSV_CACHE_FILE_SERVICES
except ImportError:
    CSV_CACHE_FILE_GOODS = 'data/goods/datos_mercancias_cache.csv'
    CSV_CACHE_FILE_SERVICES = 'data/services/datos_servicios_cache.csv'
    def update_data_if_needed():
        return False

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    page_title="Monitor Balanza Completa",
    page_icon="💶",
    layout="wide"
)

# --- MAPEO DE PAÍSES A CÓDIGOS ISO ---
CODIGO_PAIS = {
    'Austria': 'AT',
    'Bélgica': 'BE',
    'Bulgaria': 'BG',
    'Croacia': 'HR',
    'Chipre': 'CY',
    'República Checa': 'CZ',
    'Dinamarca': 'DK',
    'Estonia': 'EE',
    'Finlandia': 'FI',
    'Francia': 'FR',
    'Alemania': 'DE',
    'Grecia': 'GR',
    'Hungría': 'HU',
    'Irlanda': 'IE',
    'Italia': 'IT',
    'Letonia': 'LV',
    'Lituania': 'LT',
    'Luxemburgo': 'LU',
    'Malta': 'MT',
    'Países Bajos': 'NL',
    'Polonia': 'PL',
    'Portugal': 'PT',
    'Rumanía': 'RO',
    'Eslovaquia': 'SK',
    'Eslovenia': 'SI',
    'España': 'ES',
    'Suecia': 'SE',
    'Reino Unido': 'GB',
    'Noruega': 'NO',
    'Suiza': 'CH',
    'Islandia': 'IS',
}

# --- SECTORES SITC ---
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

# --- MAPEO DE CÓDIGOS DE PAÍSES A NOMBRES Y BANDERAS ---
PAISES_NOMBRE = {
    # UE-27
    'AT': 'Austria', 'BE': 'Bélgica', 'BG': 'Bulgaria', 'HR': 'Croacia',
    'CY': 'Chipre', 'CZ': 'República Checa', 'DK': 'Dinamarca', 'EE': 'Estonia',
    'FI': 'Finlandia', 'FR': 'Francia', 'DE': 'Alemania', 'GR': 'Grecia',
    'HU': 'Hungría', 'IE': 'Irlanda', 'IT': 'Italia', 'LV': 'Letonia',
    'LT': 'Lituania', 'LU': 'Luxemburgo', 'MT': 'Malta', 'NL': 'Países Bajos',
    'PL': 'Polonia', 'PT': 'Portugal', 'RO': 'Rumanía', 'SK': 'Eslovaquia',
    'SI': 'Eslovenia', 'ES': 'España', 'SE': 'Suecia',
    # Extra-UE
    'GB': 'Reino Unido', 'CH': 'Suiza', 'NO': 'Noruega', 'IS': 'Islandia',
    'CN': 'China', 'US': 'Estados Unidos', 'TR': 'Turquía', 'RU': 'Rusia',
    'JP': 'Japón', 'IN': 'India', 'KR': 'Corea del Sur', 'BR': 'Brasil',
    'MX': 'México', 'CA': 'Canadá', 'AU': 'Australia', 'SA': 'Arabia Saudita',
    'AE': 'EAU', 'ZA': 'Sudáfrica', 'SG': 'Singapur', 'TH': 'Tailandia',
    'MY': 'Malasia', 'ID': 'Indonesia', 'VN': 'Vietnam', 'PH': 'Filipinas',
    'AR': 'Argentina', 'CL': 'Chile', 'CO': 'Colombia', 'PE': 'Perú',
    'EG': 'Egipto', 'NG': 'Nigeria', 'KE': 'Kenia', 'MA': 'Marruecos',
    'DZ': 'Argelia', 'TN': 'Túnez', 'IL': 'Israel', 'IQ': 'Irak',
    'IR': 'Irán', 'PK': 'Pakistán', 'BD': 'Bangladesh', 'UA': 'Ucrania',
    'BY': 'Bielorrusia', 'KZ': 'Kazajistán', 'NZ': 'Nueva Zelanda'
}

BANDERAS = {
    # UE-27
    'AT': '🇦🇹', 'BE': '🇧🇪', 'BG': '🇧🇬', 'HR': '🇭🇷', 'CY': '🇨🇾',
    'CZ': '🇨🇿', 'DK': '🇩🇰', 'EE': '🇪🇪', 'FI': '🇫🇮', 'FR': '🇫🇷',
    'DE': '🇩🇪', 'GR': '🇬🇷', 'HU': '🇭🇺', 'IE': '🇮🇪', 'IT': '🇮🇹',
    'LV': '🇱🇻', 'LT': '🇱🇹', 'LU': '🇱🇺', 'MT': '🇲🇹', 'NL': '🇳🇱',
    'PL': '🇵🇱', 'PT': '🇵🇹', 'RO': '🇷🇴', 'SK': '🇸🇰', 'SI': '🇸🇮',
    'ES': '🇪🇸', 'SE': '🇸🇪',
    # Extra-UE
    'GB': '🇬🇧', 'CH': '🇨🇭', 'NO': '🇳🇴', 'IS': '🇮🇸', 'CN': '🇨🇳',
    'US': '🇺🇸', 'TR': '🇹🇷', 'RU': '🇷🇺', 'JP': '🇯🇵', 'IN': '🇮🇳',
    'KR': '🇰🇷', 'BR': '🇧🇷', 'MX': '🇲🇽', 'CA': '🇨🇦', 'AU': '🇦🇺',
    'SA': '🇸🇦', 'AE': '🇦🇪', 'ZA': '🇿🇦', 'SG': '🇸🇬', 'TH': '🇹🇭',
    'MY': '🇲🇾', 'ID': '🇮🇩', 'VN': '🇻🇳', 'PH': '🇵🇭', 'AR': '🇦🇷',
    'CL': '🇨🇱', 'CO': '🇨🇴', 'PE': '🇵🇪', 'EG': '🇪🇬', 'NG': '🇳🇬',
    'KE': '🇰🇪', 'MA': '🇲🇦', 'DZ': '🇩🇿', 'TN': '🇹🇳', 'IL': '🇮🇱',
    'IQ': '🇮🇶', 'IR': '🇮🇷', 'PK': '🇵🇰', 'BD': '🇧🇩', 'UA': '🇺🇦',
    'BY': '🇧🇾', 'KZ': '🇰🇿', 'NZ': '🇳🇿'
}

def format_partner_name(code):
    """Retorna nombre del país con bandera"""
    nombre = PAISES_NOMBRE.get(code, code)
    bandera = BANDERAS.get(code, '🏳️')
    return f"{bandera} {nombre}"

# --- FUNCIONES AUXILIARES ---
def format_currency(value):
    """Convierte números grandes a formato legible (Billones/Millones)"""
    if value >= 1_000_000_000:
        return f"€{value/1_000_000_000:.2f} B"
    elif value >= 1_000_000:
        return f"€{value/1_000_000:.1f} M"
    else:
        return f"€{value:,.0f}"

@st.cache_data(ttl=3600)
def load_goods_data():
    """Carga datos de mercancías (bienes)"""
    if not os.path.exists(CSV_CACHE_FILE_GOODS):
        st.error("⚠️ Ejecuta primero 'python etl_loader_completo.py' para generar los datos.")
        st.stop()

    df_raw = pd.read_csv(CSV_CACHE_FILE_GOODS)

    # Mapeo de países
    PAISES_NOMBRES = {
        'Austria': 'Austria',
        'Belgium (incl. Luxembourg \'LU\' -> 1998)': 'Bélgica',
        'Bulgaria': 'Bulgaria',
        'Croatia': 'Croacia',
        'Cyprus': 'Chipre',
        'Czechia': 'República Checa',
        'Denmark': 'Dinamarca',
        'Estonia': 'Estonia',
        'Finland': 'Finlandia',
        'France (incl. Saint Barthélemy \'BL\' -> 2012; incl. French Guiana \'GF\', Guadeloupe \'GP\', Martinique \'MQ\', Réunion \'RE\' from 1997; incl. Mayotte \'YT\' from 2014)': 'Francia',
        'Germany (incl. German Democratic Republic \'DD\' from 1991)': 'Alemania',
        'Greece': 'Grecia',
        'Hungary': 'Hungría',
        'Ireland (Eire)': 'Irlanda',
        'Italy (incl. San Marino \'SM\' -> 1993)': 'Italia',
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
        'Spain (incl. Canary Islands \'XB\' from 1997)': 'España',
        'Sweden': 'Suecia',
        'United Kingdom': 'Reino Unido',
        'Norway (incl. Svalbard and Jan Mayen \'SJ\' -> 1994 and again from 1997)': 'Noruega',
        'Switzerland (incl. Liechtenstein \'LI\' -> 1994)': 'Suiza',
        'European Union - 27 countries (AT, BE, BG, CY, CZ, DE, DK, EE, EL, ES, FI, FR, HR, HU, IE, IT, LT, LU, LV, MT, NL, PL, PT, RO, SE, SI, SK)': 'Unión Europea (27)',
    }

    # Mapeo de sectores
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

    df_raw['pais'] = df_raw['reporter'].map(PAISES_NOMBRES)
    df_raw['sector'] = df_raw['product'].map(SECTORES_NOMBRES)
    df_raw = df_raw.dropna(subset=['pais', 'sector'])

    df_raw = df_raw.rename(columns={
        'TIME_PERIOD': 'fecha',
        'OBS_VALUE': 'valor',
        'flow': 'flujo'
    })

    df_raw['fecha'] = pd.to_datetime(df_raw['fecha'])
    df_raw['valor'] = pd.to_numeric(df_raw['valor'].replace(':', '0'), errors='coerce').fillna(0)

    df_pivot = df_raw.pivot_table(
        index=['fecha', 'pais', 'sector'],
        columns='flujo',
        values='valor',
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    df_pivot.columns.name = None
    column_mapping = {}
    for col in df_pivot.columns:
        if 'EXPORT' in str(col).upper():
            column_mapping[col] = 'exportaciones'
        elif 'IMPORT' in str(col).upper():
            column_mapping[col] = 'importaciones'
    df_pivot = df_pivot.rename(columns=column_mapping)

    if 'exportaciones' not in df_pivot.columns:
        df_pivot['exportaciones'] = 0
    if 'importaciones' not in df_pivot.columns:
        df_pivot['importaciones'] = 0

    df_pivot['balance'] = df_pivot['exportaciones'] - df_pivot['importaciones']
    df_pivot['tipo'] = 'Bienes'

    return df_pivot


@st.cache_data(ttl=3600)
def load_services_data():
    """Carga datos de servicios (incluye turismo)"""
    if not os.path.exists(CSV_CACHE_FILE_SERVICES):
        return pd.DataFrame()  # Retornar vacío si no existe

    try:
        df_raw = pd.read_csv(CSV_CACHE_FILE_SERVICES)

        # Mapeo de códigos de países BOP a español
        PAISES_BOP = {
            'Austria': 'Austria',
            'Belgium': 'Bélgica',
            'Bulgaria': 'Bulgaria',
            'Croatia': 'Croacia',
            'Cyprus': 'Chipre',
            'Czechia': 'República Checa',
            'Denmark': 'Dinamarca',
            'Estonia': 'Estonia',
            'Finland': 'Finlandia',
            'France': 'Francia',
            'Germany': 'Alemania',
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
            'United Kingdom': 'Reino Unido',
        }

        # Filtrar y mapear países
        df_raw['pais'] = df_raw['geo'].map(PAISES_BOP)
        df_raw = df_raw.dropna(subset=['pais'])

        # Renombrar columnas
        df_raw = df_raw.rename(columns={
            'TIME_PERIOD': 'fecha',
            'OBS_VALUE': 'valor'
        })

        # Convertir fecha a datetime (formato YYYY-MM después de interpolación)
        df_raw['fecha'] = pd.to_datetime(df_raw['fecha'], format='%Y-%m', errors='coerce')

        # Convertir valor a numérico
        # IMPORTANTE: Los datos BOP vienen en MILLONES de EUR
        # Los datos de mercancías están en EUR
        # Necesitamos convertir millones → EUR para compatibilidad
        df_raw['valor'] = pd.to_numeric(df_raw['valor'], errors='coerce')

        # Filtrar solo filas donde valor es NaN (no convertible) - mantener ceros
        df_raw = df_raw[df_raw['valor'].notna()]

        # Rellenar NaN restantes con 0
        df_raw['valor'] = df_raw['valor'].fillna(0)

        # Convertir de millones EUR a EUR (multiplicar por 1,000,000)
        df_raw['valor'] = df_raw['valor'] * 1_000_000

        if df_raw.empty:
            return pd.DataFrame()

        # Los datos BOP vienen con stk_flow = CRE (créditos/exportaciones) y DEB (débitos/importaciones)
        # Ya no estimamos, sino que usamos los valores reales

        # Pivotar para tener CRE y DEB como columnas separadas
        df_pivot = df_raw.pivot_table(
            index=['fecha', 'pais', 'bop_item'],
            columns='stk_flow',
            values='valor',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        df_pivot.columns.name = None

        # Renombrar columnas CRE -> exportaciones, DEB -> importaciones
        column_mapping = {}
        for col in df_pivot.columns:
            if col == 'Credit':
                column_mapping[col] = 'exportaciones'
            elif col == 'Debit':
                column_mapping[col] = 'importaciones'

        df_pivot = df_pivot.rename(columns=column_mapping)

        # Si no hay las columnas esperadas, intentar nombres alternativos
        if 'exportaciones' not in df_pivot.columns:
            # Buscar cualquier columna que contenga 'cre' o 'credit'
            for col in df_pivot.columns:
                if 'cre' in str(col).lower() or 'credit' in str(col).lower():
                    df_pivot = df_pivot.rename(columns={col: 'exportaciones'})
                    break

        if 'importaciones' not in df_pivot.columns:
            # Buscar cualquier columna que contenga 'deb' or 'debit'
            for col in df_pivot.columns:
                if 'deb' in str(col).lower() or 'debit' in str(col).lower():
                    df_pivot = df_pivot.rename(columns={col: 'importaciones'})
                    break

        # Asegurar que existen las columnas
        if 'exportaciones' not in df_pivot.columns:
            df_pivot['exportaciones'] = 0
        if 'importaciones' not in df_pivot.columns:
            df_pivot['importaciones'] = 0

        # Calcular balance real: exportaciones - importaciones
        df_pivot['balance'] = df_pivot['exportaciones'] - df_pivot['importaciones']

        df_raw = df_pivot

        # Añadir sector según bop_item
        df_raw['sector'] = df_raw['bop_item'].map({
            'Services': 'Servicios',
            'Services: transport': 'Servicios de Transporte',
            'S': 'Servicios',
            'SC': 'Servicios (comerciales)',
            'CA': 'Cuenta Corriente'
        }).fillna('Servicios')

        df_raw['tipo'] = 'Servicios'

        # Seleccionar columnas necesarias
        df_result = df_raw[['fecha', 'pais', 'sector', 'exportaciones', 'importaciones', 'balance', 'tipo']]

        return df_result

    except Exception as e:
        # Mostrar error solo si no es un DataFrame vacío esperado
        st.warning(f"⚠️ No se pudieron cargar datos de servicios: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_partners_data(country_code, data_type='goods'):
    """
    Carga datos de socios comerciales para un país específico.

    Args:
        country_code: Código ISO del país (e.g., 'ES', 'FR', 'DE')
        data_type: 'goods' (bienes) o 'services' (servicios)

    Returns:
        dict: Diccionario con DataFrames de imports, exports y combined
              None si no existen los datos
    """
    from pathlib import Path

    if data_type == 'goods':
        cache_dir = Path('data/partners')
        prefix = 'partners'
    else:  # services
        cache_dir = Path('data/partners_services')
        prefix = 'services_partners'

    imports_file = cache_dir / f'{prefix}_{country_code}_imports.csv'
    exports_file = cache_dir / f'{prefix}_{country_code}_exports.csv'

    if not imports_file.exists() or not exports_file.exists():
        return None

    try:
        df_imports = pd.read_csv(imports_file)
        df_exports = pd.read_csv(exports_file)

        # Añadir columna de flujo
        df_imports['flow_type'] = 'Importaciones'
        df_exports['flow_type'] = 'Exportaciones'

        # Convertir TIME_PERIOD a datetime
        df_imports['fecha'] = pd.to_datetime(df_imports['TIME_PERIOD'])
        df_exports['fecha'] = pd.to_datetime(df_exports['TIME_PERIOD'])

        # Para bienes: convertir product a string
        # Para servicios: no hay columna product (solo TOTAL)
        if data_type == 'goods' and 'product' in df_imports.columns:
            df_imports['product'] = df_imports['product'].astype(str)
            df_exports['product'] = df_exports['product'].astype(str)
        elif data_type == 'services':
            # Añadir columna product='TOTAL' para compatibilidad
            df_imports['product'] = 'TOTAL'
            df_exports['product'] = 'TOTAL'

        return {
            'imports': df_imports,
            'exports': df_exports,
            'combined': pd.concat([df_imports, df_exports], ignore_index=True)
        }
    except Exception as e:
        st.warning(f"Error cargando datos de socios ({data_type}) para {country_code}: {e}")
        return None


# --- CARGA DE DATOS ---
try:
    df_goods = load_goods_data()
    df_services = load_services_data()
except Exception as e:
    st.error(f"Error cargando datos: {e}")
    st.stop()

# --- SIDEBAR (CONFIGURACIÓN) ---
st.sidebar.title("Configuración")

# Preparar ambos datasets (siempre)
df_full_goods = df_goods
df_full_services = df_services if not df_services.empty else None

# 1. Selector de País (usa bienes como base)
paises = sorted(df_full_goods['pais'].unique())

# Mantener selección de país al cambiar entre modos
if 'pais_seleccionado' not in st.session_state:
    st.session_state.pais_seleccionado = 'España' if 'España' in paises else paises[0]

# Verificar que el país seleccionado existe en los datos actuales
if st.session_state.pais_seleccionado not in paises:
    st.session_state.pais_seleccionado = paises[0]

st.sidebar.caption(f"📊 {len(paises)} países disponibles")
idx_def = paises.index(st.session_state.pais_seleccionado)
pais_sel = st.sidebar.selectbox("País", paises, index=idx_def, key='selector_pais')

# Actualizar session state cuando el usuario cambia el país
if pais_sel != st.session_state.pais_seleccionado:
    st.session_state.pais_seleccionado = pais_sel

# 2. Selector de Rango temporal
# Usar bienes para determinar fechas disponibles (dataset más completo)
df_temp = df_full_goods[df_full_goods['pais'] == pais_sel].sort_values('fecha')
fechas_disponibles = sorted(df_temp['fecha'].unique())
min_date = fechas_disponibles[0].date()
max_date = fechas_disponibles[-1].date()

st.sidebar.subheader("Periodo de Análisis")

col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.date_input(
        "Desde:",
        value=min_date,
        min_value=min_date,
        max_value=max_date,
        format="DD/MM/YYYY"
    )
with col2:
    end_date = st.date_input(
        "Hasta:",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
        format="DD/MM/YYYY"
    )

# Convertir date a datetime para filtrado
start_datetime = pd.to_datetime(start_date)
end_datetime = pd.to_datetime(end_date)

# --- DASHBOARD ---
st.title(f"🌍 Balanza Comercial: {pais_sel}")
date_str_start = start_datetime.strftime('%B %Y')
date_str_end = end_datetime.strftime('%B %Y')
st.markdown(f"**Periodo analizado:** {date_str_start} a {date_str_end}")
st.markdown("---")

# --- TABS: Balance por País y Socios Comerciales ---
tab1, tab2 = st.tabs(["📊 Balance por País", "🌍 Socios Comerciales"])

with tab1:
    st.header("📊 Balance por País")

    # --- SELECTOR DE TIPO DE BALANZA (MOVIDO DESDE SIDEBAR) ---
    modo = st.radio(
        "Tipo de Balanza",
        options=["Solo Bienes", "Bienes + Servicios"],
        horizontal=True,
        index=0,
        help="Selecciona si quieres ver solo mercancías o incluir servicios (turismo, transporte, etc.)"
    )

    # Determinar qué datos usar según el modo seleccionado
    if modo == "Solo Bienes":
        df_full = df_full_goods
        modo_activo = "Solo Bienes"
    elif df_full_services is None:
        # Usuario seleccionó "Bienes + Servicios" pero no hay datos de servicios
        df_full = df_full_goods
        modo_activo = "Solo Bienes"
        st.warning("⚠️ **Datos de servicios no disponibles** - Mostrando solo mercancías")
    else:
        # Combinar bienes y servicios
        df_full = pd.concat([df_full_goods, df_full_services], ignore_index=True)
        modo_activo = "Bienes + Servicios"

        # --- PROTECCIÓN CONTRA EFECTO ACANTILADO ---
        max_fecha_bienes = df_full_goods['fecha'].max()
        max_fecha_servicios = df_full_services['fecha'].max()

        # Almacenar fecha de corte
        st.session_state.fecha_corte_servicios = min(max_fecha_bienes, max_fecha_servicios)

        # Advertir si hay desincronización
        if max_fecha_bienes > max_fecha_servicios:
            meses_diferencia = (max_fecha_bienes.year - max_fecha_servicios.year) * 12 + (max_fecha_bienes.month - max_fecha_servicios.month)
            st.info(f"ℹ️ Datos de servicios disponibles hasta {max_fecha_servicios.strftime('%B %Y')} ({meses_diferencia} meses de retraso)")

        st.success(f"✅ Mostrando {len(df_full_goods):,} registros de bienes + {len(df_full_services):,} de servicios")

    st.divider()

    # Filtrar datos por país
    df_pais = df_full[df_full['pais'] == pais_sel].sort_values('fecha')

    # --- APLICAR CORTE DE FECHA EN MODO BIENES + SERVICIOS ---
    if modo_activo == "Bienes + Servicios" and 'fecha_corte_servicios' in st.session_state:
        # Limitar a la fecha máxima donde ambos datasets tienen datos
        df_pais = df_pais[df_pais['fecha'] <= st.session_state.fecha_corte_servicios]

    # Advertencia si el país no tiene datos de servicios en modo "Bienes + Servicios"
    if modo_activo == "Bienes + Servicios" and df_full_services is not None:
        pais_tiene_servicios = pais_sel in df_full_services['pais'].unique()
        if not pais_tiene_servicios:
            st.warning(f"⚠️ {pais_sel} no tiene datos de servicios BOP disponibles. Mostrando solo mercancías.")

    # FILTRADO DINÁMICO por periodo
    df_filtrado = df_pais[
        (df_pais['fecha'] >= start_datetime) &
        (df_pais['fecha'] <= end_datetime)
    ]

    # --- 1. KPIs ---
    df_total_comercio = df_filtrado[df_filtrado['sector'] == 'Total Comercio']

    if df_total_comercio.empty:
        df_agrupado = df_filtrado.groupby('fecha')[['exportaciones', 'importaciones', 'balance']].sum().reset_index()
    else:
        df_agrupado = df_total_comercio.groupby('fecha')[['exportaciones', 'importaciones', 'balance']].sum().reset_index()

    tot_exp = df_agrupado['exportaciones'].sum()
    tot_imp = df_agrupado['importaciones'].sum()
    tot_bal = df_agrupado['balance'].sum()
    cobertura = (tot_exp / tot_imp * 100) if tot_imp > 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Exportaciones (Total Periodo)", format_currency(tot_exp), border=True)
    c2.metric("Importaciones (Total Periodo)", format_currency(tot_imp), border=True)
    c3.metric("Balanza Comercial", format_currency(tot_bal),
          delta=format_currency(tot_bal), delta_color="normal", border=True)
    c4.metric("Tasa Cobertura", f"{cobertura:.1f}%", help=">100% indica superávit", border=True)

    # --- 2. GRÁFICO DE LÍNEAS (Evolución) ---
    st.subheader("📈 Evolución Mensual")

    fig_line = go.Figure()

    # Exportaciones e Importaciones en el eje primario
    fig_line.add_trace(go.Scatter(
        x=df_agrupado['fecha'],
        y=df_agrupado['exportaciones'],
        name='Exportaciones',
        line=dict(color='#00CC96', width=2),
        yaxis='y'
    ))
    fig_line.add_trace(go.Scatter(
        x=df_agrupado['fecha'],
        y=df_agrupado['importaciones'],
        name='Importaciones',
        line=dict(color='#EF553B', width=2),
        yaxis='y'
    ))

    # Balance en el eje secundario (barras)
    fig_line.add_trace(go.Bar(
        x=df_agrupado['fecha'],
        y=df_agrupado['balance'],
        name='Balance Comercial',
        marker_color=['#00CC96' if val >= 0 else '#EF553B' for val in df_agrupado['balance']],
        opacity=0.4,
        yaxis='y2'
    ))

    fig_line.update_layout(
        height=400,
        hovermode="x unified",
        legend=dict(orientation="h", y=1.12),
        yaxis=dict(
            title="Comercio Total (€)",
            side='left'
        ),
        yaxis2=dict(
            title="Balance Comercial (€)",
            side='right',
            overlaying='y',
            showgrid=False
        ),
        margin=dict(t=50)
    )
    st.plotly_chart(fig_line, use_container_width=True)

    # --- 3. ANÁLISIS POR SECTOR (Dinámico) ---
    st.subheader("🔍 Desglose por Sectores (Acumulado)")
    st.caption(f"Suma total de exportaciones e importaciones desde {date_str_start} hasta {date_str_end}")

    # Filtramos para quitar el total y quedarnos solo con sectores
    df_sectores = df_filtrado[df_filtrado['sector'] != 'Total Comercio']

    # AGRUPACIÓN DINÁMICA
    df_sectores_agrupado = df_sectores.groupby('sector')[['exportaciones', 'importaciones']].sum().reset_index()

    # Calculamos volumen total para ordenar
    df_sectores_agrupado['Volumen Total'] = df_sectores_agrupado['exportaciones'] + df_sectores_agrupado['importaciones']
    df_sectores_agrupado = df_sectores_agrupado.sort_values('Volumen Total', ascending=True)

    # Convertimos a formato largo
    import plotly.express as px
    df_melted_sec = df_sectores_agrupado.melt(
        id_vars='sector',
        value_vars=['exportaciones', 'importaciones'],
        var_name='Flujo',
        value_name='Valor'
    )

    fig_bar = px.bar(
        df_melted_sec,
        y='sector',
        x='Valor',
        color='Flujo',
        orientation='h',
        barmode='group',
        color_discrete_map={'exportaciones': '#00CC96', 'importaciones': '#EF553B'},
        text_auto='.2s'
    )

    fig_bar.update_layout(
        height=600,
        xaxis_title="Valor Acumulado (€)",
        yaxis_title="",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(t=80)
    )

    st.plotly_chart(fig_bar, use_container_width=True)


with tab2:
    # --- NUEVA FUNCIONALIDAD: SOCIOS COMERCIALES ---
    st.header(f"🌍 Análisis de Socios Comerciales: {pais_sel}")

    # Obtener código ISO del país
    country_code = CODIGO_PAIS.get(pais_sel)

    if country_code is None:
        st.warning(f"⚠️ No se encontró el código ISO para {pais_sel}")
        st.stop()

    # NUEVO: Selector de tipo de datos (Bienes/Servicios/Ambos)
    st.markdown("---")
    data_type_option = st.radio(
        "Tipo de Comercio",
        ["Bienes", "Servicios", "Bienes + Servicios"],
        horizontal=True,
        help="Bienes: Mercancías físicas (40 socios, 10 sectores) | Servicios: Turismo, transporte, etc. (32 socios, solo total)"
    )

    # Cargar datos según selección
    if data_type_option == "Bienes":
        partners_data = load_partners_data(country_code, 'goods')
        if partners_data is None:
            st.warning("⚠️ Datos de socios de bienes no disponibles.")
            st.info("Ejecuta: `python etl_partners.py`")
            st.stop()
        show_sectors = True
        data_label = "Bienes"

    elif data_type_option == "Servicios":
        partners_data = load_partners_data(country_code, 'services')
        if partners_data is None:
            st.warning("⚠️ Datos de socios de servicios no disponibles.")
            st.info("Ejecuta: `python etl_partners_services.py`")
            st.stop()
        show_sectors = False  # Servicios solo tiene TOTAL
        data_label = "Servicios"

    else:  # Bienes + Servicios
        partners_goods = load_partners_data(country_code, 'goods')
        partners_services = load_partners_data(country_code, 'services')

        if partners_goods is None and partners_services is None:
            st.warning("⚠️ No hay datos de socios disponibles.")
            st.info("""
            Ejecuta:
            - `python etl_partners.py` (bienes)
            - `python etl_partners_services.py` (servicios)
            """)
            st.stop()

        # Combinar datos
        if partners_goods is not None and partners_services is not None:
            # Combinar imports
            df_imp_goods = partners_goods['imports'].copy()
            df_imp_services = partners_services['imports'].copy()
            df_imp_goods['tipo'] = 'Bienes'
            df_imp_services['tipo'] = 'Servicios'

            # Combinar exports
            df_exp_goods = partners_goods['exports'].copy()
            df_exp_services = partners_services['exports'].copy()
            df_exp_goods['tipo'] = 'Bienes'
            df_exp_services['tipo'] = 'Servicios'

            partners_data = {
                'imports': pd.concat([df_imp_goods, df_imp_services], ignore_index=True),
                'exports': pd.concat([df_exp_goods, df_exp_services], ignore_index=True),
                'combined': pd.concat([
                    df_imp_goods, df_imp_services,
                    df_exp_goods, df_exp_services
                ], ignore_index=True)
            }
        elif partners_goods is not None:
            partners_data = partners_goods
            st.info("⚠️ Solo datos de bienes disponibles (falta servicios)")
        else:
            partners_data = partners_services
            st.info("⚠️ Solo datos de servicios disponibles (falta bienes)")

        show_sectors = False  # En modo combinado, no mostrar sectores
        data_label = "Bienes + Servicios"

    # Selector de flujo y sector
    if show_sectors:
        col1, col2, col3 = st.columns(3)
    else:
        col1, col2 = st.columns(2)

    with col1:
        flow_option = st.radio(
            "Flujo",
            ["Importaciones", "Exportaciones", "Ambos"],
            horizontal=True
        )

    if show_sectors:
        with col2:
            # Añadir opción TOTAL calculando la suma
            sector_options = ['TOTAL'] + list(SECTORES_SITC.keys())[1:]  # TOTAL + 0-9
            sector_sel = st.selectbox(
                "Sector",
                options=sector_options,
                format_func=lambda x: SECTORES_SITC.get(x, x),
                index=0  # TOTAL por defecto
            )
        with col3:
            top_n = st.selectbox("Top N socios", [5, 10, 15, 20, 40], index=1)
    else:
        sector_sel = 'TOTAL'  # Servicios solo tiene TOTAL
        with col2:
            # Ajustar máximo para servicios (32 socios)
            max_socios = 40 if data_type_option == "Bienes" else 32
            top_n_options = [5, 10, 15, 20]
            if max_socios >= 32:
                top_n_options.append(32)
            if max_socios >= 40:
                top_n_options.append(40)
            top_n = st.selectbox("Top N socios", top_n_options, index=1)

    # Filtrar por flujo
    if flow_option == "Importaciones":
        df_display = partners_data['imports'].copy()
    elif flow_option == "Exportaciones":
        df_display = partners_data['exports'].copy()
    else:
        df_display = partners_data['combined'].copy()

    # Filtrar por sector o calcular TOTAL
    if sector_sel == 'TOTAL':
        # Sumar todos los sectores (0-9)
        df_display = df_display.groupby(['partner', 'fecha', 'TIME_PERIOD', 'flow_type'])['OBS_VALUE'].sum().reset_index()
    else:
        df_display = df_display[df_display['product'] == sector_sel]

    # Filtrar por fechas (reutilizar start_date, end_date del sidebar)
    df_display = df_display[
        (df_display['fecha'] >= start_datetime) &
        (df_display['fecha'] <= end_datetime)
    ]

    if df_display.empty:
        st.warning("⚠️ No hay datos disponibles para el período y sector seleccionado")
        st.stop()

    # --- KPIs DE SOCIOS COMERCIALES ---
    st.markdown("---")

    # Definir sector_label para usar en títulos y KPIs
    sector_label = SECTORES_SITC.get(sector_sel, sector_sel) if show_sectors else "Total Comercio"

    # Calcular totales y top socio
    if flow_option == "Ambos":
        # Usar datos completos del periodo con filtros de sector
        df_imp_kpi = partners_data['imports'].copy()
        df_exp_kpi = partners_data['exports'].copy()

        # Filtrar por sector y fechas
        if sector_sel == 'TOTAL':
            df_imp_kpi = df_imp_kpi[(df_imp_kpi['fecha'] >= start_datetime) & (df_imp_kpi['fecha'] <= end_datetime)]
            df_exp_kpi = df_exp_kpi[(df_exp_kpi['fecha'] >= start_datetime) & (df_exp_kpi['fecha'] <= end_datetime)]
            imp_total = df_imp_kpi.groupby('partner')['OBS_VALUE'].sum()
            exp_total = df_exp_kpi.groupby('partner')['OBS_VALUE'].sum()
        else:
            df_imp_kpi = df_imp_kpi[(df_imp_kpi['product'] == sector_sel) &
                                     (df_imp_kpi['fecha'] >= start_datetime) &
                                     (df_imp_kpi['fecha'] <= end_datetime)]
            df_exp_kpi = df_exp_kpi[(df_exp_kpi['product'] == sector_sel) &
                                     (df_exp_kpi['fecha'] >= start_datetime) &
                                     (df_exp_kpi['fecha'] <= end_datetime)]
            imp_total = df_imp_kpi.groupby('partner')['OBS_VALUE'].sum()
            exp_total = df_exp_kpi.groupby('partner')['OBS_VALUE'].sum()

        total_imp = imp_total.sum()
        total_exp = exp_total.sum()
        balance_total = total_exp - total_imp

        # Top socio de importaciones y exportaciones
        top_imp_code = imp_total.idxmax() if not imp_total.empty else 'N/A'
        top_exp_code = exp_total.idxmax() if not exp_total.empty else 'N/A'

        top_imp_valor = imp_total.max() if not imp_total.empty else 0
        top_exp_valor = exp_total.max() if not exp_total.empty else 0

        # Calcular % del total
        pct_imp = (top_imp_valor / total_imp * 100) if total_imp > 0 else 0
        pct_exp = (top_exp_valor / total_exp * 100) if total_exp > 0 else 0

        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric(
            f"Total Importaciones ({data_label})",
            format_currency(total_imp),
            help=f"Desde {len(imp_total)} socios comerciales"
        )
        kpi2.metric(
            "Principal Proveedor",
            format_partner_name(top_imp_code),
            delta=f"{pct_imp:.1f}% del total",
            help=f"Importaciones: {format_currency(top_imp_valor)}"
        )
        kpi3.metric(
            f"Total Exportaciones ({data_label})",
            format_currency(total_exp),
            help=f"Hacia {len(exp_total)} socios comerciales"
        )
        kpi4.metric(
            "Principal Cliente",
            format_partner_name(top_exp_code),
            delta=f"{pct_exp:.1f}% del total",
            help=f"Exportaciones: {format_currency(top_exp_valor)}"
        )

    else:
        # Modo simple (solo imports o exports)
        totales_kpi = df_display.groupby('partner')['OBS_VALUE'].sum()
        total_valor = totales_kpi.sum()
        top_socio_code = totales_kpi.idxmax() if not totales_kpi.empty else 'N/A'
        top_socio_valor = totales_kpi.max() if not totales_kpi.empty else 0
        pct_top = (top_socio_valor / total_valor * 100) if total_valor > 0 else 0

        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric(
            f"Total {flow_option}",
            format_currency(total_valor),
            help=f"Desde/hacia {len(totales_kpi)} socios comerciales"
        )
        kpi2.metric(
            f"Principal {'Proveedor' if flow_option == 'Importaciones' else 'Cliente'}",
            format_partner_name(top_socio_code),
            delta=f"{pct_top:.1f}% del total",
            help=f"Valor: {format_currency(top_socio_valor)}"
        )
        kpi3.metric(
            "Socios Activos",
            f"{len(totales_kpi)} países",
            help=f"Con comercio de {data_label.lower()} en {sector_label}"
        )

    st.markdown("---")

    # --- GRÁFICO 1: Top N socios (barras) ---
    st.subheader(f"📊 Top {top_n} Socios - {data_label}: {sector_label}")

    if flow_option == "Ambos":
        # Barras lado a lado (imports vs exports)
        df_imports_total = partners_data['imports'].copy()
        df_exports_total = partners_data['exports'].copy()

        # Filtrar por fechas
        df_imports_total = df_imports_total[(df_imports_total['fecha'] >= start_datetime) &
                                             (df_imports_total['fecha'] <= end_datetime)]
        df_exports_total = df_exports_total[(df_exports_total['fecha'] >= start_datetime) &
                                             (df_exports_total['fecha'] <= end_datetime)]

        # Filtrar por sector
        if sector_sel == 'TOTAL':
            df_imports_total = df_imports_total.groupby('partner')['OBS_VALUE'].sum()
            df_exports_total = df_exports_total.groupby('partner')['OBS_VALUE'].sum()
        else:
            df_imports_total = df_imports_total[df_imports_total['product'] == sector_sel].groupby('partner')['OBS_VALUE'].sum()
            df_exports_total = df_exports_total[df_exports_total['product'] == sector_sel].groupby('partner')['OBS_VALUE'].sum()

        # Combinar y ordenar por suma total
        df_combined_total = pd.DataFrame({
            'imports': df_imports_total,
            'exports': df_exports_total
        }).fillna(0)
        df_combined_total['total'] = df_combined_total['imports'] + df_combined_total['exports']
        df_combined_total['balance'] = df_combined_total['exports'] - df_combined_total['imports']
        df_combined_total = df_combined_total.sort_values('total', ascending=False).head(top_n)

        # Añadir nombres con banderas
        partner_labels = [format_partner_name(code) for code in df_combined_total.index]

        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(
            y=partner_labels,
            x=df_combined_total['imports'] / 1e9,
            name='Importaciones',
            orientation='h',
            marker_color='#EF553B',
            text=[f'€{v/1e9:.1f}B' for v in df_combined_total['imports']],
            textposition='inside',
            textfont=dict(color='white')
        ))
        fig_bar.add_trace(go.Bar(
            y=partner_labels,
            x=df_combined_total['exports'] / 1e9,
            name='Exportaciones',
            orientation='h',
            marker_color='#00CC96',
            text=[f'€{v/1e9:.1f}B' for v in df_combined_total['exports']],
            textposition='inside',
            textfont=dict(color='white')
        ))

        fig_bar.update_layout(
            barmode='group',
            height=max(450, top_n * 35),
            xaxis_title="Valor (€ Billones)",
            yaxis={'categoryorder': 'total ascending'},
            legend=dict(orientation="h", y=1.08, x=0.5, xanchor='center'),
            margin=dict(l=150, r=50, t=50, b=50)
        )

    else:
        # Barras simples
        totales = df_display.groupby('partner')['OBS_VALUE'].sum().sort_values(ascending=False).head(top_n)

        # Añadir nombres con banderas
        partner_labels = [format_partner_name(code) for code in totales.index]

        fig_bar = go.Figure(go.Bar(
            y=partner_labels,
            x=totales.values / 1e9,
            orientation='h',
            marker_color='#EF553B' if flow_option == 'Importaciones' else '#00CC96',
            text=[f'€{v/1e9:.1f}B' for v in totales.values],
            textposition='outside'
        ))

        fig_bar.update_layout(
            height=max(450, top_n * 35),
            xaxis_title="Valor (€ Billones)",
            yaxis={'categoryorder': 'total ascending'},
            showlegend=False,
            margin=dict(l=150, r=50, t=50, b=50)
        )

    st.plotly_chart(fig_bar, use_container_width=True)

    # --- GRÁFICO 2: Balance Comercial (solo en modo "Ambos") ---
    if flow_option == "Ambos":
        st.subheader("💰 Balance Comercial por Socio (Top 10)")

        # Usar datos ya calculados
        df_balance = df_combined_total.head(10).copy()

        # Añadir nombres con banderas
        balance_labels = [format_partner_name(code) for code in df_balance.index]

        fig_balance = go.Figure()

        # Colores según superávit/déficit
        colors = ['#00CC96' if bal >= 0 else '#EF553B' for bal in df_balance['balance']]

        fig_balance.add_trace(go.Bar(
            y=balance_labels,
            x=df_balance['balance'] / 1e9,
            orientation='h',
            marker_color=colors,
            text=[f'€{v/1e9:.1f}B' for v in df_balance['balance']],
            textposition='outside'
        ))

        fig_balance.update_layout(
            height=400,
            xaxis_title="Balance (€ Billones)",
            yaxis={'categoryorder': 'total ascending'},
            showlegend=False,
            margin=dict(l=150, r=50, t=30, b=50)
        )

        # Añadir línea vertical en 0
        fig_balance.add_vline(x=0, line_width=2, line_dash="dash", line_color="gray")

        st.plotly_chart(fig_balance, use_container_width=True)
        st.caption("💡 Verde = Superávit (exportamos más de lo que importamos) | Rojo = Déficit (importamos más de lo que exportamos)")

    # --- GRÁFICO 3: Evolución temporal (Top 5) ---
    st.subheader("📈 Evolución Temporal (Top 5 Socios)")

    # Obtener top 5 socios
    top5_partners = df_display.groupby('partner')['OBS_VALUE'].sum().nlargest(5).index

    fig_line = go.Figure()

    for partner in top5_partners:
        df_partner = df_display[df_display['partner'] == partner]
        df_partner_monthly = df_partner.groupby('fecha')['OBS_VALUE'].sum().reset_index()

        fig_line.add_trace(go.Scatter(
            x=df_partner_monthly['fecha'],
            y=df_partner_monthly['OBS_VALUE'] / 1e9,
            name=format_partner_name(partner),
            mode='lines+markers',
            line=dict(width=2)
        ))

    fig_line.update_layout(
        height=450,
        xaxis_title="Fecha",
        yaxis_title="Valor (€ Billones)",
        hovermode='x unified',
        legend=dict(orientation="v", y=1, x=1.02),
        margin=dict(r=150)
    )

    st.plotly_chart(fig_line, use_container_width=True)

    # --- TABLA DETALLADA ---
    st.subheader("📋 Datos Detallados por Mes")

    # Pivot table: socios × meses
    df_pivot = df_display.pivot_table(
        values='OBS_VALUE',
        index='partner',
        columns='TIME_PERIOD',
        aggfunc='sum',
        fill_value=0
    )

    # Añadir columna total
    df_pivot['TOTAL'] = df_pivot.sum(axis=1)
    df_pivot = df_pivot.sort_values('TOTAL', ascending=False)

    # Formatear valores (millones EUR) y añadir nombres con banderas
    df_pivot_display = df_pivot / 1e6
    df_pivot_display.index = [format_partner_name(code) for code in df_pivot_display.index]

    # Formatear valores con estilo
    st.dataframe(
        df_pivot_display.style.format("{:.1f}"),
        use_container_width=True,
        height=400
    )

    st.caption("💡 Valores en millones de euros (M€)")

    # Botón descarga
    csv = df_pivot.to_csv().encode('utf-8')
    st.download_button(
        label="📥 Descargar CSV",
        data=csv,
        file_name=f"socios_{pais_sel}_{flow_option}_{sector_sel}_{start_date}_{end_date}.csv",
        mime="text/csv"
    )

    st.caption(f"📊 Mostrando datos de **{data_label}**: {len(df_display['partner'].unique())} socios comerciales en **{sector_label}** desde {date_str_start} hasta {date_str_end}")


# Footer
st.markdown("---")
st.caption(f"Fuente: Eurostat DS-059331 (Bienes) + BOP_C6_M (Servicios) | Última actualización: {datetime.fromtimestamp(os.path.getmtime(CSV_CACHE_FILE_GOODS)).strftime('%Y-%m-%d %H:%M')}")
st.caption("💶 Datos reales desde la API oficial de Eurostat")
