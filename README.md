# 🌍 Widget Balanza Comercial

[![Streamlit](https://img.shields.io/badge/Streamlit-1.28+-FF4B4B?logo=streamlit)](https://streamlit.io)
[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python)](https://www.python.org)
[![Eurostat](https://img.shields.io/badge/Data-Eurostat-003399)](https://ec.europa.eu/eurostat)

Dashboard interactivo para análisis de balanza comercial de países europeos con datos de mercancías, servicios y socios comerciales bilaterales.

## 📑 Contenido
- [Características](#-características)
- [Instalación](#-instalación-y-uso)
- [Estructura de Datos](#-estructura-de-datos)
- [Sistema de Actualización](#-sistema-de-actualización)
- [Troubleshooting](#-troubleshooting)

## 📊 Características

### 📈 Tab 1: Balance por País
- **Selector de Tipo**: Bienes | Bienes + Servicios
- **KPIs principales**: Exportaciones, importaciones, balance y tasa de cobertura
- **Evolución temporal**: Gráficos de tendencias mensuales
- **Desglose sectorial**: Análisis por 10 sectores SITC

### 🌍 Tab 2: Socios Comerciales
- **40 socios comerciales**: 20 UE-27 + 20 extra-UE (>98% coverage)
- **Visualizaciones**:
  - Ranking top N socios (barras horizontales)
  - Evolución temporal top 5 (líneas)
  - Balance bilateral (surplus/déficit)
  - Tabla pivote descargable CSV

## 🚀 Instalación y Uso

### Requisitos
```bash
pip install streamlit pandas plotly requests
```

### 1. Descargar Datos

#### Primera Vez (descarga completa ~30 minutos)
```bash
# 1. Mercancías + Servicios agregados (~2 min)
python3 etl_loader_completo.py

# 2. Socios comerciales BIENES (~15 min)
python3 etl_partners.py

# 3. Socios comerciales SERVICIOS (~10 min)
python3 etl_partners_services.py
```

#### Actualizaciones (usa script maestro)
```bash
python3 update_all_data.py           # Solo si cache > 7 días
python3 update_all_data.py --force   # Forzar re-descarga
python3 update_all_data.py --skip-partners  # Solo agregados (rápido)
```

### 2. Ejecutar Dashboard
```bash
streamlit run widget_balanza_completa.py
```

## 📦 Cobertura de Datos

### Países (31)
**UE-27** + Reino Unido, Suiza, Noruega, Islandia

### Socios Comerciales (40)
- **UE-27** (20): FR, DE, IT, NL, BE, ES, PL, AT, CZ, SE, DK, PT, RO, HU, FI, IE, GR, SK, BG, HR
- **Extra-UE** (20): GB, CH, NO, CN, US, TR, RU, JP, IN, KR, BR, MX, CA, AU, SA, AE, ZA, SG, TH, MY

### Sectores SITC (10 + TOTAL)
0-Alimentos | 1-Bebidas | 2-Materias primas | 3-Energía | 4-Aceites | 5-Químicos | 6-Manufacturas básicas | 7-Maquinaria | 8-Manufacturas diversas | 9-Otros

### Periodo Temporal
- **Mercancías**: 2002-2025 (mensual)
- **Servicios**: 2002-2025 (trimestral → interpolado mensual)

## 🗂️ Estructura del Proyecto

```
WidgetBalanza/
├── README.md                      # Este archivo
├── etl_loader_completo.py         # ETL mercancías + servicios agregados
├── etl_partners.py                # ETL socios BIENES
├── etl_partners_services.py       # ETL socios SERVICIOS (UNIFICADO)
├── update_all_data.py             # Script maestro actualización
├── widget_balanza_completa.py     # Dashboard Streamlit
├── .gitignore                     # Excluir data/
└── data/                          # Directorio de datos (gitignored)
    ├── goods/
    │   └── datos_mercancias_cache.csv (34 MB)
    ├── services/
    │   └── datos_servicios_cache.csv (2.4 MB)
    ├── partners/                  # 62 archivos (31 × 2)
    │   ├── partners_ES_imports.csv
    │   ├── partners_ES_exports.csv
    │   └── ...
    └── partners_services/         # 62 archivos (31 × 2)
        ├── services_partners_ES_imports.csv
        ├── services_partners_ES_exports.csv
        └── ...
```

## 🔄 Sistema de Actualización

### Cache y TTL
Todos los datasets tienen **TTL de 7 días**. Los scripts ETL verifican automáticamente:
1. ✅ Existencia de archivo
2. ✅ Tamaño mínimo (>1 KB)
3. ✅ Antigüedad (<7 días)

### Ubicación de Cache
| Dataset | Archivo | Tamaño | Verificación |
|---------|---------|--------|--------------|
| Mercancías | data/goods/datos_mercancias_cache.csv | 34 MB | Automática |
| Servicios | data/services/datos_servicios_cache.csv | 2.4 MB | Automática |
| Socios Bienes | data/partners/*.csv (62 archivos) | ~310 MB | Automática |
| Socios Servicios | data/partners_services/*.csv (62 archivos) | ~55 MB | Automática |

### Forzar Actualización
```bash
# Método 1: Eliminar cache manualmente
rm -rf data/
python3 update_all_data.py

# Método 2: Usar flag --force
python3 update_all_data.py --force
```

### Script Maestro (update_all_data.py)
Ejecuta los 3 ETL en secuencia con:
- ✅ Manejo de errores por script
- ✅ Logging de tiempos
- ✅ Resumen final
- ✅ Exit code apropiado

**Opciones**:
- `--force`: Elimina cache y re-descarga todo
- `--skip-partners`: Solo actualiza agregados (más rápido)

## 🔧 Troubleshooting

### Error: "Sin datos para país X"
**Causa**: País no disponible en Eurostat para el periodo
**Solución**: Verificar disponibilidad en [Eurostat Data Browser](https://ec.europa.eu/eurostat/databrowser)

### Error: "API Error 400"
**Causa**: Parámetros inválidos en query o API temporalmente caída
**Solución**:
1. Verificar conectividad: `curl https://ec.europa.eu/eurostat/api/comext`
2. Intentar más tarde (API Eurostat tiene mantenimientos)
3. Verificar códigos de país en scripts ETL

### Widget Carga Lento
**Causa**: Cache de Streamlit expirado (TTL 1 hora)
**Solución**: Reducir TTL en widget_balanza_completa.py línea 133:
```python
@st.cache_data(ttl=1800)  # 30 minutos en vez de 1 hora
```

### Datos Desactualizados
**Causa**: Cache > 7 días no se actualiza automáticamente
**Solución**:
```bash
python3 update_all_data.py  # Re-ejecutar ETLs
```

### Archivo all_bop_services.csv Ocupa Espacio
**Causa**: Archivo temporal no eliminado (versiones antiguas)
**Solución**: Usar etl_partners_services.py unificado que limpia automáticamente

## 📊 Fuentes de Datos
- **Eurostat COMEXT** (DS-059331): Comercio internacional de mercancías
- **Eurostat BOP** (BOP_C6_Q): Balanza de pagos (servicios)
- **Última actualización**: Enero 2026

## 🔗 Links Útiles
- [Repositorio GitHub](https://github.com/jaimeberdejo/WidgetMeteoconomics)
- [Eurostat API Documentation](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access)
- [Streamlit Documentation](https://docs.streamlit.io)

---
**Versión**: 1.0.0
**Última actualización**: Enero 2026
