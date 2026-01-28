# 📦 Instrucciones para Generar y Subir Datos

## ✅ Todo está listo

El repositorio ya está configurado para aceptar archivos CSV de datos. El `.gitignore` ha sido modificado para permitir el push del directorio `data/`.

## 🚀 Opción 1: Script Automático (Recomendado)

### Uso Simple
```bash
# 1. Configurar tu token de GitHub
export GITHUB_TOKEN=ghp_tu_token_aqui

# 2. Ejecutar el script (genera datos y hace push)
./generate_and_push_data.sh
```

El script hace:
1. ✅ Ejecuta `update_all_data.py` (descarga todos los datos)
2. ✅ Verifica que no haya archivos >100MB
3. ✅ Hace `git add data/`
4. ✅ Crea commit con mensaje descriptivo
5. ✅ Push a GitHub

### Ventajas
- Todo automatizado
- Validación de tamaños
- Mensajes de progreso
- Manejo de errores

## 🔧 Opción 2: Manual

### Paso 1: Generar Datos
```bash
# Generar todos los datos (tarda ~30 minutos)
python3 update_all_data.py

# O generar selectivamente:
python3 etl_loader_completo.py          # Agregados (~2 min)
python3 etl_partners.py                 # Bienes (~15 min)
python3 etl_partners_services.py        # Servicios (~10 min)
```

### Paso 2: Verificar Tamaños
```bash
# Ver tamaño de cada directorio
du -sh data/*

# Buscar archivos >100MB (GitHub los rechazaría)
find data -type f -size +100M
```

### Paso 3: Git Add, Commit y Push
```bash
# Añadir datos
git add data/

# Ver qué se va a subir
git status

# Commit
git commit -m "data: añadir datasets completos

- Mercancías: data/goods/datos_mercancias_cache.csv
- Servicios: data/services/datos_servicios_cache.csv
- Socios Bienes: data/partners/*.csv (62 archivos)
- Socios Servicios: data/partners_services/*.csv (62 archivos)

Generado: $(date)"

# Push (con tu token)
git push https://TU_TOKEN@github.com/jaimeberdejo/WidgetMeteoconomics.git main
```

## 📊 Estructura de Datos Generada

```
data/
├── goods/
│   └── datos_mercancias_cache.csv        (~34 MB)
├── services/
│   └── datos_servicios_cache.csv         (~2.4 MB)
├── partners/                              (62 archivos)
│   ├── partners_ES_imports.csv
│   ├── partners_ES_exports.csv
│   ├── partners_FR_imports.csv
│   └── ...                                (~310 MB total)
└── partners_services/                     (62 archivos)
    ├── services_partners_ES_imports.csv
    ├── services_partners_ES_exports.csv
    └── ...                                (~55 MB total)

TOTAL: ~400 MB, 126 archivos
```

## ⚠️ Notas Importantes

### Límites de GitHub
- ✅ Archivos individuales: máximo 100 MB
- ✅ Repositorio total: recomendado < 1 GB
- ✅ Nuestros archivos más grandes: ~34 MB ✓

### Actualización de Datos
Los datos tienen **TTL de 7 días**. Para actualizar:

```bash
# Re-ejecutar el script (detecta cache expirado)
./generate_and_push_data.sh

# O forzar actualización
python3 update_all_data.py --force
git add data/
git commit -m "data: actualización $(date +%Y-%m-%d)"
git push
```

### Seguridad del Token
- ⚠️ **NUNCA** incluyas tu token directamente en scripts
- ✅ Usa variable de entorno: `export GITHUB_TOKEN=...`
- ✅ Añade el token a tu `.bashrc` o `.zshrc` (opcional)
- ✅ Revoca tokens viejos en: https://github.com/settings/tokens

## 🎯 Resumen Rápido

```bash
# Todo en uno (recomendado)
export GITHUB_TOKEN=ghp_tu_token
./generate_and_push_data.sh
```

¡Y listo! Los datos estarán en GitHub en ~30 minutos.
