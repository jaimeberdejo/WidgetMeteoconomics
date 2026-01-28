#!/bin/bash
# Script para generar datos y hacer push a GitHub
# Uso: ./generate_and_push_data.sh

set -e  # Salir si hay errores

echo "=========================================="
echo "GENERACIÓN Y PUSH DE DATOS"
echo "=========================================="
echo ""

# Verificar que estamos en el directorio correcto
if [ ! -f "etl_loader_completo.py" ]; then
    echo "❌ Error: Este script debe ejecutarse desde el directorio WidgetBalanza"
    exit 1
fi

# 1. Generar datos
echo "📥 PASO 1/4: Generando datos..."
echo ""

python3 update_all_data.py

if [ $? -ne 0 ]; then
    echo "❌ Error generando datos"
    exit 1
fi

echo ""
echo "✅ Datos generados correctamente"
echo ""

# 2. Verificar tamaños
echo "📊 PASO 2/4: Verificando tamaños de archivos..."
echo ""

# Buscar archivos >100MB que GitHub rechazaría
large_files=$(find data -type f -size +100M 2>/dev/null || echo "")

if [ -n "$large_files" ]; then
    echo "⚠️ ADVERTENCIA: Archivos >100MB encontrados (GitHub los rechazará):"
    echo "$large_files"
    echo ""
    read -p "¿Continuar de todos modos? (y/N): " response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        echo "❌ Push cancelado"
        exit 1
    fi
fi

# Mostrar resumen de tamaños
echo "📦 Tamaño de directorios:"
du -sh data/* 2>/dev/null | sort -h
echo ""
echo "📁 Total data/:"
du -sh data 2>/dev/null
echo ""

# 3. Git add
echo "📝 PASO 3/4: Añadiendo archivos a Git..."
echo ""

git add data/

# Contar archivos añadidos
num_files=$(git status --short | grep "^A" | wc -l | tr -d ' ')
echo "✅ $num_files archivos añadidos al staging"
echo ""

# 4. Commit y Push
echo "🚀 PASO 4/4: Commit y Push a GitHub..."
echo ""

# Verificar si hay cambios
if git diff --cached --quiet; then
    echo "ℹ️ No hay cambios nuevos para hacer commit"
else
    # Crear commit
    git commit -m "data: añadir datasets completos

- Mercancías: data/goods/datos_mercancias_cache.csv
- Servicios: data/services/datos_servicios_cache.csv
- Socios Bienes: data/partners/*.csv (62 archivos)
- Socios Servicios: data/partners_services/*.csv (62 archivos)

Generado con: update_all_data.py
Fecha: $(date '+%Y-%m-%d %H:%M:%S')"

    echo "✅ Commit creado"
    echo ""

    # Push con token (usa variable de entorno GITHUB_TOKEN)
    echo "Haciendo push a GitHub..."

    if [ -z "$GITHUB_TOKEN" ]; then
        echo "⚠️ Variable GITHUB_TOKEN no definida"
        echo "Usando git push normal (requerirá autenticación)..."
        git push origin main
    else
        git push https://$GITHUB_TOKEN@github.com/jaimeberdejo/WidgetMeteoconomics.git main
    fi

    if [ $? -eq 0 ]; then
        echo ""
        echo "=========================================="
        echo "✅ PUSH COMPLETADO EXITOSAMENTE"
        echo "=========================================="
        echo ""
        echo "📊 Repositorio: https://github.com/jaimeberdejo/WidgetMeteoconomics"
        echo "🎉 Los datos están ahora en GitHub"
    else
        echo ""
        echo "❌ Error durante el push"
        exit 1
    fi
fi
