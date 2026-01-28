"""
Script maestro para actualizar todos los datos del widget
Ejecuta los 3 ETL en secuencia con manejo de errores y logging
"""

import subprocess
import sys
import argparse
import shutil
from pathlib import Path
from datetime import datetime

def run_etl_script(script_name, description):
    """Ejecuta un script ETL con logging de tiempo"""
    print(f"\n{'='*80}")
    print(f"🔄 {description}")
    print(f"{'='*80}\n")

    start_time = datetime.now()

    result = subprocess.run(
        ['python3', script_name],
        capture_output=True,
        text=True
    )

    elapsed = (datetime.now() - start_time).total_seconds()

    if result.returncode == 0:
        print(f"\n✅ {description} completado en {elapsed:.1f}s")
        return True
    else:
        print(f"\n❌ {description} falló:")
        print(result.stderr[-500:] if len(result.stderr) > 500 else result.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Actualizar datos del Widget Balanza Comercial',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python3 update_all_data.py                    # Actualizar solo si cache expirado
  python3 update_all_data.py --force            # Forzar actualización completa
  python3 update_all_data.py --skip-partners    # Solo agregados (más rápido)
        """
    )
    parser.add_argument('--force', action='store_true',
                       help='Forzar actualización eliminando cache')
    parser.add_argument('--skip-partners', action='store_true',
                       help='Saltar actualización de socios (más rápido)')
    args = parser.parse_args()

    print("=" * 80)
    print("ACTUALIZACIÓN MAESTRA - WIDGET BALANZA COMERCIAL")
    print("=" * 80)

    # Forzar actualización
    if args.force:
        print("\n🗑️  FORZANDO ACTUALIZACIÓN: Eliminando cache...")
        dirs_to_clean = ['data/goods', 'data/services', 'data/partners', 'data/partners_services']
        for dir_name in dirs_to_clean:
            dir_path = Path(dir_name)
            if dir_path.exists():
                shutil.rmtree(dir_path)
                print(f"   ✓ {dir_name}/ eliminado")
        print()

    # Definir ETLs a ejecutar
    etl_scripts = [
        ('etl_loader_completo.py', 'Mercancías + Servicios agregados'),
    ]

    if not args.skip_partners:
        etl_scripts.extend([
            ('etl_partners.py', 'Socios comerciales - BIENES'),
            ('etl_partners_services.py', 'Socios comerciales - SERVICIOS'),
        ])

    start_total = datetime.now()
    success_count = 0
    failed_scripts = []

    # Ejecutar ETLs
    for script, description in etl_scripts:
        if run_etl_script(script, description):
            success_count += 1
        else:
            failed_scripts.append(script)
            print(f"\n⚠️ Continuando con siguiente ETL...")

    # Resumen
    elapsed_total = (datetime.now() - start_total).total_seconds()

    print(f"\n{'='*80}")
    print(f"📊 RESUMEN DE ACTUALIZACIÓN")
    print(f"{'='*80}")
    print(f"✓ Scripts exitosos: {success_count}/{len(etl_scripts)}")
    print(f"⏱️  Tiempo total: {elapsed_total/60:.1f} minutos")

    if failed_scripts:
        print(f"\n❌ Scripts fallidos:")
        for script in failed_scripts:
            print(f"   - {script}")

    if success_count == len(etl_scripts):
        print("\n✅ ACTUALIZACIÓN COMPLETA - Todos los datos actualizados")
        sys.exit(0)
    elif success_count > 0:
        print(f"\n⚠️ ACTUALIZACIÓN PARCIAL - {success_count}/{len(etl_scripts)} completados")
        print("💡 Ejecuta scripts fallidos manualmente para completar")
        sys.exit(1)
    else:
        print("\n❌ ACTUALIZACIÓN FALLIDA - Ningún script completado")
        print("💡 Verifica conectividad y logs de error")
        sys.exit(1)

if __name__ == "__main__":
    main()
