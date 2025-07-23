import gzip
import json

file_path = r"C:\Users\ulven\Programming\Work\TeamCore\PruebaTécnica\shared\data\sample.log.gz"

# Contar registros y verificar estructura
count = 0
error_count = 0
status_500_count = 0

print("Analizando archivo sample.log.gz...")

with gzip.open(file_path, 'rt', encoding='utf-8') as f:
    for i, line in enumerate(f):
        line = line.strip()
        if not line:
            continue
            
        count += 1
        
        try:
            record = json.loads(line)
            if record.get('status_code', 0) >= 500:
                status_500_count += 1
                
            # Mostrar primer registro como muestra
            if count == 1:
                print("\nPrimer registro (muestra):")
                print(json.dumps(record, indent=2))
                
        except json.JSONDecodeError:
            error_count += 1
            
        # Mostrar progreso cada millón
        if count % 1_000_000 == 0:
            print(f"Procesados {count:,} registros...")

print(f"\n=== ANÁLISIS COMPLETO ===")
print(f"Total registros: {count:,}")
print(f"Registros con status >= 500: {status_500_count:,}")
print(f"Registros con errores JSON: {error_count:,}")
print(f"Porcentaje filtrado: {(status_500_count/count)*100:.2f}%")
