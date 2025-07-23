import gzip
import shutil
from pathlib import Path

# Rutas
data_dir = Path("C:/Users/ulven/Programming/Work/TeamCore/PruebaTécnica/shared/data")
original_file = data_dir / "sample.log.gz"
temp_file = data_dir / "sample.log"
compressed_file = data_dir / "sample.log.gz.compressed"

# Copiar el archivo original a uno temporal
shutil.copy(original_file, temp_file)

# Comprimir el archivo temporal
with open(temp_file, 'rb') as f_in:
    with gzip.open(compressed_file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

# Reemplazar el archivo original
shutil.move(compressed_file, original_file)

# Limpiar archivo temporal
temp_file.unlink()

print(f"Archivo comprimido correctamente: {original_file}")
