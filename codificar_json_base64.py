import base64
from pathlib import Path

def codificar_a_base64(nombre_archivo: str) -> str:
    ruta = Path(nombre_archivo)
    if not ruta.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {ruta}")
    
    with ruta.open("rb") as f:
        contenido = f.read()
        codificado = base64.b64encode(contenido).decode("utf-8")
        return codificado

def guardar_en_txt(nombre_base: str, contenido: str):
    salida = Path(f"{nombre_base}.b64.txt")
    with salida.open("w", encoding="utf-8") as f:
        f.write(contenido)
    print(f"Base64 guardado en: {salida.resolve()}")

if __name__ == "__main__":
    nombre_archivo = input("Ingresa el nombre del archivo a codificar (por ejemplo: credentials.json o certificate.pem): ").strip()
    
    try:
        base64_codificado = codificar_a_base64(nombre_archivo)
        nombre_base = Path(nombre_archivo).stem  # "credentials" de "credentials.json" o "certificate" de "certificate.pem"
        guardar_en_txt(nombre_base, base64_codificado)
    except FileNotFoundError as e:
        print(f"Error: {e}")
