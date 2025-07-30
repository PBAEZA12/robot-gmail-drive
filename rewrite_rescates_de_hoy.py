

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
import openpyxl
import tempfile
import shutil
import pandas as pd
from googleapiclient.http import MediaIoBaseDownload
import io
from google.auth.transport.requests import Request
import pickle

# If modifying these SCOPES, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']

# Permite usar variables de entorno personalizadas para las rutas de credenciales
GOOGLE_CREDENTIALS_PATH = os.environ.get('GOOGLE_OAUTH_CLIENT_SECRET', os.environ.get('GOOGLE_CREDENTIALS_PATH', os.path.join('credenciales', 'client_secret.json')))
TOKEN_PATH = os.environ.get('GOOGLE_OAUTH_TOKEN', os.environ.get('TOKEN_PATH', os.path.join('credenciales', 'token.json')))

def authenticate_drive():
    creds = None
    if os.path.exists(TOKEN_PATH):
        try:
            with open(TOKEN_PATH, 'rb') as token:
                creds = pickle.load(token)
        except Exception as token_error:
            print(f"Advertencia: No se pudo cargar el token existente ({token_error}). Se eliminará y se generará uno nuevo.")
            try:
                os.remove(TOKEN_PATH)
            except Exception as remove_error:
                print(f"No se pudo eliminar el token corrupto: {remove_error}")
            creds = None
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                GOOGLE_CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'wb') as token:
            pickle.dump(creds, token)
    return creds

def list_files_in_folder(service, folder_id):
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print(f"{item['name']} (mimeType: {item.get('mimeType', 'N/A')})")
        return items

if __name__ == '__main__':
    # Reemplaza este ID por el de la carpeta de Drive que deseas listar
    FOLDER_ID = '1LphUYvK4gbujTxDdo64tdRCXgVuubjOc'
    creds = authenticate_drive()
    service = build('drive', 'v3', credentials=creds)

    # Primero listar los archivos en la carpeta de Drive
    archivos = list_files_in_folder(service, FOLDER_ID)

    # Filtrar archivos con patrón 'Rescates de hoy dd-mm-yyyy' (con o sin .xlsx) y mimeType de Excel
    import re
    from datetime import datetime

    patron = re.compile(r"Rescates de hoy (\d{2}-\d{2}-\d{4})(?:\.xlsx)?$")
    mimetypes_validos = [
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',  # Excel xlsx
        'application/vnd.ms-excel',  # Excel xls
        'application/vnd.google-apps.spreadsheet'  # Google Sheets
    ]
    archivos_fecha = []
    for a in archivos:
        m = patron.fullmatch(a['name'])
        if m and a.get('mimeType') in mimetypes_validos:
            try:
                fecha = datetime.strptime(m.group(1), "%d-%m-%Y")
                archivos_fecha.append((fecha, a))
            except ValueError:
                pass

    if archivos_fecha:
        archivos_fecha.sort(reverse=True)  # Más reciente primero
        fecha_reciente, archivo_reciente = archivos_fecha[0]
        print(f"El archivo más reciente es: {archivo_reciente['name']}")

        # Descargar el archivo más reciente a un directorio temporal
        with tempfile.TemporaryDirectory() as tmpdirname:
            file_id = archivo_reciente['id']
            file_name = archivo_reciente['name']
            mime_type = archivo_reciente.get('mimeType')
            local_path = os.path.join(tmpdirname, file_name + '.xlsx')

            # Si es Google Sheets, exportar como xlsx
            if mime_type == 'application/vnd.google-apps.spreadsheet':
                request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            else:
                request = service.files().get_media(fileId=file_id)
            fh = io.FileIO(local_path, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            fh.close()

            # Leer los datos del archivo descargado
            df = pd.read_excel(local_path, engine='openpyxl' if local_path.endswith('.xlsx') else None)

            # Buscar si existe 'Rescates de hoy T-Habil.xlsx' en la carpeta
            habil_filename = 'Rescates de hoy T-Habil.xlsx'
            habil_file = None
            for a in archivos:
                if a['name'] == habil_filename:
                    habil_file = a
                    break

            # Guardar los datos en un nuevo archivo Excel temporal
            habil_local_path = os.path.join(tmpdirname, habil_filename)
            df.to_excel(habil_local_path, index=False)

            # Subir (o reemplazar) el archivo en Drive
            media = MediaFileUpload(habil_local_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            if habil_file:
                # Actualizar el archivo existente
                service.files().update(fileId=habil_file['id'], media_body=media).execute()
                print(f"Archivo '{habil_filename}' sobreescrito en Drive.")
            else:
                # Crear el archivo
                file_metadata = {
                    'name': habil_filename,
                    'parents': [FOLDER_ID],
                    'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                }
                service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                print(f"Archivo '{habil_filename}' creado en Drive.")
    else:
        print("No se encontraron archivos de Excel o Google Sheets con el patrón 'Rescates de hoy dd-mm-yyyy' (con o sin .xlsx).")
        