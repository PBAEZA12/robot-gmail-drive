import base64
import io
import os
import pyzipper
from datetime import datetime, timedelta
import pytz

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import google.auth.exceptions
import pickle

# --- CONFIGURACIÓN ---
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/drive']

EMAIL_SUBJECT_PART_1 = 'DCV - Archivo'
EMAIL_SUBJECT_PART_2 = 'L002'
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID_MOVIMIENTOS_Y_SALDOS')

# Permite usar variables de entorno personalizadas para las rutas de credenciales
GOOGLE_CREDENTIALS_PATH = os.environ.get('GOOGLE_OAUTH_CLIENT_SECRET',
    os.environ.get('GOOGLE_CREDENTIALS_PATH', 'client_secret.json'))
TOKEN_PATH = os.environ.get('GOOGLE_OAUTH_TOKEN',
    os.environ.get('TOKEN_PATH', 'token.json'))

def run_process():
    """Función principal con la lógica de búsqueda, extracción y subida a Drive."""
    print("--- Iniciando ejecución del proceso ---")

    zip_password_str = os.environ.get('ZIP_PASSWORD')
    if not zip_password_str:
        print("Error: La variable de entorno 'ZIP_PASSWORD' no está configurada.")
        return "Error: ZIP_PASSWORD no configurada."
    
    zip_password_bytes = zip_password_str.encode('utf-8')


    try:
        # --- AUTENTIFICACIÓN CON OAUTH2 (usuario, refresh token) ---
        print("Autenticando con OAuth2 de usuario...")
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
        # Si no hay credenciales válidas, iniciar flujo OAuth
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except google.auth.exceptions.RefreshError:
                    creds = None
            if not creds:
                flow = InstalledAppFlow.from_client_secrets_file(
                    GOOGLE_CREDENTIALS_PATH, SCOPES)
                creds = flow.run_local_server(port=0)
            # Guardar el token para la próxima vez
            with open(TOKEN_PATH, 'wb') as token:
                pickle.dump(creds, token)
        gmail_service = build('gmail', 'v1', credentials=creds)
        drive_service = build('drive', 'v3', credentials=creds)
        print("Autenticación exitosa.")

        # --- CÁLCULO DE FECHA ---
        santiago_tz = pytz.timezone('America/Santiago')
        search_date = datetime.now(santiago_tz) - timedelta(days=1)
        date_str = search_date.strftime('%Y%m%d')
        print(f"La fecha calculada para la búsqueda es: {date_str}")

        # --- BÚSQUEDA MODIFICADA ---

        # Buscar correos cuyo asunto contenga ambas partes clave, aunque tenga texto antes, entre o después
        subject_key1 = EMAIL_SUBJECT_PART_1
        subject_key2 = f"{EMAIL_SUBJECT_PART_2}{date_str}"
        query = f'subject:("{subject_key1}") subject:("{subject_key2}") has:attachment'

        print(f"Buscando correo con la consulta: {query}")

        results = gmail_service.users().messages().list(userId='me', q=query, maxResults=1).execute()
        messages = results.get('messages', [])

        if not messages:
            print("Resultado: No se encontró ningún correo que coincida.")
            return "No se encontró el correo."

        msg_id = None
        attachment_id = None
        source_txt_name = None
        import re
        # El patrón ahora es EMAIL_SUBJECT_PART_2 + fecha + 4 dígitos + .txt
        pattern_str = rf'^{re.escape(EMAIL_SUBJECT_PART_2)}{date_str}\d{{4}}\.txt$'
        pattern = re.compile(pattern_str, re.IGNORECASE)

        # Buscar el mensaje y adjunto que cumpla con el patrón
        for msg in messages:
            m = gmail_service.users().messages().get(userId='me', id=msg['id']).execute()
            for part in m['payload'].get('parts', []):
                filename = part.get('filename', '')
                if pattern.match(filename):
                    if part.get('body') and part['body'].get('attachmentId'):
                        msg_id = msg['id']
                        message = m
                        attachment_id = part['body']['attachmentId']
                        source_txt_name = filename
                        break
            if msg_id:
                break

        if not attachment_id:
            print("No se encontró un adjunto .TXT con formato nnnn.txt en los correos.")
            return "No se encontró un adjunto .TXT válido."

        print(f"Adjunto encontrado: {source_txt_name}. Descargando...")
        attachment = gmail_service.users().messages().attachments().get(userId='me', messageId=msg_id, id=attachment_id).execute()
        file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))

        # --- DESCOMPRIMIR Y SUBIR ARCHIVOS ---
        print("Descomprimiendo el archivo en memoria...")
        with pyzipper.AESZipFile(io.BytesIO(file_data), 'r') as zf:
            zf.setpassword(zip_password_bytes)
            for filename in zf.namelist():
                print(f"Procesando archivo extraído: {filename}")
                response = drive_service.files().list(
                    q=f"name='{filename}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false",
                    spaces='drive', fields='files(id)').execute()
                for file in response.get('files', []):
                    drive_service.files().delete(fileId=file.get('id')).execute()
                    print(f"  -> Versión antigua de '{filename}' eliminada de Drive.")
                file_metadata = {'name': filename, 'parents': [DRIVE_FOLDER_ID]}
                media = MediaIoBaseUpload(io.BytesIO(zf.read(filename)), mimetype='application/octet-stream', resumable=True)
                drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                print(f"  -> '{filename}' subido a Google Drive exitosamente.")

        print("--- Proceso completado exitosamente ---")
        return "Proceso completado."

    except Exception as e:
        print(f"Ocurrió un error crítico: {e}")
        raise e

if __name__ == '__main__':
    run_process()