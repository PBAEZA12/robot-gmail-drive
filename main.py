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
EMAIL_SUBJECT_PART_1 = 'DCV - Archivo RVCA'
EMAIL_SUBJECT_PART_2 = '10228.DAT'
TEXT_TO_FIND_IN_BODY = 'ReportesEmisores@dcv.cl'
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', '1pWkAErjDtUqCq8CBaAJEzGbSlPKNXz9C')

# Por defecto busca los archivos en la raíz del proyecto
GOOGLE_CREDENTIALS_PATH = os.environ.get('GOOGLE_CREDENTIALS_PATH', 'client_secret.json')
TOKEN_PATH = os.environ.get('TOKEN_PATH', 'token.json')

def run_process():
    """Función principal con la lógica de búsqueda, extracción y subida a Drive."""
    print("--- Iniciando ejecución del proceso ---")

    zip_password_str = os.environ.get('ZIP_PASSWORD', '76917333')
    if not zip_password_str:
        print("Error: La variable de entorno 'ZIP_PASSWORD' no está configurada.")
        return "Error: ZIP_PASSWORD no configurada."
    
    zip_password_bytes = zip_password_str.encode('utf-8')

    try:
        # --- AUTENTICACIÓN CON OAUTH2 (usuario, refresh token) ---
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
        search_date = datetime.now(santiago_tz) - timedelta(days=3)
        date_str = search_date.strftime('%y%m%d')
        print(f"La fecha calculada para la búsqueda es: {date_str}")
        
        # --- BÚSQUEDA MODIFICADA ---
        subject_to_find = f"{EMAIL_SUBJECT_PART_1}{date_str}{EMAIL_SUBJECT_PART_2}"
        # Se quitó "from:" y se añadió el texto a buscar en el cuerpo del correo
        query = f'subject:("{subject_to_find}") "{TEXT_TO_FIND_IN_BODY}" has:attachment'
        
        print(f"Buscando correo con la consulta: {query}")

        results = gmail_service.users().messages().list(userId='me', q=query, maxResults=1).execute()
        messages = results.get('messages', [])

        if not messages:
            print("Resultado: No se encontró ningún correo que coincida.")
            return "No se encontró el correo."

        msg_id = messages[0]['id']
        message = gmail_service.users().messages().get(userId='me', id=msg_id).execute()
        
        attachment_id = None
        source_zip_name = None
        for part in message['payload']['parts']:
            if part.get('filename') and part.get('filename').lower().endswith('.zip'):
                if part.get('body') and part['body'].get('attachmentId'):
                    attachment_id = part['body']['attachmentId']
                    source_zip_name = part['filename']
                    break

        if not attachment_id:
            print("No se encontró un adjunto .ZIP en el correo.")
            return "No se encontró un adjunto .ZIP."

        print(f"Adjunto encontrado: {source_zip_name}. Descargando...")
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
    santiago_tz = pytz.timezone('America/Santiago')
    today = datetime.now(santiago_tz)
    if today.weekday() >= 5:  # 5 = sábado, 6 = domingo
        print("Hoy es fin de semana (sábado o domingo). El proceso no se ejecuta.")
    else:
        run_process()