#!/usr/bin/env python3

import datetime
import os
import sys
import json
import pandas as pd
import yfinance as yf
from yahoofinancials import YahooFinancials
from google.cloud import storage

# Configuración de las credenciales de Google Cloud
PATH = os.path.join(os.getcwd(), 'insertar_api_key.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH

# Obtener el directorio donde se ejecuta el script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def download_ticker_data(ticker, start_date='2013-01-01'):
    """
    Descarga datos históricos de precios desde Yahoo Finance y los guarda en un archivo CSV.

    Args:
    - ticker (str): Símbolo del activo financiero.
    - start_date (str): Fecha de inicio para los datos históricos en formato 'YYYY-MM-DD'.

    Returns:
    - str: Ruta del archivo CSV generado.
    """
    end_date = datetime.datetime.today()
    stock_data = yf.download(ticker, start=start_date, end=end_date, progress=False)
    
    filename = os.path.join(SCRIPT_DIR, f"{ticker}_data.csv")
    stock_data.to_csv(filename)
    return filename

def upload_to_cloud_storage(bucket_name, source_file_name, destination_file_name):
    """
    Sube un archivo desde la máquina local a un bucket de Google Cloud Storage.

    Args:
    - bucket_name (str): Nombre del bucket de Google Cloud Storage.
    - source_file_name (str): Ruta del archivo local a subir.
    - destination_file_name (str): Nombre del archivo en el bucket.

    Returns:
    - bool: True si la subida fue exitosa, False en caso contrario.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_file_name)
        blob.upload_from_filename(source_file_name)
        return True
    except Exception as e:
        print(f"Error al subir el archivo: {e}")
        return False

def main():
    """
    Función principal que descarga datos de tickers y los sube a Google Cloud Storage.
    """
    print('Iniciando proceso de descarga y subida de datos...')
    
    tickers = ['VNQ', 'VMBS']  # VNQ: Vanguard Real Estate ETF, VMBS: Vanguard Mortgage-Backed Securities ETF
    bucket_name = 'market-data001'

    for ticker in tickers:
        print(f"Procesando {ticker}...")
        local_file = download_ticker_data(ticker)
        cloud_file = f'{ticker}_data_update.csv'
        
        if upload_to_cloud_storage(bucket_name, local_file, cloud_file):
            print(f"Archivo '{local_file}' subido exitosamente como '{cloud_file}'")
        else:
            print(f"Fallo al subir '{local_file}'")

if __name__ == "__main__":
    main()
