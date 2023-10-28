#!/usr/bin/python3
import datetime
import os
import sys
import json
import pandas as pd
import yfinance as yf
from yahoofinancials import YahooFinancials
from google.cloud import storage

PATH = os.path.join(os.getcwd(), 'insertar_api_key.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH

# Obtener el directorio donde se corre el script
script_dir = os.path.dirname(__file__)

print ('Subiendo data a Cloud Storage ... ')
def ticker_data(ticker):
    """
    Baja datos de precio historico desde Yahoo Finance
    luego los guarda a .csv
    """

    end_date = datetime.datetime.today()
    stock_data = yf.download(ticker, start='2013-01-01', end=end_date, progress=False)
    
    # Crear una direccion absoluta para el CSV en el script del directorio
    filename = os.path.join(script_dir, f"{ticker}_data.csv")
    stock_data.to_csv(filename)

def upload_cs_file(bucket_name, source_file_name, destination_file_name):
    """
    Sube un archivo desde maquina local a un bucket de Google Cloud Storage.

    Args:
    - bucket_name (str): Nombre de bucket de Google Cloud Storage.
    - source_file_name (str): Nombre del archivo local a subir.
    - destination_file_name (str): Nombre del archivo en el bucket.

    Returns:
    - True si la subida fue exitosa.
    """

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_file_name)
    blob.upload_from_filename(source_file_name)

    return True


if __name__ == "__main__":
    tickers = ['VNQ', 'VMBS']             # VNQ Vanguard ETF, VMBS Vanguard Mortgage

    for ticker in tickers:
        ticker_data(ticker)
        uploaded = upload_cs_file('market-data001', f'{ticker}_data.csv', f'{ticker}_data_update.csv')
        
        if uploaded:
            print(f"Archivo '{ticker}_data.csv' fue subido exitosamente como: '{ticker}_data_update.csv'")
        else:
            print(f"Fallo al subir '{ticker}_data.csv'")
