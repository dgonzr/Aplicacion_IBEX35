import yfinance as yf
from sesion_spark import spark

def descargar_histórico(empresa:str, fecha_inicio:str, fecha_fin:str):
    pdf = yf.Ticker(empresa).history(start=fecha_inicio, end=fecha_fin, interval="1d", auto_adjust=False)
    pdf.reset_index(inplace=True) # Convertir el índice 'Date' a columna
    df = spark.createDataFrame(pdf)
    return df

bbva_df = descargar_histórico("BBVA.MC", "2024-10-01", "2026-10-01")

