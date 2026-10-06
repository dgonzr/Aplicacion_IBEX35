import yfinance as yf
from sesion_spark import spark

#Ej 1-a
def descargar_histórico(empresa:str, fecha_inicio:str, fecha_fin:str):
    pdf = yf.Ticker(empresa).history(start=fecha_inicio, end=fecha_fin, interval="1d", auto_adjust=False)
    pdf.reset_index(inplace=True) # Convertir el índice 'Date' a columna
    df = spark.createDataFrame(pdf)
    return df

#Ej 1-b
empresas = ["ANA.MC", "ANE.MC", "ACX.MC", "ACS.MC", "AENA.MC", "AMS.MC", "MTS.MC", "BBVA.MC", "SAB.MC", "SAN.MC", "BKT.MC", "CABK.MC", "CLNX.MC", "COL.MC", "ENG.MC", "ELE.MC", "FER.MC", "FDR.MC", "GRF.MC", "IAG.MC", "IBE.MC", "ITX.MC", "IDR.MC", "LOG.MC", "MAP.MC", "MRL.MC", "NTGY.MC", "PUIG.MC", "RED.MC", "REP.MC", "ROVI.MC", "SCYR.MC", "SLR.MC", "TEF.MC", "UNI.MC"]
for empresa in empresas:
    df = descargar_histórico(empresa, "2024-10-01", "2026-10-01")
    df.write.mode("overwrite").parquet("data/lake/bronze/ibex")
