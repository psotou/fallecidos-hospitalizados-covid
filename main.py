import pandas as pd
import numpy as np
import os
import shutil
from datetime import datetime, timedelta

hosp_uci_etario = "https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto9/HospitalizadosUCIEtario_std.csv"
fallecidos_etario = "https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto10/FallecidosEtario_std.csv"

def main(url: str):    
    raw_df = pd.read_csv(url, encoding="utf-8")

    # cambiamos el nombre a las columnas
    intermediate_df = raw_df.rename(columns={"Grupo de edad":"grupo_etario", "Casos confirmados":"casos_confirmados"}) 
    intermediate_df.dropna()

    # generamos nuevas columnas
    intermediate_df["date"] = pd.to_datetime(intermediate_df.fecha) # si agregamos el - timedelta(1) se calculan los casos para la semana de lunes a domingo
    intermediate_df["week"] = intermediate_df.date.dt.strftime("%U") # semana del año como número
    intermediate_df["year"] = intermediate_df.date.dt.strftime("%Y") # año
    intermediate_df = intermediate_df.astype({"week": int})          # transformamos la semana del año a int

    # correlativo de final_dfs
    # intermediate_df.week[intermediate_df.year == "2021"] = intermediate_df.week[intermediate_df.year == "2021"] + 53
    intermediate_df.week = np.where(intermediate_df.year == "2021", intermediate_df.week + 53, intermediate_df.week)

    # hacemos un mapeo a los nuevos rangos etáreos según url
    if url == hosp_uci_etario:
        age_map = {"<=39":"<50", "40-49":"<50", "50-59":"50-69", "60-69":"50-69", ">=70":">=70"}
    elif url == fallecidos_etario:
        age_map = {"<=39":"<50", "40-49":"<50", "50-59":"50-69", "60-69":"50-69", "70-79":">=70", "80-89":">=70", ">=90":">=70"}

    intermediate_df.grupo_etario = intermediate_df.grupo_etario.map(age_map)

    # Al restar 1, la ultima final_df 2020 queda en la misma final_df que la primera del 2021
    # intermediate_df.week[intermediate_df.week >= 53] = intermediate_df.week[intermediate_df.week >= 53] - 1
    intermediate_df.week = np.where(intermediate_df.week >= 53, intermediate_df.week - 1, intermediate_df.week)

    # generamos las columnas de fecha min y max agrupando por número de final_df
    intermediate_df["min_dates"] = intermediate_df.groupby("week")["date"].transform("min")
    intermediate_df["max_dates"] = intermediate_df.groupby("week")["date"].transform("max")

    intermediate_df["largo_semana"] = 1 + (intermediate_df.max_dates - intermediate_df.min_dates).dt.days
    intermediate_df["semana_texto"] = intermediate_df.min_dates.dt.strftime("%d %b") + ' - ' + intermediate_df.max_dates.dt.strftime("%d %b %y")

    # agrupamos para obtener casos acumulados por semana
    # final_df = intermediate_df.groupby(["grupo_etario", "week", "semana_texto", "largo_semana"]).casos_confirmados.sum().reset_index()
    final_df = intermediate_df.groupby(["grupo_etario", "week", "semana_texto", "largo_semana", "min_dates", "max_dates"]).casos_confirmados.sum().reset_index()
    final_df = final_df.rename(columns={"casos_confirmados": "casos_totales", "week": "semana", "min_dates": "inicio_semana", "max_dates": "fin_semana"})

    final_df["casos_diarios_promedio"] = final_df.casos_totales / final_df.largo_semana
    final_df["promedio_final_df_ant"] = final_df.casos_diarios_promedio.shift()
    final_df["diferencia_promedios"] = final_df.casos_diarios_promedio - final_df.promedio_final_df_ant
    final_df["cambio_porcentual"] = round((final_df.diferencia_promedios / final_df.promedio_final_df_ant) * 100, 2)
    final_df = final_df.astype({"casos_diarios_promedio": int}) # para reportar el promedio diario como int

    final_df_reducido = final_df[["grupo_etario", "semana", "semana_texto", "inicio_semana", "casos_totales", "casos_diarios_promedio", "cambio_porcentual"]]

    return final_df_reducido, final_df


if __name__ == "__main__":
    # nota: se presenta la semana de domingo a sábado
    today = datetime.now().strftime("%Y%m%d")

    # hospitalizados etario
    if os.path.exists(f"./hospitalizados_etario/{today}"):
        shutil.rmtree(f"./hospitalizados_etario/{today}")
    
    os.mkdir(f"./hospitalizados_etario/{today}")

    ruta_hospitalizados_sabana = f"./hospitalizados_etario/{today}/hospitalizados_etario_sabana.csv"
    ruta_hospitalizados = f"./hospitalizados_etario/{today}/hospitalizados_etario.csv"

    df_reducido_hosp, df_sabana_hosp = main(hosp_uci_etario)
    df_sabana_hosp.to_csv(ruta_hospitalizados_sabana, index=False)
    df_reducido_hosp.to_csv(ruta_hospitalizados, index=False)

    # fallecidos etario
    if os.path.exists(f"./fallecidos_etario/{today}"):
        shutil.rmtree(f"./fallecidos_etario/{today}")
    
    os.mkdir(f"./fallecidos_etario/{today}")

    ruta_fallecidos_sabana = f"./fallecidos_etario/{today}/fallecidos_etario_sabana.csv"
    ruta_fallecidos = f"./fallecidos_etario/{today}/fallecidos_etario.csv"

    df_reducido_fallecidos, df_sabana_fallecidos = main(fallecidos_etario)
    df_sabana_fallecidos.to_csv(ruta_fallecidos_sabana, index=False)
    df_reducido_fallecidos.to_csv(ruta_fallecidos, index=False)
