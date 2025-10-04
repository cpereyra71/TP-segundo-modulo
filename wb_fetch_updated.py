
import argparse
import time
import sys
import requests
import pandas as pd

BASE = "https://api.worldbank.org/v2"

# ---- Países: Mercosur (ARG, BRA, PRY, URY, VEN [suspendido]) + Chile ----
COUNTRIES = {
    "Argentina": "ARG",
    "Brasil": "BRA",
    "Paraguay": "PRY",
    "Uruguay": "URY",
    "Venezuela": "VEN",
    "Chile": "CHL",
}

# ---- Filtros de nombres de TÓPICOS ----
# Buscamos de forma robusta por nombre del tópico
TOPIC_NAME_KEYWORDS = {
    "economy_growth": ["economy", "growth"],
    "external_debt": ["debt"],  # incluye "External debt", "Debt & financial flows", etc.
}

def _get_json(url, params=None, max_retries=5, backoff=1.5):
    """GET con reintentos simples (World Bank API es robusta, pero a veces página/timeout)."""
    for attempt in range(1, max_retries+1):
        try:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == max_retries:
                raise
            sleep_for = backoff ** attempt
            time.sleep(sleep_for)

def list_topics():
    """Devuelve dataframe de tópicos (id, value)"""
    j = _get_json(f"{BASE}/topic", params={"format": "json"})
    # estructura: [ metadata, [{id, value},{...}] ]
    data = pd.DataFrame(j[1])
    return data[["id","value"]]

def pick_topic_ids_by_keywords(topics_df, keywords):
    """Dado df de tópicos y una lista de palabras clave, devuelve ids cuyos nombres contienen todas las keywords."""
    ids = []
    for _, row in topics_df.iterrows():
        name = str(row["value"]).lower()
        if all(kw.lower() in name for kw in keywords):
            ids.append(str(row["id"]))
    return ids

def list_indicators_for_topic(topic_id):
    """Lista todos los indicadores para un topic_id (aplicando paginación)."""
    indicators = []
    page = 1
    while True:
        j = _get_json(f"{BASE}/topic/{topic_id}/indicator", params={
            "format":"json",
            "per_page": 20000,
            "page": page
        })
        meta = j[0]
        data = j[1] if len(j) > 1 else []
        indicators.extend(data)
        if page >= meta.get("pages", 1):
            break
        page += 1
        time.sleep(0.2)
    if not indicators:
        return pd.DataFrame(columns=["id","name","unit","sourceNote","sourceOrganization","topic_id"])
    df = pd.DataFrame(indicators)
    df["topic_id"] = topic_id
    # Normalizamos columnas comunes
    cols = ["id","name","unit","sourceNote","sourceOrganization","topic_id"]
    return df.reindex(columns=cols)

def list_indicators_for_topics(topic_ids):
    frames = []
    for tid in topic_ids:
        frames.append(list_indicators_for_topic(tid))
    if frames:
        df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["id"]).reset_index(drop=True)
    else:
        df = pd.DataFrame(columns=["id","name","unit","sourceNote","sourceOrganization","topic_id"])
    return df

def fetch_series(indicator_code, country_codes, start_year, end_year):
    """Descarga la serie de un indicador para múltiples países en un rango de años. Maneja paginación."""
    series = []
    page = 1
    codes = ";".join(country_codes)
    while True:
        j = _get_json(f"{BASE}/country/{codes}/indicator/{indicator_code}", params={
            "date": f"{start_year}:{end_year}",
            "format": "json",
            "per_page": 20000,
            "page": page
        })
        meta = j[0]
        data = j[1] if len(j) > 1 else []
        for item in data:
            series.append({
                "country_iso3": item.get("countryiso3code"),
                "country": (item.get("country") or {}).get("value"),
                "indicator": indicator_code,
                "date": item.get("date"),
                "value": item.get("value")
            })
        if page >= meta.get("pages", 1):
            break
        page += 1
        time.sleep(0.25)
    if not series:
        return pd.DataFrame(columns=["country_iso3","country","indicator","date","value"])
    df = pd.DataFrame(series)
    # Limpieza de tipos
    df["year"] = pd.to_numeric(df["date"], errors="coerce").astype("Int64")
    df = df.drop(columns=["date"])
    return df[["country_iso3","country","indicator","year","value"]]

def main():
    parser = argparse.ArgumentParser(description="Descarga indicadores WDI para temas Economy & Growth + External Debt (Mercosur + Chile).")
    parser.add_argument("--start-year", type=int, default=2000, help="Año inicial (default 2000)")
    parser.add_argument("--end-year", type=int, default=2024, help="Año final (default 2024)")
    parser.add_argument("--out-prefix", type=str, default="worldbank_wdi_mercosur_chile", help="Prefijo de salida")
    parser.add_argument("--sleep", type=float, default=0.1, help="Pausa entre requests (seg.)")
    args = parser.parse_args()

    start_year = args.start_year
    end_year = args.end_year
    out_prefix = args.out_prefix
    pause = args.sleep

    print("1) Listando tópicos del World Bank...")
    topics_df = list_topics()
    print(topics_df)

    # Identificar IDs de tópicos por keywords
    econ_ids = pick_topic_ids_by_keywords(topics_df, TOPIC_NAME_KEYWORDS["economy_growth"])
    debt_ids = pick_topic_ids_by_keywords(topics_df, TOPIC_NAME_KEYWORDS["external_debt"])

    if not econ_ids:
        print("⚠️ No se encontraron tópicos para 'Economy & Growth' con esas keywords. Revisa los nombres.", file=sys.stderr)
    if not debt_ids:
        print("⚠️ No se encontraron tópicos para 'External debt / Debt' con esas keywords. Revisa los nombres.", file=sys.stderr)

    topic_ids = list(dict.fromkeys(econ_ids + debt_ids))  # dedupe preservando orden

    if not topic_ids:
        print("❌ No se encontraron IDs de tópicos. Abortando.", file=sys.stderr)
        sys.exit(1)

    print(f"2) Descargando lista de indicadores para topics {topic_ids} ...")
    indicators_df = list_indicators_for_topics(topic_ids)
    indicators_df = indicators_df.rename(columns={
        "id":"indicator_code",
        "name":"indicator_name",
        "unit":"unit",
        "sourceNote":"source_note",
        "sourceOrganization":"source_org"
    })
    indicators_df["topic_ids"] = indicators_df["topic_id"]
    indicators_df = indicators_df.drop(columns=["topic_id"])
    indicators_df = indicators_df.sort_values("indicator_code").reset_index(drop=True)

    # Guardar metadatos de indicadores
    indicators_csv = f"{out_prefix}_indicators_meta.csv"
    indicators_df.to_csv(indicators_csv, index=False, encoding="utf-8")
    print(f"✅ Guardado: {indicators_csv} (n={len(indicators_df)})")

    # 3) Descargar series para TODOS los indicadores encontrados
    all_obs = []
    country_codes = list(COUNTRIES.values())

    print(f"3) Descargando series {start_year}-{end_year} para {len(indicators_df)} indicadores y países {country_codes} ...")
    for i, row in indicators_df.iterrows():
        ind = row["indicator_code"]
        try:
            obs = fetch_series(ind, country_codes, start_year, end_year)
            if not obs.empty:
                all_obs.append(obs)
        except Exception as e:
            print(f"   ⚠️ Error en indicador {ind}: {e}", file=sys.stderr)
        time.sleep(pause)  # cortesía a la API

        if (i+1) % 25 == 0:
            print(f"   Progreso: {i+1}/{len(indicators_df)} indicadores")

    if all_obs:
        observations_df = pd.concat(all_obs, ignore_index=True)
    else:
        observations_df = pd.DataFrame(columns=["country_iso3","country","indicator","year","value"])

    # Guardar observaciones en CSV y Excel
    obs_csv = f"{out_prefix}_observations.csv"
    observations_df.to_csv(obs_csv, index=False, encoding="utf-8")
    print(f"✅ Guardado: {obs_csv} (filas={len(observations_df)})")

    xlsx_path = f"{out_prefix}.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as xw:
        indicators_df.to_excel(xw, sheet_name="indicators_meta", index=False)
        observations_df.to_excel(xw, sheet_name="observations", index=False)
    print(f"✅ Guardado: {xlsx_path}")

    # Resumen rápido
    try:
        summary = (observations_df
                   .groupby(["country_iso3","indicator"], dropna=False)["value"]
                   .count()
                   .reset_index(name="num_points")
                   .sort_values(["country_iso3","num_points"], ascending=[True, False]))
        summary_csv = f"{out_prefix}_summary_counts.csv"
        summary.to_csv(summary_csv, index=False, encoding="utf-8")
        print(f"📄 Resumen de puntos por país/indicador: {summary_csv}")
    except Exception as e:
        print(f"⚠️ No se pudo generar resumen: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
