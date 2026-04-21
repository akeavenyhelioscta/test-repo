from datetime import datetime, timedelta
from io import StringIO

import pandas as pd


def parse_content(
        content: str,
        region: str,
        metric: str,
        mode: str,
        logger,
    ) -> pd.DataFrame:

    lines = content.strip().split("\n")

    metadata = lines[0]
    logger.info(f"\tMetadata: {metadata}")
    parts = metadata.split(",")
    forecast_info = parts[1]
    date_str = forecast_info.replace("Forecast Made ", "").strip()
    forecast_datetime = datetime.strptime(date_str, "%b %d %Y %H%M %Z")

    df = pd.read_csv(StringIO(content), delimiter=",", skiprows=2, on_bad_lines="skip")

    new_columns: list[str] = []
    for col in df.columns:
        metric_name = None

        if mode == "average" and "Average" in col:
            metric_name = "Average"
        elif mode == "hddcdd" and ("HDD" in col or "CDD" in col):
            metric_name = "HDD" if "HDD" in col else "CDD"
        elif mode == "minmax" and ("Min" in col or "Max" in col or "HDD" in col):
            metric_name = "Min" if "Min" in col else "Max"

        if metric_name is None:
            new_columns.append(col)
            continue

        if "." in col:
            day_offset = int(col.split(".")[1])
        else:
            day_offset = 0
        forecast_date = (forecast_datetime.date() + timedelta(days=day_offset)).strftime("%Y-%m-%d")
        new_columns.append(f"{metric_name} - {forecast_date}")

    df.columns = new_columns

    rename_map = {"City:": "wsi_forecast_table_city"}
    if mode == "minmax":
        rename_map.update({"(F)": "min_normals", "(F).1": "max_normals"})
    else:
        rename_map.update({"(F)": "normals"})
    df = df.rename(columns=rename_map)

    df["forecast_datetime"] = forecast_datetime
    df["region"] = region
    df["station_name"] = df["wsi_forecast_table_city"].str.split("(").str[0].str.strip()
    df["station_name"] = df["station_name"].str.replace(r"\s+[A-Z]{2}$", "", regex=True)
    df["site_id"] = df["wsi_forecast_table_city"].str.split("(").str[1].str.split(")").str[0]
    df.loc[df["site_id"].str.startswith("Aggregate"), "site_id"] = region

    return df


def pivot_forecast(
        df: pd.DataFrame,
        normal_columns: list[str],
    ) -> pd.DataFrame:

    primary_keys = [
        "forecast_datetime",
        "wsi_forecast_table_city",
        "region",
        "site_id",
        "station_name",
    ] + normal_columns
    cols = [col for col in df.columns if col not in primary_keys]

    df_melt = df.melt(
        id_vars=primary_keys,
        value_vars=cols,
        var_name="metric_and_forecast_date",
        value_name="value",
    )

    df_melt[["metric_type", "forecast_date"]] = df_melt["metric_and_forecast_date"].str.split(" - ", expand=True)
    df_melt = df_melt.drop(columns=["metric_and_forecast_date"])

    for col in normal_columns + ["value"]:
        df_melt[col] = pd.to_numeric(df_melt[col], errors="coerce").fillna(0)

    index_cols = [
        "forecast_datetime",
        "forecast_date",
        "wsi_forecast_table_city",
        "region",
        "site_id",
        "station_name",
    ] + normal_columns

    df_pivot = df_melt.pivot_table(
        index=index_cols,
        columns="metric_type",
        values="value",
        aggfunc="first",
    ).reset_index()

    df_pivot.columns.name = None
    return df_pivot


def format_forecast(
        df: pd.DataFrame,
        primary_keys: list[str],
    ) -> pd.DataFrame:

    df.columns = df.columns.str.lower().str.replace(" ", "_").str.rstrip("_")

    other_columns = [col for col in df.columns if col not in primary_keys]
    df = df[primary_keys + other_columns]

    df["forecast_datetime"] = pd.to_datetime(df["forecast_datetime"], errors="coerce")
    df["forecast_date"] = pd.to_datetime(df["forecast_date"], errors="coerce").dt.date

    for col in other_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    return df
