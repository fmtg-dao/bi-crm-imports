from config import load_mysql_config
from mysql_client import MySQLClient
import pandas as pd
import re

DATE_FIELDS = {
        'modified'
    }


def clean_column_name(col: str) -> str:
    col = col.strip().lower()
    col = re.sub(r"\s+", "_", col)          # Replace whitespace with _
    col = re.sub(r"[^a-zA-Z0-9_]", "", col) # Remove special characters
    col = re.sub(r"_+", "_", col)           # Replace multiple _ with single _
    col = col.strip("_")                    # Remove leading/trailing _
    return col

def save_object_in_mysqL(local_path:str, table_name:str, if_exists:str='replace', delimiter: str = ","):

    df = pd.read_csv(local_path, dtype=str, keep_default_na=False, sep=delimiter)

    df.columns = [clean_column_name(col) for col in df.columns]

  
    for col in df.columns:
        if col in DATE_FIELDS:
            df[col] = pd.to_datetime(df[col], utc=True, format="ISO8601")
        else:
            df[col] = df[col].astype("string")

    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)


    db.create_table_from_df(df, table_name, if_exists)


if __name__ == "__main__":

    # path = 'local_data/csv/resid_numid_mapping.csv'
    # save_object_in_mysqL(path, 'mig_resid_numid_mapping')

    # path = 'local_data/csv/entra_id_80k.csv'
    # save_object_in_mysqL(path, 'mig_loyality_entra_id')

    path = 'local_data/csv/20260528_all_investors.csv'
    save_object_in_mysqL(path, 'stg_imp_invest_20260519', delimiter=";")