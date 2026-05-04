from config import load_mysql_config
from mysql_client import MySQLClient
import pandas as pd

DATE_FIELDS = {
        'modified'
    }


def save_object_in_mysqL(local_path:str, table_name:str, if_exists:str='replace'):

    df = pd.read_csv(local_path, dtype=str, keep_default_na=False)

  
    for col in df.columns:
        if col in DATE_FIELDS:
            df[col] = pd.to_datetime(df[col], utc=True, format='ISO8601')
        else:
            df[col] = df[col].astype('string')

    cfg_mysql = load_mysql_config()
    db = MySQLClient(cfg_mysql)

    db.create_table_from_df(df, table_name, if_exists)


if __name__ == "__main__":

    # path = 'local_data/csv/resid_numid_mapping.csv'
    # save_object_in_mysqL(path, 'mig_resid_numid_mapping')

    # path = 'local_data/csv/entra_id_80k.csv'
    # save_object_in_mysqL(path, 'mig_loyality_entra_id')

    path = 'local_data/csv/Entra_Import_Program_Members_part2_results.csv'
    save_object_in_mysqL(path, 'mig_loyality_entra_id_2')