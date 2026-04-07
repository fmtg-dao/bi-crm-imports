from typing import Any, Dict, List, Optional, Sequence
from contextlib import contextmanager
import mysql.connector
import pandas as pd
from mysql.connector import pooling
import json
import math
import numpy as np
from config import MySQLConfig


class MySQLClient:
    def __init__(self, cfg: MySQLConfig) -> None:
        self._pool = pooling.MySQLConnectionPool(
            pool_name="mysql_pool",
            pool_size=cfg.pool_size,
            host=cfg.host,
            user=cfg.user,
            password=cfg.password,
            database=cfg.database,
            port=cfg.port,
            autocommit=False,
        )

    @contextmanager
    def connection(self):
        conn = self._pool.get_connection()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self):
        with self.connection() as conn:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def fetch_all(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> List[Dict[str, Any]]:
        with self.connection() as conn:
            cur = conn.cursor(dictionary=True)
            try:
                cur.execute(sql, params)
                return cur.fetchall()
            finally:
                cur.close()

    def fetch_one(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        with self.connection() as conn:
            cur = conn.cursor(dictionary=True)
            try:
                cur.execute(sql, params)
                return cur.fetchone()
            finally:
                cur.close()


    def fetch_df(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> pd.DataFrame:
        rows = self.fetch_all(sql, params)
        return pd.DataFrame(rows)

    def execute(
        self,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        *,
        conn=None,
    ) -> int:
        owns_conn = conn is None
        if owns_conn:
            conn = self._pool.get_connection()

        try:
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
                return cur.rowcount
            finally:
                cur.close()
        finally:
            if owns_conn:
                conn.commit()
                conn.close()


    def create_table_from_df(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "fail",  # fail | replace | append
        *,
        conn=None,
        chunk_size: int = 1000,
    ) -> int:
        if df.empty:
            raise ValueError("DataFrame is empty. Nothing to create.")

        # Clean / normalize column names
        df = df.copy()
        df.columns = [str(col).strip() for col in df.columns]

        owns_conn = conn is None
        if owns_conn:
            conn = self._pool.get_connection()

        cur = None
        try:
            cur = conn.cursor()

            def map_dtype(series: pd.Series) -> str:
                dtype = series.dtype

                if pd.api.types.is_bool_dtype(dtype):
                    return "BOOLEAN"

                if pd.api.types.is_integer_dtype(dtype):
                    return "BIGINT"

                if pd.api.types.is_float_dtype(dtype):
                    return "DOUBLE"

                if pd.api.types.is_datetime64_any_dtype(dtype):
                    return "DATETIME"

                return "TEXT"

            if if_exists == "replace":
                cur.execute(f"DROP TABLE IF EXISTS `{table_name}`")
            elif if_exists == "fail":
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                      AND table_name = %s
                    """,
                    (table_name,),
                )
                if cur.fetchone()[0] > 0:
                    raise ValueError(f"Table `{table_name}` already exists.")
            elif if_exists != "append":
                raise ValueError("if_exists must be one of: 'fail', 'replace', 'append'")

            if if_exists in ("fail", "replace"):
                cols = []
                for col in df.columns:
                    mysql_type = map_dtype(df[col])
                    cols.append(f"`{col}` {mysql_type}")

                create_sql = f"""
                    CREATE TABLE `{table_name}` (
                        {', '.join(cols)}
                    )
                """
                cur.execute(create_sql)

            placeholders = ", ".join(["%s"] * len(df.columns))
            col_names = ", ".join([f"`{c}`" for c in df.columns])

            insert_sql = f"""
                INSERT INTO `{table_name}` ({col_names})
                VALUES ({placeholders})
            """

            # Force object dtype first, then replace all missing values with None
            df_clean = df.copy().astype(object)
            df_clean = df_clean.where(pd.notna(df_clean), None)

            def normalize_value(value):
                if value is None:
                    return None

                if value is pd.NaT:
                    return None

                if isinstance(value, float) and math.isnan(value):
                    return None

                if isinstance(value, np.floating) and np.isnan(value):
                    return None

                if isinstance(value, pd.Timestamp):
                    return value.to_pydatetime()

                if isinstance(value, np.datetime64):
                    ts = pd.Timestamp(value)
                    return None if pd.isna(ts) else ts.to_pydatetime()

                if isinstance(value, (dict, list)):
                    return json.dumps(value, ensure_ascii=False)

                if isinstance(value, np.generic):
                    value = value.item()
                    if isinstance(value, float) and math.isnan(value):
                        return None

                return value

            for col in df_clean.columns:
                df_clean[col] = pd.Series(
                    [normalize_value(v) for v in df_clean[col].tolist()],
                    dtype=object,
                )

            data = [tuple(row) for row in df_clean.itertuples(index=False, name=None)]

            # Debug guard against any remaining nan values
            for row_idx, row in enumerate(data):
                for col_idx, value in enumerate(row):
                    if isinstance(value, float) and math.isnan(value):
                        raise ValueError(
                            f"Unconverted float nan found at row {row_idx}, column {df.columns[col_idx]}"
                        )
                    if isinstance(value, np.floating) and np.isnan(value):
                        raise ValueError(
                            f"Unconverted numpy nan found at row {row_idx}, column {df.columns[col_idx]}"
                        )

            total_inserted = 0
            for start in range(0, len(data), chunk_size):
                batch = data[start:start + chunk_size]
                cur.executemany(insert_sql, batch)
                total_inserted += len(batch)

            if owns_conn:
                conn.commit()

            return total_inserted

        except Exception:
            if owns_conn:
                conn.rollback()
            raise
        finally:
            if cur is not None:
                cur.close()
            if owns_conn:
                conn.close()