import pandas as pd
import json
import os
import glob
from datetime import datetime
from pathlib import Path
import io

def saveAndAppend(json_str: str, base_dir: str = "apps/data/result"):
    try:
        today_date = datetime.now().strftime("%Y-%m-%d")
        now_full   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        Path(base_dir).mkdir(parents=True, exist_ok=True)

        existing_json    = glob.glob(f"{base_dir}/data_{today_date}_*.json")
        existing_parquet = glob.glob(f"{base_dir}/data_{today_date}_*.parquet")

        old_json_path    = existing_json[0]    if existing_json    else None
        old_parquet_path = existing_parquet[0] if existing_parquet else None

        new_json_path    = f"{base_dir}/data_{now_full}.json"
        new_parquet_path = f"{base_dir}/data_{now_full}.parquet"

        df_new = pd.read_json(io.StringIO(json_str))

        if df_new.empty:
            return True

        for col in df_new.columns:
            if df_new[col].dtype == 'object':
                df_new[col] = df_new[col].astype(str).tolist()

        new_data_list = json.loads(df_new.to_json(orient='records'))

        existing_data = []
        if old_json_path and os.path.exists(old_json_path):
            try:
                with open(old_json_path, 'r') as f:
                    existing_data = json.load(f)
            except json.JSONDecodeError:
                existing_data = []

        existing_data.extend(new_data_list)

        with open(new_json_path, 'w') as f:
            json.dump(existing_data, f, indent=4)

        if old_json_path and old_json_path != new_json_path:
            os.remove(old_json_path)

        try:
            if old_parquet_path and os.path.exists(old_parquet_path):
                df_existing = pd.read_parquet(old_parquet_path)
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = df_new

            df_combined.to_parquet(new_parquet_path, engine='pyarrow', index=False)

            if old_parquet_path and old_parquet_path != new_parquet_path:
                os.remove(old_parquet_path)

        except Exception as pq_err:
            print(f"\n[INFO] File Parquet lama bermasalah ({pq_err}).")
            print("[INFO] Memaksa menimpa file Parquet dengan data baru...")
            df_new.to_parquet(new_parquet_path, engine='pyarrow', index=False)
            if old_parquet_path and os.path.exists(old_parquet_path):
                os.remove(old_parquet_path)

        print(f"Data SUKSES tersimpan ke: {new_json_path} & {new_parquet_path}")
        return True

    except Exception as e:
        print(f"Fatal Error saat append file: {e}")
        return False