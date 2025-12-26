# ==========================================================================================
# ============= Stored Procedure: Load Bronze Layer (Source -> Bronze) =====================
# ==========================================================================================

# ==> This stored procedure loads data into the "bronze" schema from external CSV files and
#     you don't need change consult permitions of any file.
# ==> It performs the following actions:
#       - Truncate the bronze tables before loading data.
#       - Uses the 'COPY' command to load data from CSV file to bronze tables.
#
# ==> Usage Example: You need to run in the root project path like this:
#        python3 ./scripts/bronze/load_data_bronzelayer.py
#  =========================================================================================

import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# == Configuration ==
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"), # Change for os.getenv()
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

list_CSV_PATH = ["data_sets/source_crm/cust_info.csv",
                 "data_sets/source_crm/prd_info.csv",
                 "data_sets/source_crm/sales_details.csv",
                 "data_sets/source_erp/CUST_AZ12.csv",
                 "data_sets/source_erp/LOC_A101.csv",
                 "data_sets/source_erp/PX_CAT_G1V2.csv"]

list_TABLE_NAME = ["bronze.crm_cust_info",
                   "bronze.crm_prd_info",
                   "bronze.crm_sales_details",
                   "bronze.erp_cust_az12",
                   "bronze.erp_loc_a101",
                   "bronze.erp_px_cat_g1v2"]

def log_to_db(cursor, table, status, msg, start_time, log_id=None):
    """Función auxiliar para escribir en la tabla de logs"""
    if log_id is None:
        # Insertar nuevo log (Inicio)
        query = """
            INSERT INTO bronze.load_logs (table_name, start_time, status)
            VALUES (%s, %s, %s) RETURNING log_id;
        """
        cursor.execute(query, (table, start_time, status))
        return cursor.fetchone()[0]
    else:
        # Actualizar log existente (Fin)
        query = """
            UPDATE bronze.load_logs 
            SET end_time = %s, status = %s, error_message = %s 
            WHERE log_id = %s;
        """
        cursor.execute(query, (datetime.now(), status, msg, log_id))

def run_etl(table, csv_path):
    print(f"==== Initializing Load in table: {table} ---")
    
    conn = None
    log_id = None
    start_time = datetime.now()

    try:
        # === Create the connection
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # === Truncate the table
        print("Truncating the table...")
        cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY;")
        
        # === Load CSV info
        print("Loading the CSV file data...")
        with open(csv_path, 'r') as f:
            # copy_expert permite pasar el archivo abierto directamente (f)
            sql_copy = f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER)"
            cur.copy_expert(sql_copy, f)

        conn.commit()
        print("======= Success Load =======")

    except Exception as e:
        print(f"❌ Detected Error: {e}")
        if conn:
            conn.rollback() # Revert the step because we dont want incomplete info
            
    finally:
        if conn:
            conn.close()
            end_time = datetime.now()

            time_spend = end_time-start_time
            print("Elapsed Time: ", time_spend, end="\n\n")

def main():
    print("==========================================")
    print("   STARTING BRONZE LAYER INGESTION")
    print("==========================================\n")

    whole_start_time = datetime.now()
    for csv_path, table in zip(list_CSV_PATH, list_TABLE_NAME):
        run_etl(table=table, csv_path=csv_path)

    whole_end_time = datetime.now()
    whole_spend_time = whole_end_time - whole_start_time
    
    print("==========================================")
    print(f"   ALL PROCESSES FINISHED")
    print(f"   Total Time: {whole_spend_time}")
    print("==========================================")

if __name__ == "__main__":
    main()