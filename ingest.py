import pandas as pd
from sqlalchemy import create_engine, text 
from sqlalchemy.exc import OperationalError
import psycopg2 
from io import StringIO
from pandas.io import sql as pd_sql 

# --- 1. ตั้งค่าการเชื่อมต่อฐานข้อมูล (Database Connection Setup) ---
DB_USER = 'DB_AIE321_BIG_DATA'
DB_PASSWORD = '321bigdatawork'
DB_HOST = 'localhost' 
DB_PORT = '6666'      
DB_NAME = 'AIE321' 

# ตั้งค่าปลายทาง: 🚨 ใช้ชื่อเดิม (ตัวพิมพ์เล็ก) แต่ตอนนี้จะถูกอ้างอิงอย่างถูกต้อง
SCHEMA_NAME = 'raw_data' 
TABLE_NAME = 'tmdb_movies_raw' 
CSV_FILE_PATH = r'D:/AIE321/PJ/AIE321_Bigdata_Movie_KPI_1M/TMDB_movies.csv' 

# ใช้ชื่อตารางเดียว: Unquoted Name (ใช้ในการแสดงผลและ drop table)
FULL_TABLE_NAME = f"{SCHEMA_NAME}.{TABLE_NAME}"

def create_raw_schema_and_table(engine, df):
    """ฟังก์ชันสำหรับสร้าง Schema และตารางเปล่าด้วยคำสั่ง SQL ตรง"""
    try:
        # 1. สร้าง Schema
        with engine.begin() as conn: 
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")) 
            print(f"[SUCCESS] Schema '{SCHEMA_NAME}' checked/created.") 
        
        # 2. ใช้ Pandas สร้างคำสั่ง CREATE TABLE (DDL)
        table_ddl = pd_sql.get_schema(
            df.head(0),
            name=TABLE_NAME, 
            con=engine, 
            keys=None, 
            schema=SCHEMA_NAME
        )
        
        # 3. ลบตารางเก่า และสร้างตารางใหม่ด้วย DDL ที่สร้างขึ้น
        with engine.begin() as conn:
            # ลบตารางเก่าก่อน (ใช้ชื่อ UNQUOTED)
            conn.execute(text(f"DROP TABLE IF EXISTS {FULL_TABLE_NAME} CASCADE"))
            # สร้างตารางใหม่ (DDL นี้จะมี Quoting อยู่ภายในแล้ว)
            conn.execute(text(table_ddl))
        
        print(f"[SUCCESS] Table structure '{FULL_TABLE_NAME}' created using DDL.")
        
    except Exception as e:
        print(f"[ERROR] Failed to create Schema or Table structure: {e}")
        raise 

def ingest_data():
    """ฟังก์ชันหลักในการดึงและโหลดข้อมูลโดยใช้ Bulk Copy (COPY_EXPERT)"""
    # Connection string สำหรับ psycopg2
    conn_string = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"
    
    try:
        # 1. สร้าง Engine สำหรับการเชื่อมต่อ SQLAlchemy
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

        # 2. อ่านไฟล์ CSV ด้วย Pandas
        print(f"Reading CSV file: {CSV_FILE_PATH}...")
        df = pd.read_csv(CSV_FILE_PATH, low_memory=False) 
        print(f"Read data complete. Rows: {len(df):,}")
        
        # 3. สร้าง Schema และโครงสร้างตาราง
        create_raw_schema_and_table(engine, df)

        # 4. โหลดข้อมูลเข้า PostgreSQL ด้วย Bulk Copy
        print(f"Loading data into table {FULL_TABLE_NAME} using Bulk Copy (COPY EXPERT)...")
        
        buffer = StringIO()
        # ใช้ tab (\t) เป็นตัวคั่น
        df.to_csv(buffer, index=False, header=False, sep='\t', encoding='utf-8') 
        buffer.seek(0)
        
        # 5. เชื่อมต่อด้วย psycopg2 โดยตรงเพื่อใช้ copy_expert
        # COPY_EXPERT ช่วยให้เราควบคุมการ Quoting ในคำสั่ง SQL ได้อย่างแม่นยำ
        copy_command = f"""COPY "{SCHEMA_NAME}"."{TABLE_NAME}" FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t')"""
        
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                # 🚨 ใช้ copy_expert และใส่ Double Quotes (") ครอบชื่อ Schema/Table
                cursor.copy_expert(copy_command, buffer) 
            conn.commit() 
        
        print(f"[SUCCESS] Bulk Copy to PostgreSQL complete. Table: {FULL_TABLE_NAME}")

    except FileNotFoundError:
        print(f"[ERROR] CSV not found at: {CSV_FILE_PATH}. Check file name and path.")
    except OperationalError as e:
        print(f"[ERROR] Connection Refused (OperationalError) Check:\n- Is Docker Container 'db' running?\n- Is Host Port (6666) free or mapped correctly?")
    except Exception as e:
        print(f"[ERROR] An unknown error occurred during ingestion: {e}")

if __name__ == "__main__":
    ingest_data()