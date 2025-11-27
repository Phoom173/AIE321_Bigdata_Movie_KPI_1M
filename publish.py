# publish.py

import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# นำเข้าไลบรารีสำหรับ Google Sheets
import gspread
from gspread_dataframe import set_with_dataframe 
import numpy as np 

# --- 1. การตั้งค่าตัวแปรคงที่ (รายการตารางที่ต้องการเผยแพร่) ---
PRODUCTION_SCHEMA_NAME = 'production'

# 🚨 แก้ไขค่านี้: ลดจาก 100,000 เหลือ 50,000 เพื่อความปลอดภัยสูงสุด 🚨
ROW_LIMIT_FOR_LARGE_TABLES = 50000 
# ถ้าต้องการใช้ข้อมูลที่เยอะขึ้น ให้ลองปรับค่านี้ลง หรือ สร้าง Google Sheet ไฟล์ใหม่

# กำหนดตารางทั้งหมดที่ต้องการดึง (ใช้ Dict เพื่อเก็บชื่อตารางและชื่อ Worksheet ที่สอดคล้องกัน)
TABLES_TO_PUBLISH = {
    # 1. ตาราง Fact หลัก (ไม่แยก Genre) -> ใช้ LIMIT
    'movie_facts': 'Movie_Facts_Original', 
    
    # 2. ตาราง Fact ที่มีการแยก Genre แล้ว (สำหรับ Visualization หลัก) -> ใช้ LIMIT
    'movie_genre_fact': 'Movie_Genre_Facts', 
    
    # 3. ตารางสรุป (สำหรับตอบคำถามข้อ 1: รายได้เฉลี่ย) -> ไม่ต้องใช้ LIMIT เพราะขนาดเล็ก
    'genre_average_revenue': 'Genre_Summary'
}

# 🚨 แทนที่ด้วย File ID ที่คุณคัดลอกมา
GOOGLE_SHEET_ID = '1ZGoqwqq17L2_6ywhCK27-KsPyJ-V0xcgQfjIYblNmpw' 

# 🚨 ใช้ชื่อไฟล์มาตรฐานสำหรับ Service Account (แก้ไขชื่อตัวแปรให้ตรงกับไฟล์ที่คุณใช้)
CREDENTIALS_FILE = r'D:\AIE321\PJ\AIE321_Bigdata_Movie_KPI_1M\client_secret.json' 

# -------------------------------------------------------------
# --- ฟังก์ชันย่อย: จัดการข้อมูลและเผยแพร่ไปยัง Worksheet ---
# -------------------------------------------------------------
def prepare_and_publish_df(df, worksheet_name, gc, spreadsheet):
    """ทำความสะอาด DataFrame และเผยแพร่ไปยัง Worksheet ที่กำหนด"""
    if df.empty:
        print(f"!!! ไม่พบข้อมูลใน DataFrame สำหรับ Worksheet: {worksheet_name} !!!")
        return

    print(f"\n--- เริ่มเตรียมและเผยแพร่ข้อมูลไปยัง Worksheet: {worksheet_name} ({len(df):,} แถว) ---")

    # 🚨 ขั้นตอนการเตรียมข้อมูล: จัดการ NaN/Inf ใน DataFrame หลัก 🚨
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    # 1. จัดการคอลัมน์ Float ที่มี NaN (เพื่อหลีกเลี่ยง gspread error)
    float_cols_with_nan = df.select_dtypes(include=['float']).columns[df.select_dtypes(include=['float']).isna().any()]
    
    for col in float_cols_with_nan:
        df[col] = df[col].astype(object)
        df[col] = df[col].mask(df[col].isna(), None) 

    try:
        # หา Worksheet ถ้าไม่มีให้สร้างใหม่
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            print(f"พบ Worksheet: '{worksheet_name}'")
        except gspread.WorksheetNotFound:
            # ใช้ขนาดที่เหมาะสมในการสร้าง
            # *** ข้อควรระวัง: แม้จะใช้ limit 50,000 แต่ตอนสร้าง worksheet ต้องคำนวณ rows/cols จาก df.columns และ df.len() ที่ถูกจำกัดแล้ว
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=str(len(df) + 100), cols=str(len(df.columns) + 5))
            print(f"สร้าง Worksheet ใหม่ชื่อ '{worksheet_name}'")
            
        # 🚨 การแบ่ง Batch 🚨
        CHUNK_SIZE = 50000 
        
        # 1. ล้างข้อมูลเก่า
        worksheet.clear() 
        print(f"ล้างข้อมูลเก่าใน Worksheet '{worksheet_name}' เรียบร้อย")
        
        # 2. เขียน Header 
        header = [list(df.columns)]
        worksheet.append_rows(header, value_input_option='USER_ENTERED')

        # 3. อัปโหลดข้อมูลแบบ Batch
        total_rows = len(df)
        print(f"เริ่มอัปโหลดข้อมูล {total_rows:,} แถว แบ่งเป็น Batch ละ {CHUNK_SIZE:,} แถว...")

        for i in range(0, total_rows, CHUNK_SIZE):
            df_chunk = df.iloc[i:i + CHUNK_SIZE].copy() 
            data_to_send = df_chunk.values.tolist()
            
            print(f"-> กำลังอัปโหลด Batch {i//CHUNK_SIZE + 1} ({len(df_chunk):,} แถว)...")
            worksheet.append_rows(data_to_send, value_input_option='USER_ENTERED')
            
        print(f"*** เผยแพร่ข้อมูล {total_rows:,} แถวไปยัง Worksheet: {worksheet_name} เสร็จสิ้น ***")

    except Exception as e:
        print(f"!!! เกิดข้อผิดพลาดในการเผยแพร่ไปยัง Worksheet {worksheet_name}: {e}")
        # ใช้ raise เพื่อให้ Exception ถูกส่งต่อและแจ้งในส่วนควบคุมหลัก
        raise

# -------------------------------------------------------------
# --- ฟังก์ชันหลักในการรัน Pipeline ---
# -------------------------------------------------------------
def run_publication_pipeline():
    load_dotenv()
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_PORT = os.getenv("DB_PORT")
    
    # -------------------------------------------------------------
    # --- 1. การเชื่อมต่อฐานข้อมูลและดึงข้อมูลทั้ง 3 ตาราง ---
    # -------------------------------------------------------------
    data_to_publish = {}
    
    try:
        conn_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(conn_string)

        for table_name, worksheet_name in TABLES_TO_PUBLISH.items():
            print(f"\n--- เริ่มดึงข้อมูลจาก {PRODUCTION_SCHEMA_NAME}.{table_name} ---")
            
            limit_clause = ""
            # 🚨 ใช้ LIMIT เฉพาะตารางที่มีขนาดใหญ่เท่านั้น 🚨
            if table_name in ['movie_facts', 'movie_genre_fact']:
                limit_clause = f" LIMIT {ROW_LIMIT_FOR_LARGE_TABLES}"
                print(f"**ใช้ LIMIT {ROW_LIMIT_FOR_LARGE_TABLES:,} แถว เนื่องจากขนาดข้อมูลใหญ่มาก**") 
                
            sql_query = f"SELECT * FROM {PRODUCTION_SCHEMA_NAME}.{table_name}{limit_clause};"
            
            with engine.connect() as conn:
                raw_conn = conn.connection
                df = pd.read_sql(sql_query, con=raw_conn)
            
            print(f"[SUCCESS] ดึงตาราง {table_name} ได้ {len(df):,} แถว")
            data_to_publish[worksheet_name] = df 

    except Exception as e:
        print(f"!!! Error: ไม่สามารถเชื่อมต่อ DB หรือดึงข้อมูลได้ !!!")
        print(f"สาเหตุ: {e}")
        return 

    # -------------------------------------------------------------
    # --- 2. การเชื่อมต่อ Google Sheets และเผยแพร่ข้อมูลทั้งหมด ---
    # -------------------------------------------------------------
    try:
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        print(f"\n[SUCCESS] เชื่อมต่อ Google Sheets สำเร็จด้วย Service Account.") 
        
        spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID) 
        print(f"[SUCCESS] พบ Spreadsheet ด้วย ID: {GOOGLE_SHEET_ID}")
        
        # วนลูปเพื่อเผยแพร่ข้อมูลทุกตารางที่ดึงมา
        for worksheet_name, df in data_to_publish.items():
            prepare_and_publish_df(df, worksheet_name, gc, spreadsheet)

        print(f"\n=========================================================================")
        print(f"*** ไปป์ไลน์เผยแพร่ข้อมูลทั้งหมด {len(TABLES_TO_PUBLISH)} ตารางสำเร็จ! ***")
        print(f"ลิงก์ Spreadsheet: {spreadsheet.url}")
        print(f"=========================================================================")
        
    except FileNotFoundError:
        print(f"!!! ERROR: ไม่พบไฟล์ {CREDENTIALS_FILE} โปรดตรวจสอบพาธ !!!")
    except gspread.SpreadsheetNotFound:
        print(f"!!! Error: ไม่พบ Spreadsheet ด้วย ID นี้ ({GOOGLE_SHEET_ID})")
    except Exception as e:
        # หากเกิด APIError ที่เกี่ยวกับ Cell Limit ซ้ำอีกครั้ง จะถูกจับที่นี่
        print(f"เกิดข้อผิดพลาดในการเชื่อมต่อ/เผยแพร่ Google Sheets โดยรวม: {e}")

if __name__ == '__main__':
    run_publication_pipeline()