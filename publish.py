# publish.py

import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# นำเข้าไลบรารีสำหรับ Google Sheets
import gspread
from gspread_dataframe import set_with_dataframe 
import numpy as np 

# --- 1. การตั้งค่าตัวแปรคงที่ ---
PRODUCTION_TABLE_NAME = 'movie_facts' 
PRODUCTION_SCHEMA_NAME = 'production'
# GOOGLE_SHEET_TITLE = 'Kaggle Data Pipeline Report'  <--- (ไม่ต้องใช้แล้ว)
WORKSHEET_NAME = 'Final Data' 

# 🚨 แทนที่ด้วย File ID ที่คุณคัดลอกมา
GOOGLE_SHEET_ID = '1ZGoqwqq17L2_6ywhCK27-KsPyJ-V0xcgQfjIYblNmpw' 

# 🚨 ใช้ชื่อไฟล์มาตรฐานสำหรับ Service Account (แก้ไขชื่อตัวแปรให้ตรงกับไฟล์ที่คุณใช้)
CREDENTIALS_FILE = 'client_secret.json' 

def run_publication_pipeline():
    # 1. โหลดตัวแปรสภาพแวดล้อมจาก .env
    load_dotenv()
    # ดึงค่าการเชื่อมต่อจาก .env 
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_PORT = os.getenv("DB_PORT")

    # --- 2. การเชื่อมต่อฐานข้อมูล (ใช้โค้ดที่แก้ไขล่าสุด) ---
    try:
        conn_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(conn_string)

        print(f"--- เริ่มดึงข้อมูลจาก {PRODUCTION_SCHEMA_NAME}.{PRODUCTION_TABLE_NAME} (Host: {DB_HOST}:{DB_PORT}) ---")
        
        sql_query = f"SELECT * FROM {PRODUCTION_SCHEMA_NAME}.{PRODUCTION_TABLE_NAME};"
        
        # ใช้ Connection object ที่เป็นพื้นฐาน (DBAPI Connection) 
        with engine.connect() as conn:
            raw_conn = conn.connection
            final_df = pd.read_sql(sql_query, con=raw_conn)
        
        print(f"ดึงข้อมูลพร้อมเผยแพร่มาได้ {len(final_df)} แถว")

    except Exception as e:
        print(f"!!! Error: ไม่สามารถเชื่อมต่อ DB หรือดึงข้อมูลได้ !!!")
        print(f"สาเหตุ: {e}")
        return 

    # --- 3. การเผยแพร่ไปยัง Google Sheets (แก้ไขการจัดการข้อมูล Batch ขั้นสุดท้าย) ---
    if final_df.empty:
        print("!!! ไม่พบข้อมูลในตาราง Production ไม่สามารถเผยแพร่ได้ !!!")
        return

    # 🚨 ขั้นตอนการเตรียมข้อมูล: จัดการ NaN/Inf ใน DataFrame หลัก 🚨
    # 1. จัดการค่า Inf/-Inf โดยเปลี่ยนให้เป็น NaN
    final_df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # 2. ค้นหาคอลัมน์ที่เป็น float และมีค่า NaN (เพื่อทำการแปลง)
    # เราต้องแปลงคอลัมน์ที่เป็น float และมี NaN เป็น dtype object ก่อน
    float_cols_with_nan = final_df.select_dtypes(include=['float']).columns[final_df.select_dtypes(include=['float']).isna().any()]
    
    # 3. แปลง float columns ที่มี NaN ให้เป็น object และแทนที่ NaN ด้วย None
    for col in float_cols_with_nan:
        final_df[col] = final_df[col].astype(object)
        
        # 🚨 การแก้ไข ERROR: ใช้ .isna().mask() 🚨
        # แทนที่ค่าที่เป็น NaN ในคอลัมน์ด้วย None
        final_df[col] = final_df[col].mask(final_df[col].isna(), None) # 👈 ใช้เมธอด .isna() บน Series โดยตรง
    
    # สำหรับคอลัมน์ float ที่ไม่มี NaN เราไม่ต้องทำอะไร เพราะมันจะแปลงเป็นตัวเลขใน JSON ได้

    print(f"--- เริ่มเผยแพร่ข้อมูลไปยัง Google Sheets: {GOOGLE_SHEET_ID} (แบ่งเป็น Batch) ---")
    
    try:
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        print(f"เชื่อมต่อสำเร็จด้วย Service Account.") 
        
        try:
            spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID) 
            print(f"พบ Spreadsheet ด้วย ID: {GOOGLE_SHEET_ID}")
        except gspread.SpreadsheetNotFound:
            print(f"!!! Error: ไม่พบ Spreadsheet ด้วย ID นี้ ({GOOGLE_SHEET_ID})")
            print("โปรดตรวจสอบ ID และยืนยันว่าได้แชร์สิทธิ์ Editor ให้ Service Account แล้ว")
            return
            
        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows="100", cols="20")
            print(f"สร้าง Worksheet ใหม่ชื่อ '{WORKSHEET_NAME}'")
        
        
        # 🚨 การแบ่ง Batch 🚨
        CHUNK_SIZE = 50000 
        
        # 1. ล้างข้อมูลเก่า
        worksheet.clear() 
        print("ล้างข้อมูลเก่าใน Worksheet เรียบร้อย")
        
        # 2. เขียน Header 
        header = [list(final_df.columns)]
        worksheet.append_rows(header, value_input_option='USER_ENTERED')
        print("เขียน Header เรียบร้อย")

        # 3. อัปโหลดข้อมูลแบบ Batch
        total_rows = len(final_df)
        print(f"เริ่มอัปโหลดข้อมูล {total_rows} แถว แบ่งเป็น Batch ละ {CHUNK_SIZE} แถว...")

        for i in range(0, total_rows, CHUNK_SIZE):
            df_chunk = final_df.iloc[i:i + CHUNK_SIZE].copy() 
            
            # เนื่องจากเราจัดการ NaN/Inf ใน final_df ก่อนเข้าลูปแล้ว 
            # ตอนนี้เราสามารถแปลงเป็น list ได้โดยตรง
            data_to_send = df_chunk.values.tolist()
            
            print(f"-> กำลังอัปโหลด Batch {i//CHUNK_SIZE + 1} ({len(df_chunk):,} แถว)...")
            
            # ใช้ append_rows เพื่อแทรกข้อมูลต่อจากแถวเดิม
            worksheet.append_rows(data_to_send, value_input_option='USER_ENTERED')
            
        print(f"*** เผยแพร่ข้อมูล {total_rows} แถวไปยัง ID: {GOOGLE_SHEET_ID} เสร็จสิ้น ***")
        print(f"ลิงก์ Spreadsheet: {spreadsheet.url}")
        
    except FileNotFoundError:
        print(f"!!! ERROR: ไม่พบไฟล์ {CREDENTIALS_FILE} โปรดตรวจสอบพาธ !!!")
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการเชื่อมต่อ/เผยแพร่ Google Sheets: {e}")
        print("โปรดตรวจสอบว่าได้แชร์ Spreadsheet ให้กับอีเมล Service Account แล้ว")


if __name__ == '__main__':
    run_publication_pipeline()