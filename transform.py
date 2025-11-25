import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import json
from ast import literal_eval
import numpy as np
# 🚨 ใช้ psycopg2 และ StringIO สำหรับ Bulk Copy
import psycopg2 
from io import StringIO

# --- 1. ตั้งค่าการเชื่อมต่อฐานข้อมูล (ใช้ค่าเดียวกัน) ---
DB_USER = 'DB_AIE321_BIG_DATA'
DB_PASSWORD = '321bigdatawork'
DB_HOST = 'localhost' 
DB_PORT = '6666'      
DB_NAME = 'AIE321' 

# ตั้งค่า Schema และ Table
RAW_SCHEMA = 'raw_data'
RAW_TABLE = 'tmdb_movies_raw'
PRODUCTION_SCHEMA = 'production'
MOVIE_FACTS_TABLE = 'movie_facts'
GENRE_SUMMARY_TABLE = 'genre_average_revenue'

# Connection string สำหรับ psycopg2 โดยตรง
CONN_STRING = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"
FULL_FACTS_TABLE = f'"{PRODUCTION_SCHEMA}"."{MOVIE_FACTS_TABLE}"'
FULL_GENRE_TABLE = f'"{PRODUCTION_SCHEMA}"."{GENRE_SUMMARY_TABLE}"'

# --- 2. ฟังก์ชันช่วยในการจัดการ JSON/Array ---
def parse_and_extract_names(json_string):
    """แปลง JSON string เป็น List ของชื่อ (e.g., genre names)"""
    if pd.isna(json_string) or json_string == '[]' or json_string == '':
        return []
    try:
        list_of_dicts = literal_eval(json_string)
        if isinstance(list_of_dicts, list) and all(isinstance(d, dict) for d in list_of_dicts):
            return [d.get('name') or d.get('iso_3166_1', 'Unknown') for d in list_of_dicts]
    except (ValueError, SyntaxError):
        pass
    return []

# --- 3. ฟังก์ชันช่วยในการสร้างตารางและ Bulk Copy (ใช้ในการเขียนข้อมูลกลับ) ---
def create_table_and_bulk_copy(engine, conn_string, df, table_name_unquoted, table_name_quoted, schema_name):
    """สร้างตารางเปล่าและโหลดข้อมูลด้วย COPY EXPERT"""
    from pandas.io import sql as pd_sql 
    
    try:
        # A. ใช้ Pandas สร้างคำสั่ง CREATE TABLE (DDL)
        table_ddl = pd_sql.get_schema(
            df.head(0),
            name=table_name_unquoted, 
            con=engine, 
            keys=None, 
            schema=schema_name
        )
        
        # B. ลบตารางเก่าและสร้างตารางใหม่
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{table_name_unquoted} CASCADE"))
            conn.execute(text(table_ddl))
        
        # C. โหลดข้อมูลด้วย Bulk Copy
        buffer = StringIO()
        # ใช้ tab (\t) เป็นตัวคั่น
        df.to_csv(buffer, index=False, header=False, sep='\t', encoding='utf-8') 
        buffer.seek(0)
        
        copy_command = f"""COPY {table_name_quoted} FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t')"""
        
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                cursor.copy_expert(copy_command, buffer) 
            conn.commit() 
        
        return True

    except Exception as e:
        print(f"[ERROR] Bulk Copy to {schema_name}.{table_name_unquoted} Failed: {e}")
        raise


# --- 4. ฟังก์ชันหลักในการแปลงข้อมูล (The Refinery) ---
def transform_data():
    try:
        # 1. สร้าง Engine สำหรับการจัดการ DDL (Create Table, Drop Table)
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        
        # 2. สร้าง Schema Production (ถ้ายังไม่มี)
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {PRODUCTION_SCHEMA}"))
            print(f"[SUCCESS] Schema '{PRODUCTION_SCHEMA}' ถูกสร้างเรียบร้อยแล้ว")
        
        # 3. อ่านข้อมูลดิบ (ใช้ psycopg2 ในการอ่าน)
        print(f"กำลังอ่านข้อมูลดิบจาก {RAW_SCHEMA}.{RAW_TABLE} โดยใช้ psycopg2...")
        
        with psycopg2.connect(CONN_STRING) as conn:
            query = f"SELECT * FROM {RAW_SCHEMA}.{RAW_TABLE}"
            df = pd.read_sql_query(query, con=conn) 
        
        print(f"[SUCCESS] อ่านข้อมูลเสร็จสิ้น จำนวนแถว: {len(df):,}")
        
        # --- 4. Data Cleaning และ Feature Engineering ---
        print("กำลังแปลงคอลัมน์ Genres, Production Countries, Cast, Director และ Writers...")
        
        # ... (ส่วนการทำ Data Cleaning ยังคงเดิม) ...
        json_cols = ['genres', 'production_countries', 'production_companies', 'spoken_languages', 'cast', 'writers', 'producers']
        for col in json_cols:
            df[f'{col}_list'] = df[col].astype(str).apply(parse_and_extract_names)
            
        numeric_cols = ['revenue', 'budget', 'runtime', 'vote_count', 'imdb_votes', 'imdb_rating', 'popularity']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['release_year'] = pd.to_datetime(df['release_date'], errors='coerce').dt.year
        df['movie_fact_id'] = df['id'].astype('Int64') 

        movie_facts_cols = [
            'movie_fact_id', 'title', 'original_title', 'release_year', 'release_date',
            'status', 'runtime', 'budget', 'revenue', 'vote_average', 'vote_count',
            'imdb_rating', 'imdb_votes', 'popularity', 'original_language',
            'genres_list', 'production_countries_list'
        ]
        df_facts = df[movie_facts_cols].copy()
        
        # --- 5. โหลดตารางหลัก: production.movie_facts (ใช้ Bulk Copy) ---
        print(f"กำลังโหลดตารางหลัก {PRODUCTION_SCHEMA}.{MOVIE_FACTS_TABLE} ด้วย Bulk Copy...")
        
        # ต้องแปลงคอลัมน์ List ให้เป็น String ก่อนทำ Bulk Copy
        df_facts_copy = df_facts.copy()
        df_facts_copy['genres_list'] = df_facts_copy['genres_list'].apply(lambda x: '[' + ','.join(map(str, x)) + ']')
        df_facts_copy['production_countries_list'] = df_facts_copy['production_countries_list'].apply(lambda x: '[' + ','.join(map(str, x)) + ']')
        
        create_table_and_bulk_copy(engine, CONN_STRING, df_facts_copy, MOVIE_FACTS_TABLE, FULL_FACTS_TABLE, PRODUCTION_SCHEMA)
        print(f"[SUCCESS] โหลดตารางหลักสำเร็จ! ตาราง: {PRODUCTION_SCHEMA}.{MOVIE_FACTS_TABLE}")

        # --- 6. Aggregation: สรุปรายได้เฉลี่ยตาม Genres (ตอบโจทย์ Q1) ---
        print("กำลังสรุปและโหลดตารางสรุปรายได้เฉลี่ยตาม Genres...")
        
        df_exploded = df_facts.explode('genres_list')
        df_filtered = df_exploded[(df_exploded['revenue'] > 0) & (df_exploded['budget'] > 0) & (df_exploded['genres_list'].notna())].copy()
        
        df_genre_summary = df_filtered.groupby('genres_list').agg(
            average_revenue=('revenue', 'mean'),
            total_movies=('movie_fact_id', 'count')
        ).reset_index().rename(columns={'genres_list': 'genre_name'})
        
        df_genre_summary = df_genre_summary.sort_values(by='average_revenue', ascending=False)
        
        # 7. โหลดตารางสรุป (ใช้ Bulk Copy)
        create_table_and_bulk_copy(engine, CONN_STRING, df_genre_summary, GENRE_SUMMARY_TABLE, FULL_GENRE_TABLE, PRODUCTION_SCHEMA)
        print(f"[SUCCESS] โหลดตารางสรุป Genres สำเร็จ! ตาราง: {PRODUCTION_SCHEMA}.{GENRE_SUMMARY_TABLE}")

    except OperationalError as e:
        print(f"[ERROR] การเชื่อมต่อ PostgreSQL ล้มเหลว: ตรวจสอบ Docker และ Port")
        print(e)
    except Exception as e:
        print(f"[ERROR] เกิดข้อผิดพลาดที่ไม่ทราบสาเหตุในขั้นตอน Transformation: {e}")

if __name__ == "__main__":
    transform_data()