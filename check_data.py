import pandas as pd
import psycopg2 

# --- 1. ตั้งค่าการเชื่อมต่อฐานข้อมูล (ใช้ค่าเดียวกัน) ---
DB_USER = 'DB_AIE321_BIG_DATA'
DB_PASSWORD = '321bigdatawork'
DB_HOST = 'localhost' 
DB_PORT = '6666'      
DB_NAME = 'AIE321' 

FULL_TABLE_NAME = 'raw_data.tmdb_movies_raw'

def check_data_info():
    """เชื่อมต่อด้วย psycopg2 ดึงข้อมูลทั้งหมด และแสดง df.info()"""
    # Connection string สำหรับ psycopg2 โดยตรง
    conn_string = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"

    try:
        print("--- 🔬 เริ่มต้นการตรวจสอบโครงสร้างข้อมูลด้วย df.info() ---")
        
        # 1. เชื่อมต่อด้วย psycopg2 โดยตรง
        with psycopg2.connect(conn_string) as conn:
            
            # 2. ใช้ Pandas read_sql_query ร่วมกับ psycopg2 Connection Object
            # วิธีนี้ทำงานได้ดีกว่าการใช้ SQLAlchemy Engine ในกรณีนี้
            full_query = f"SELECT * FROM {FULL_TABLE_NAME}"
            df = pd.read_sql_query(full_query, conn)
        
        print(f"[SUCCESS] ดึงข้อมูล {len(df):,} แถวจาก {FULL_TABLE_NAME} สำเร็จ")
        print("\n--- 📝 ข้อมูลโครงสร้างตาราง (df.info()) ---")

        # 3. แสดงผลลัพธ์ df.info()
        df.info()
        
        print("\n[SUCCESS] การตรวจสอบข้อมูลเสร็จสมบูรณ์")

    except OperationalError as e:
        print(f"[ERROR] การเชื่อมต่อถูกปฏิเสธ (OperationalError) โปรดตรวจสอบ Docker และ Port")
    except Exception as e:
        print(f"[ERROR] เกิดข้อผิดพลาดในการตรวจสอบข้อมูล: {e}")

if __name__ == "__main__":
    check_data_info()