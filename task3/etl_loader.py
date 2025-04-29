#!/usr/bin/env python3
import argparse
import asyncio
import asyncpg
import time
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Set, Tuple

DEFAULT_BATCH_SIZE = 1000
DEFAULT_POOL_SIZE = 20
DEFAULT_HOST = "localhost"
DEFAULT_DATABASE = "tiktok_streaming"
DEFAULT_USER = "postgres"
DEFAULT_PASSWORD = "postgres"
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Hh9wPMVThGmXrctBrG15eOux8l5I9m5T1vaRisHqpF4/export?format=csv&gid=431063534"

async def create_tables(pool: asyncpg.Pool) -> None:
    try:
        with open('task3/sql/create_schema.sql', 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        async with pool.acquire() as conn:
            await conn.execute(schema_sql)
    except Exception as e:
        print(f"Error creating schema: {e}")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS subjects (
            subject_id SERIAL PRIMARY KEY,
            subject_name VARCHAR(100) NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS courses (
            course_id SERIAL PRIMARY KEY,
            course_name VARCHAR(255) NOT NULL,
            subject_id INTEGER NOT NULL REFERENCES subjects(subject_id),
            UNIQUE(course_name, subject_id)
        );
        CREATE TABLE IF NOT EXISTS packages (
            package_id SERIAL PRIMARY KEY,
            package_name VARCHAR(255) NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS orders (
            order_id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            course_id INTEGER REFERENCES courses(course_id),
            package_id INTEGER REFERENCES packages(package_id),
            order_date DATE NOT NULL,
            amount NUMERIC(10, 2) NOT NULL,
            payment_status VARCHAR(20) NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
        CREATE INDEX IF NOT EXISTS idx_orders_course_id ON orders(course_id);
        CREATE INDEX IF NOT EXISTS idx_orders_package_id ON orders(package_id);
        """
        
        async with pool.acquire() as conn:
            await conn.execute(create_table_query)

async def extract_reference_data(df: pd.DataFrame) -> Tuple[Set[str], Set[Tuple[str, str]], Set[str]]:
    all_subjects = set()
    for subjects_str in df['subjects'].dropna():
        subjects = [subj.strip() for subj in str(subjects_str).split(',')]
        all_subjects.update(subjects)
    
    courses = set()
    for _, row in df[['course_name', 'subjects']].dropna().iterrows():
        if pd.notna(row['subjects']) and pd.notna(row['course_name']):
            subjects = [subj.strip() for subj in str(row['subjects']).split(',')]
            for subject in subjects:
                courses.add((row['course_name'], subject))
    
    packages = set(df['duration'].dropna().unique())
    
    print(f"Extracted {len(all_subjects)} unique subjects, {len(courses)} unique courses, {len(packages)} unique packages")
    return all_subjects, courses, packages

async def load_reference_data(
    pool: asyncpg.Pool, 
    subjects: Set[str], 
    courses: Set[Tuple[str, str]], 
    packages: Set[str]
) -> Dict[str, Dict[str, int]]:
    print("Loading reference data to database")
    ref_data = {
        'subjects': {},
        'courses': {},
        'packages': {}
    }
    
    async with pool.acquire() as conn:
        async with conn.transaction():
            for subject_name in subjects:
                if not subject_name:
                    continue
                subject_id = await conn.fetchval(
                    'INSERT INTO subjects (subject_name) VALUES ($1) '
                    'ON CONFLICT (subject_name) DO UPDATE SET subject_name = $1 '
                    'RETURNING subject_id',
                    subject_name
                )
                ref_data['subjects'][subject_name] = subject_id
            
            for course_name, subject_name in courses:
                if not subject_name or not course_name:
                    continue
                subject_id = ref_data['subjects'].get(subject_name)
                if subject_id:
                    course_id = await conn.fetchval(
                        'INSERT INTO courses (course_name, subject_id) VALUES ($1, $2) '
                        'ON CONFLICT (course_name, subject_id) DO UPDATE SET course_name = $1 '
                        'RETURNING course_id',
                        course_name, subject_id
                    )
                    ref_data['courses'][(course_name, subject_name)] = course_id
            
            for package_name in packages:
                if not package_name:
                    continue
                package_id = await conn.fetchval(
                    'INSERT INTO packages (package_name) VALUES ($1) '
                    'ON CONFLICT (package_name) DO UPDATE SET package_name = $1 '
                    'RETURNING package_id',
                    package_name
                )
                ref_data['packages'][package_name] = package_id
    
    print(f"Loaded {len(ref_data['subjects'])} subjects, {len(ref_data['courses'])} courses, {len(ref_data['packages'])} packages")
    return ref_data

async def process_batch(
    pool: asyncpg.Pool, 
    batch: pd.DataFrame,
    ref_data: Dict[str, Dict[str, int]]
) -> int:
    if batch.empty:
        return 0
    
    records = []
    for _, row in batch.iterrows():
        try:
            order_date = pd.to_datetime(row['order_date'])
            if pd.isna(order_date):
                order_date = datetime.now()
        except Exception:
            order_date = datetime.now()
        
        amount = row['amount']
        if isinstance(amount, str):
            amount_str = amount.replace(' ', '').replace(',', '.')
            try:
                amount = float(amount_str)
            except ValueError:
                amount = 0.0
        elif pd.isna(amount):
            amount = 0.0
        
        user_id = 1000
        
        payment_status = "completed" 
        
        subjects = [subj.strip() for subj in str(row['subjects']).split(',')] if pd.notna(row['subjects']) else []
        course_name = row['course_name'] if pd.notna(row['course_name']) else None
        package_name = row['duration'] if pd.notna(row['duration']) else None
        
        course_id = None
        if course_name and subjects:
            for subject in subjects:
                if (course_name, subject) in ref_data['courses']:
                    course_id = ref_data['courses'][(course_name, subject)]
                    break
        
        package_id = ref_data['packages'].get(package_name) if package_name else None
        
        records.append({
            'user_id': user_id,
            'course_id': course_id,
            'package_id': package_id,
            'order_date': order_date,
            'amount': amount,
            'payment_status': payment_status
        })
    
    columns = ['user_id', 'course_id', 'package_id', 'order_date', 'amount', 'payment_status']
    values = []
    for r in records:
        values.append([r[col] if col in r and r[col] is not None else None for col in columns])
    
    async with pool.acquire() as conn:
        try:
            result = await conn.copy_records_to_table(
                'orders', 
                records=values,
                columns=columns
            )
            return len(records)
        except Exception as e:
            print(f"Error inserting data: {e}")
            inserted = 0
            for record in records:
                try:
                    await conn.execute("""
                        INSERT INTO orders (
                            user_id, course_id, package_id, order_date, amount, payment_status
                        ) VALUES ($1, $2, $3, $4, $5, $6)
                    """, 
                    record['user_id'], 
                    record['course_id'], 
                    record['package_id'], 
                    record['order_date'], 
                    record['amount'], 
                    record['payment_status'])
                    inserted += 1
                except Exception as inner_e:
                    print(f"Error inserting record: {inner_e}")
            return inserted

async def load_data_from_google_sheets(
    pool: asyncpg.Pool,
    sheet_url: str,
    batch_size: int
) -> Dict[str, Any]:
    start_time = time.time()
    
    print(f"Loading data from Google Sheets: {sheet_url}")
    
    try:
        df = pd.read_csv(
            sheet_url,
            header=0,
            skip_blank_lines=True,
            na_values=['', 'NA', 'N/A'],
            keep_default_na=True
        )
        
        if any(col.startswith('Unnamed:') for col in df.columns) or any(isinstance(col, (int, float)) for col in df.columns):
            column_names = ['name', 'source', 'order_date', 'amount', 'subjects', 'course_name', 'duration']
            for i in range(len(df.columns) - len(column_names)):
                column_names.append(f'empty{i+1}')
            
            df.columns = column_names
  
        df = df.drop(columns=[col for col in df.columns if col.startswith('empty') or col.startswith('Unnamed')])
        df = df.dropna(how='all')
        subjects, courses, packages = await extract_reference_data(df)
        ref_data = await load_reference_data(pool, subjects, courses, packages)
        total_rows = len(df)
        batches = [df[i:i+batch_size].copy() for i in range(0, total_rows, batch_size)]
        
        print(f"Total records: {total_rows}, batches to process: {len(batches)}")
        tasks = []
        for batch in batches:
            task = asyncio.create_task(process_batch(pool, batch, ref_data))
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        total_processed = sum(results)
        
        end_time = time.time()
        duration = end_time - start_time
        
        return {
            "total_records": total_processed,
            "duration_seconds": duration,
            "records_per_second": total_processed / duration if duration > 0 else 0,
            "total_batches": len(batches)
        }
    
    except Exception as e:
        print(f"Error loading data: {e}")
        raise

async def run_etl(
    sheet_url: str,
    batch_size: int,
    pool_size: int,
    host: str,
    database: str,
    user: str,
    password: str
) -> Dict[str, Any]:
    start_time = time.time()
    
    pool = await asyncpg.create_pool(
        host=host,
        database=database,
        user=user,
        password=password,
        min_size=5,
        max_size=pool_size,
        command_timeout=60
    )
    try:
        await create_tables(pool)
        result = await load_data_from_google_sheets(pool, sheet_url, batch_size)
        end_time = time.time()
        result["total_etl_duration"] = end_time - start_time
        return result
    finally:
        await pool.close()

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Loading data from Google Sheets to PostgreSQL')
    parser.add_argument('--sheet-url', default=GOOGLE_SHEET_URL, help=f'URL Google Sheets (default: {GOOGLE_SHEET_URL})')
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE, help=f'Batch size (default: {DEFAULT_BATCH_SIZE})')
    parser.add_argument('--pool-size', type=int, default=DEFAULT_POOL_SIZE, help=f'Connection pool size (default: {DEFAULT_POOL_SIZE})')
    parser.add_argument('--host', default=DEFAULT_HOST, help=f'Database host (default: {DEFAULT_HOST})')
    parser.add_argument('--database', default=DEFAULT_DATABASE, help=f'Database name (default: {DEFAULT_DATABASE})')
    parser.add_argument('--user', default=DEFAULT_USER, help=f'Database user (default: {DEFAULT_USER})')
    parser.add_argument('--password', default=DEFAULT_PASSWORD, help=f'Database password (default: {DEFAULT_PASSWORD})')
    return parser.parse_args()

async def main() -> None:
    args = parse_args()
    print("ETL Loader started with configuration:")
    print(f"  - Sheet URL: {args.sheet_url}")
    print(f"  - Batch size: {args.batch_size}")
    print(f"  - Pool size: {args.pool_size}")
    print(f"  - Database: {args.database} on {args.host}")
    
    try:
        result = await run_etl(
            sheet_url=args.sheet_url,
            batch_size=args.batch_size,
            pool_size=args.pool_size,
            host=args.host,
            database=args.database,
            user=args.user,
            password=args.password
        )
        
        print("\nETL process completed successfully")
        print(f"Records processed: {result['total_records']:,}")
        print(f"Total ETL duration: {result['total_etl_duration']:.2f} seconds")
        print(f"Data loading duration: {result['duration_seconds']:.2f} seconds")
        print(f"Records per second: {result['records_per_second']:.2f}")
        print(f"Total batches: {result['total_batches']}")
        
    except Exception as e:
        print(f"ETL process failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 