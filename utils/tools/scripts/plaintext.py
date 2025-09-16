#!/usr/bin/env python3
"""
Simple AI reference file generator
Outputs three files to mm/config/plaintext/ with current date
"""

import os
import psycopg2
import psycopg2.extras
from datetime import datetime

def get_current_date():
    return datetime.now().strftime('%Y%m%d')

def generate_file_structure():
    date_str = get_current_date()
    output_file = f"mm/config/plaintext/file_structure_plaintext_{date_str}.txt"
    
    with open(output_file, 'w') as f:
        f.write("PROJECT FILE STRUCTURE\n")
        f.write("=" * 50 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n") 
        
        for root, dirs, files in os.walk("mm"):
            level = root.replace("mm", "").count(os.sep)
            indent = "  " * level
            f.write(f"{indent}{os.path.basename(root)}/\n")
            sub_indent = "  " * (level + 1)
            for file in files:
                f.write(f"{sub_indent}{file}\n")

def generate_code_pages():
    date_str = get_current_date()
    output_file = f"mm/config/plaintext/code_pages_plaintext_{date_str}.txt"
    
    with open(output_file, 'w') as f:
        f.write("ALL CODE IN PLAINTEXT\n")
        f.write("=" * 50 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")
        
        for root, dirs, files in os.walk("mm"):
            for file in files:
                file_path = os.path.join(root, file)
                
                f.write("\n" + "=" * 80 + "\n")
                f.write(f"FILE: {file_path}\n")
                f.write("=" * 80 + "\n")
                
                try:
                    with open(file_path, 'r') as code_file:
                        content = code_file.read()
                        f.write(content)
                        if not content.endswith('\n'):
                            f.write('\n')
                except:
                    f.write("[COULD NOT READ FILE]\n")

def generate_db_schema():
    date_str = get_current_date()
    output_file = f"mm/config/plaintext/db_schema_plaintext_{date_str}.txt"
    
    dsn = os.getenv("PG_DSN", "dbname=ariadne user=postgres host=localhost")
    conn = psycopg2.connect(dsn)
    
    with open(output_file, 'w') as f:
        f.write("DATABASE SCHEMA\n")
        f.write("=" * 50 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")
        
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Get all tables
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename")
        tables = [row[0] for row in cur.fetchall()]
        
        f.write(f"TABLES ({len(tables)} found)\n")
        f.write("-" * 30 + "\n\n")
        
        for table_name in tables:
            f.write(f"TABLE: {table_name}\n")
            f.write("=" * (len(table_name) + 7) + "\n")
            
            # Columns
            cur.execute("""
                SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns = cur.fetchall()
            f.write("COLUMNS:\n")
            for col in columns:
                f.write(f"  {col['column_name']:<25} {col['data_type']}")
                if col['character_maximum_length']:
                    f.write(f"({col['character_maximum_length']})")
                if col['is_nullable'] == 'NO':
                    f.write(" NOT NULL")
                if col['column_default']:
                    f.write(f" DEFAULT {col['column_default']}")
                f.write("\n")
            
            # Constraints
            cur.execute("""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints
                WHERE table_schema = 'public' AND table_name = %s
            """, (table_name,))
            
            constraints = cur.fetchall()
            if constraints:
                f.write("\nCONSTRAINTS:\n")
                for const in constraints:
                    f.write(f"  {const['constraint_name']} ({const['constraint_type']})\n")
            
            # Indexes
            cur.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = 'public' AND tablename = %s
            """, (table_name,))
            
            indexes = cur.fetchall()
            if indexes:
                f.write("\nINDEXES:\n")
                for idx in indexes:
                    f.write(f"  {idx['indexname']}\n")
                    f.write(f"    {idx['indexdef']}\n")
            
            # Triggers
            cur.execute("""
                SELECT trigger_name, event_manipulation, action_timing, action_statement
                FROM information_schema.triggers
                WHERE event_object_schema = 'public' AND event_object_table = %s
            """, (table_name,))
            
            triggers = cur.fetchall()
            if triggers:
                f.write("\nTRIGGERS:\n")
                for trigger in triggers:
                    f.write(f"  {trigger['trigger_name']} ({trigger['action_timing']} {trigger['event_manipulation']})\n")
            
            f.write("\n" + "-" * 60 + "\n\n")
        
        cur.close()
    conn.close()

if __name__ == "__main__":
    os.makedirs("mm/config/plaintext", exist_ok=True)
    generate_file_structure()
    generate_code_pages()
    generate_db_schema()
    print("Files generated in mm/config/plaintext/")