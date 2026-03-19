"""
Discover all available tables in the kumoai catalog.
Run this first to find the correct table names.
"""
from connection import get_connection

conn = get_connection()
cursor = conn.cursor()

# List all schemas in the kumoai catalog
print("=== Schemas in kumoai catalog ===")
cursor.execute("SHOW SCHEMAS IN kumoai")
schemas = cursor.fetchall()
for s in schemas:
    print(f"  {s[0]}")

# List tables in each schema
for s in schemas:
    schema_name = s[0]
    print(f"\n=== Tables in kumoai.{schema_name} ===")
    try:
        cursor.execute(f"SHOW TABLES IN kumoai.{schema_name}")
        tables = cursor.fetchall()
        for t in tables:
            print(f"  kumoai.{schema_name}.{t[1]}")
    except Exception as e:
        print(f"  ERROR: {e}")

cursor.close()
conn.close()
