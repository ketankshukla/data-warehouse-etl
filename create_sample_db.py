"""
Create a sample SQLite database for the ETL framework demo.
"""
import sqlite3
import os

# Create the data directory if it doesn't exist
os.makedirs("data", exist_ok=True)

# Connect to the SQLite database (will be created if it doesn't exist)
conn = sqlite3.connect("data/product_db.sqlite")
cursor = conn.cursor()

# Create products table
cursor.execute("""
CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT,
    price REAL,
    inventory_count INTEGER,
    last_updated TEXT
)
""")

# Sample product data
products = [
    ("PROD123", "Premium Headphones", "Electronics", 45.50, 75, "2024-01-10"),
    ("PROD456", "Wireless Mouse", "Electronics", 34.75, 120, "2024-01-15"),
    ("PROD789", "Mechanical Keyboard", "Electronics", 89.99, 45, "2024-02-01"),
    ("PROD101", "USB-C Cable", "Accessories", 22.00, 200, "2024-01-20"),
    ("PROD202", "External SSD 500GB", "Storage", 124.99, 30, "2024-02-15"),
    ("PROD303", "Wireless Charger", "Accessories", 28.25, 85, "2024-01-25"),
    ("PROD404", "Laptop Stand", "Accessories", 35.99, 60, "2024-02-10"),
    ("PROD505", "Bluetooth Speaker", "Electronics", 79.50, 40, "2024-02-20"),
    ("PROD606", "HDMI Cable", "Accessories", 18.75, 150, "2024-01-30"),
    ("PROD707", "Gaming Mouse", "Electronics", 65.99, 55, "2024-02-05")
]

# Insert product data
cursor.executemany(
    "INSERT OR REPLACE INTO products VALUES (?, ?, ?, ?, ?, ?)",
    products
)

# Commit changes and close connection
conn.commit()
conn.close()

print("Sample SQLite database created successfully with 10 products.")
