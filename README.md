# Tiki JSON → PostgreSQL ETL

This ETL script loads **all** Tiki product JSON files matching `products_*.json` into PostgreSQL.

It is designed to work with your Project 02 helpers:

- `config.py` → `load_config()` reads `database.ini`
- `connect.py` → `connect()` returns an open psycopg2 connection

---

## Folder layout (recommended)

```
project-root/
  etl_tiki_to_postgres.py
  config.py
  connect.py
  database.ini
  data/
    products_1.json
    products_2.json
    ...
```

The script defaults to reading from `./data`.

---

## Requirements

- Python 3.9+ (any modern Python 3 is fine)
- PostgreSQL running and reachable

---

## Virtual environment (recommended)

Create and activate a `.venv` so dependencies are isolated.

### Linux / macOS
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Windows (PowerShell)
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
```

---

## Install dependencies

With the virtual environment activated, install the only required package:

```bash
pip install psycopg2-binary
```

---

## Quick PostgreSQL setup for this project (create user + database)

If someone wants to run your code from scratch, they can create:

- A database named: `tiki`
- A PostgreSQL role/user named: `youruser`
- Password: `yourpassword`

### 1) Create role + database (run as an admin / superuser)

On Linux with local Postgres installed, you can typically run:

```bash
sudo -u postgres psql
```

Then run these SQL commands:

```sql
-- Create a login role with password
CREATE ROLE youruser WITH LOGIN PASSWORD 'yourpassword';

-- Create the database owned by that user
CREATE DATABASE tiki OWNER youruser;

-- Allow the user to create objects in schema public (important!)
GRANT USAGE, CREATE ON SCHEMA public TO youruser;

-- Ensure the user can connect to the database
GRANT CONNECT ON DATABASE tiki TO youruser;
```

> If `youruser` already exists, you can set/reset the password:
>
> ```sql
> ALTER ROLE youruser WITH PASSWORD 'yourpassword';
> ```

### 2) Verify the login works

```bash
psql -h localhost -U youruser -d tiki
```

---

## Configure `database.ini`

Create/edit `database.ini` (section **[postgresql]**):

```ini
[postgresql]
host=localhost
database=tiki
user=youruser
password=yourpassword
; port=5432
```

Notes:
- `port` is optional. If you omit it, PostgreSQL defaults to **5432**.
- Replace `youruser/yourpassword` with real credentials if you prefer.

---

## What the script creates

### Table: `tiki_products`

- `id` (BIGINT, primary key)
- `name` (TEXT)
- `url_key` (TEXT)
- `price` (NUMERIC)
- `description` (TEXT)
- `images` (JSONB)
- `source_file` (TEXT)
- `ingested_at` (TIMESTAMPTZ, default now())

### Optional table (if `--normalize-images`): `tiki_product_images`

- `product_id` (FK → `tiki_products.id`)
- `position` (INT)
- `image_url` (TEXT)

---

## Run the ETL

### 1) Default run (reads `./data`, uses `./database.ini`)

```bash
python etl_tiki_to_postgres.py --ini ./database.ini
```

### 2) Specify a different data directory

```bash
python etl_tiki_to_postgres.py --data-dir "/path/to/Project 02/data" --ini ./database.ini
```

### 3) Load images into a separate table

```bash
python etl_tiki_to_postgres.py --ini ./database.ini --normalize-images
```

### 4) Load from a single JSON file

```bash
python etl_tiki_to_postgres.py --data-dir "./data/products_1.json" --ini ./database.ini
```

---

## Verify in psql

```bash
psql -h localhost -U youruser -d tiki
```

```sql
SELECT COUNT(*) FROM tiki_products;
SELECT * FROM tiki_products LIMIT 5;

-- if --normalize-images was used:
SELECT COUNT(*) FROM tiki_product_images;
SELECT * FROM tiki_product_images LIMIT 10;
```

---

## Troubleshooting

### A) `permission denied for schema public`

Connect as a superuser (often `postgres`) and run:

```bash
sudo -u postgres psql -d tiki
```

```sql
GRANT USAGE, CREATE ON SCHEMA public TO youruser;
GRANT CONNECT ON DATABASE tiki TO youruser;
```

(Optional) If you want the user to read/write ALL current and future tables:

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO youruser;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO youruser;
```

### B) `OperationalError: could not connect to server`

- Check PostgreSQL is running
- Check `host`, `database`, `user`, `password` in `database.ini`
- Verify you can connect with:

```bash
psql -h <host> -U <user> -d <database>
```

### C) `No files matching products_*.json found`

- Ensure your data files are named like: `products_1.json`, `products_2.json`, ...
- Or point `--data-dir` directly to the file.

### D) Invalid JSON

The script prints the filename and the JSON line/column.
Fix the file or remove the corrupted file and rerun.

---

## Notes

- Inserts use **UPSERT** on `id` (updates existing rows when the same `id` appears again).
- If `--normalize-images` is enabled, the script deletes old images per product batch and reinserts the current list.
