# Tiki JSON → PostgreSQL ETL

This project loads **all** Tiki product JSON files matching `products_*.json` into a PostgreSQL database.

It uses the Project 02 helper modules:

- `config.py` → `load_config()` reads `database.ini`
- `connect.py` → `connect()` returns an open psycopg2 connection

---

## Project structure

Recommended layout:

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

By default, the script reads JSON files from `./data`.

---

## Requirements

- Python 3.9+
- PostgreSQL (local or remote)

Install the only dependency:

```bash
pip install psycopg2-binary
```

---

## Virtual environment (recommended)

### Linux / macOS
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install psycopg2-binary
```

### Windows (PowerShell)
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install psycopg2-binary
```

---

## PostgreSQL setup (create user + database)

If someone wants to run this project from scratch, create:

- Database: `tiki`
- User/role: `youruser`
- Password: `yourpassword`

### 1) Create role + database (run as admin / superuser)

On Linux (local Postgres), you can typically run:

```bash
sudo -u postgres psql
```

Then run:

```sql
-- Create a login role with password
CREATE ROLE youruser WITH LOGIN PASSWORD 'yourpassword';

-- Create the database owned by that user
CREATE DATABASE tiki OWNER youruser;

-- Allow the user to create objects in schema public (important)
GRANT USAGE, CREATE ON SCHEMA public TO youruser;

-- Ensure the user can connect to the database
GRANT CONNECT ON DATABASE tiki TO youruser;
```

If `youruser` already exists and you only want to reset the password:

```sql
ALTER ROLE youruser WITH PASSWORD 'yourpassword';
```

### 2) Verify you can login

```bash
psql -h localhost -U youruser -d tiki
```

---

## Configure `database.ini`

Create `database.ini` with section **[postgresql]**:

```ini
[postgresql]
host=localhost
database=tiki
user=youruser
password=yourpassword
; port=5432
```

Notes:
- `port` is optional (default is **5432**).
- `database.ini` should be in the same folder as `etl_tiki_to_postgres.py`.

---

## Script configuration (IMPORTANT)

This script does **not** use command-line arguments.  
To change settings, edit these variables inside `main()` in `etl_tiki_to_postgres.py`:

```python
data_dir = Path("./data")      # input folder (products_*.json)
ini_path = "database.ini"      # DB config file
batch_size = 1000              # insert batch size
normalize_images = False       # set True to create/load tiki_product_images
```

### Enable image normalization (optional)

If you want to create/load the separate images table, change:

```python
normalize_images = True
```

---

## Run the ETL

From the project root:

```bash
python etl_tiki_to_postgres.py
```

You should see output like:
- Found N file(s)
- Loaded X products from each file
- Done. products=..., images=...

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
- `ingested_at` (TIMESTAMPTZ)

### Optional table: `tiki_product_images` (if `normalize_images=True`)
- `product_id` (FK → `tiki_products.id`)
- `position` (INT)
- `image_url` (TEXT)

---

## Verify results

```bash
psql -h localhost -U youruser -d tiki
```

```sql
SELECT COUNT(*) FROM tiki_products;
SELECT * FROM tiki_products LIMIT 5;

-- If normalize_images=True:
SELECT COUNT(*) FROM tiki_product_images;
SELECT * FROM tiki_product_images LIMIT 10;
```

---

## Troubleshooting

### 1) `permission denied for schema public`

Connect as a superuser and grant privileges:

```bash
sudo -u postgres psql -d tiki
```

```sql
GRANT USAGE, CREATE ON SCHEMA public TO youruser;
GRANT CONNECT ON DATABASE tiki TO youruser;
```

### 2) `OperationalError: could not connect to server`

- Check PostgreSQL is running
- Check `host/database/user/password` in `database.ini`
- Test login:

```bash
psql -h localhost -U youruser -d tiki
```

### 3) `No files matching products_*.json found`

- Ensure files in `data/` are named like `products_1.json`, `products_2.json`, ...
- Or change `data_dir` in `main()` to the correct path.

### 4) Invalid JSON

The script prints the filename and the JSON line/column. Fix or remove the corrupted file and rerun.

---

## Notes

- Inserts use **UPSERT** on `id` (updates existing rows if the same `id` appears again).
- If `normalize_images=True`, the script also writes image URLs into `tiki_product_images`.
