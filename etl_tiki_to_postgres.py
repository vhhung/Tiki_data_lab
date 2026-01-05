import json
import sys
from pathlib import Path
from typing import Any, Iterator, List, Sequence, Tuple

from psycopg2 import OperationalError
from psycopg2.errors import InsufficientPrivilege
from psycopg2.extras import Json, execute_values

from config import load_config
from connect import connect


# ----------------------------
# Schema (DDL)
# ----------------------------

DDL_PRODUCTS = """
CREATE TABLE IF NOT EXISTS tiki_products (
    id           BIGINT PRIMARY KEY,
    name         TEXT,
    url_key      TEXT,
    price        NUMERIC,
    description  TEXT,
    images       JSONB,
    source_file  TEXT,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tiki_products_price ON tiki_products(price);
CREATE INDEX IF NOT EXISTS idx_tiki_products_url_key ON tiki_products(url_key);
"""

DDL_IMAGES = """
CREATE TABLE IF NOT EXISTS tiki_product_images (
    product_id   BIGINT NOT NULL REFERENCES tiki_products(id) ON DELETE CASCADE,
    position     INT NOT NULL,
    image_url    TEXT NOT NULL,
    PRIMARY KEY (product_id, position)
);

CREATE INDEX IF NOT EXISTS idx_tiki_product_images_url ON tiki_product_images(image_url);
"""


# ----------------------------
# Logging helpers
# ----------------------------

def warn(msg: str) -> None:
    """Print a warning message to stderr."""
    print(f"[WARN] {msg}", file=sys.stderr)


def err(msg: str) -> None:
    """Print an error message to stderr."""
    print(f"[ERROR] {msg}", file=sys.stderr)


# ----------------------------
# File reading helpers
# ----------------------------

def iter_product_files(data_path: Path) -> List[Path]:
    """
    Accepts either:
    - A directory containing products_*.json
    - A single JSON file
    Returns a sorted list of Path objects.
    """
    if not data_path.exists():
        raise FileNotFoundError(f"Path not found: {data_path.resolve()}")

    if data_path.is_file():
        return [data_path]

    files = sorted(data_path.glob("products_*.json"))
    if not files:
        raise FileNotFoundError(
            "No files matching products_*.json found in: "
            f"{data_path.resolve()}\n"
            "Hint: check your filenames or point --data-dir to a specific JSON file."
        )
    return files


def load_products_from_file(path: Path) -> List[dict]:
    """Load a JSON file and ensure the root is a list."""
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("JSON root must be a list")
        return data
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Invalid JSON in {path.name} (line {e.lineno}, col {e.colno}): {e.msg}"
        ) from e
    except Exception as e:
        raise RuntimeError(f"Failed to read JSON {path.name}: {e}") from e


# ----------------------------
# Batch helpers
# ----------------------------

def chunks(seq: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    """Yield successive chunks from a sequence."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# ----------------------------
# DB upsert helpers
# ----------------------------

def upsert_products(cur, rows: Sequence[Tuple], page_size: int = 2000) -> int:
    """
    rows tuple:
      (id, name, url_key, price, description, images_jsonb, source_file)
    """
    sql = """
    INSERT INTO tiki_products (id, name, url_key, price, description, images, source_file)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        name        = EXCLUDED.name,
        url_key     = EXCLUDED.url_key,
        price       = EXCLUDED.price,
        description = EXCLUDED.description,
        images      = EXCLUDED.images,
        source_file = EXCLUDED.source_file,
        ingested_at = now()
    """
    execute_values(cur, sql, rows, page_size=page_size)
    return len(rows)


def upsert_images(cur, image_rows: Sequence[Tuple], page_size: int = 5000) -> int:
    """
    image_rows tuple:
      (product_id, position, image_url)

    Strategy:
    - Delete existing images for the product_ids in this batch
    - Reinsert (and upsert on conflict)
    """
    product_ids = sorted({r[0] for r in image_rows})
    if product_ids:
        cur.execute(
            "DELETE FROM tiki_product_images WHERE product_id = ANY(%s)",
            (product_ids,),
        )

    sql = """
    INSERT INTO tiki_product_images (product_id, position, image_url)
    VALUES %s
    ON CONFLICT (product_id, position) DO UPDATE SET
        image_url = EXCLUDED.image_url
    """
    execute_values(cur, sql, image_rows, page_size=page_size)
    return len(image_rows)


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    data_dir = Path("./data")
    ini_path = "database.ini"
    batch_size = 1000
    normalize_images = False

    files = iter_product_files(data_dir)
    print(f"Found {len(files)} file(s) from {data_dir.resolve()}")

    db_params = load_config(filename=ini_path, section="postgresql")

    total_products = 0
    total_images = 0

    try:
        with connect(db_params) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                # Create tables
                try:
                    cur.execute(DDL_PRODUCTS)
                    if normalize_images:
                        cur.execute(DDL_IMAGES)
                    conn.commit()
                except InsufficientPrivilege:
                    conn.rollback()
                    user = db_params.get("user")
                    db = db_params.get("database")
                    err("Permission denied: user lacks CREATE/USAGE privileges on the schema (often 'public').")
                    err("Fix by connecting as a superuser (e.g., postgres) and running:")
                    err(f"  GRANT USAGE, CREATE ON SCHEMA public TO {user};")
                    err(f"  GRANT CONNECT ON DATABASE {db} TO {user};")
                    return 3

                # Load data
                for fp in files:
                    products = load_products_from_file(fp)

                    product_rows: List[Tuple] = []
                    image_rows: List[Tuple] = []
                    bad_items = 0

                    for item in products:
                        try:
                            pid = int(item.get("id"))
                        except Exception:
                            bad_items += 1
                            continue

                        name = item.get("name")
                        url_key = item.get("url_key")
                        price = item.get("price")
                        desc = item.get("description")
                        images = item.get("images", [])

                        product_rows.append((pid, name, url_key, price, desc, Json(images), fp.name))

                        if normalize_images and isinstance(images, list):
                            for pos, url in enumerate(images):
                                if url:
                                    image_rows.append((pid, pos, str(url)))

                    if bad_items:
                        warn(f"{fp.name}: skipped {bad_items} item(s) with missing/invalid 'id'")

                    try:
                        for batch in chunks(product_rows, batch_size):
                            total_products += upsert_products(cur, batch)

                        if normalize_images and image_rows:
                            for batch in chunks(image_rows, batch_size * 2):
                                total_images += upsert_images(cur, batch)

                        conn.commit()
                        print(f"Loaded {len(products)} products from {fp.name}")
                    except Exception as e:
                        conn.rollback()
                        err(f"DB insert failed while processing {fp.name}: {e}")
                        return 3

    except OperationalError as e:
        err("Could not connect to PostgreSQL (wrong host/port/db/user/pass, or server not running).")
        err(str(e))
        return 3

    print(f"Done. products={total_products}, images={total_images}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except FileNotFoundError as e:
        err(str(e))
        raise SystemExit(2)
    except (ValueError, RuntimeError) as e:
        err(str(e))
        raise SystemExit(2)
    except KeyboardInterrupt:
        err("Cancelled by user (Ctrl+C).")
        raise SystemExit(130)
    except Exception as e:
        err(f"Unexpected error: {e}")
        raise SystemExit(4)
