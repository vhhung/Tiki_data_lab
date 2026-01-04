import psycopg2
from config import load_config

def connect(config=None, filename="database.ini", section="postgresql"):
    """Return an OPEN connection. Caller is responsible for closing it."""
    if config is None:
        config = load_config(filename=filename, section=section)

    conn = psycopg2.connect(**config)
    print("Connected to the PostgreSQL server.")
    return conn

if __name__ == "__main__":
    conn = connect()
    conn.close()
