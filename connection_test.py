import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv()
HOST=os.getenv("PG_HOST","127.0.0.1")
PORT=int(os.getenv("PG_PORT","5432"))
USER=os.getenv("PG_USER","postgre")
PWD=os.getenv("PG_PASSWORD","")
DB  ="postgres"

url = URL.create("postgresql+psycopg2", username=USER, password=PWD,
                 host=HOST, port=PORT, database=DB)
eng = create_engine(url, pool_pre_ping=True)

with eng.connect() as c:
    print("Connected OK")
    v = c.execute(text("select version()")).scalar()
    print(v)