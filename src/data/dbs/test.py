from sqlalchemy import text
from src.data.dbs.session import Database

db = Database(echo=True)  # SQL 출력 켬
with db.session() as s:
    print(s.execute(text("SELECT 1")).scalar())
