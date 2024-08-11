import sqlite3
  
class SQLiteService:
  
    def commit(query, values):
        db = sqlite3.connect('queue.db')
        db.row_factory = sqlite3.Row
        cursor = db.cursor()
        cursor.execute(query, values)
        db.commit()
        last_inserted_id = cursor.lastrowid
        cursor.close()
        db.close()
        return last_inserted_id

    def fetch(query, values=None):
        db = sqlite3.connect('queue.db')
        db.row_factory = sqlite3.Row 
        cursor = db.cursor()
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        rows = cursor.fetchall()
        db.close()
        result = [dict(row) for row in rows]
        return result