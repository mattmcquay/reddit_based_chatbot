import sqlite3
from sqlite3 import Error
 
 
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        conn.close()
        
def reset_database():
    connection = sqlite3.connect('C:/Users/MMCQUAY/Documents/Reddit Data/reddit_data.db')
    c = connection.cursor()
    c.execute('DROP TABLE IF EXISTS parent_reply;')
 
if __name__ == '__main__':
    create_connection("C:/Users/MMCQUAY/Documents/Reddit Data/reddit_data.db")
    reset_database()
