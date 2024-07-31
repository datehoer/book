from config import MYSQL_CONFIG
from useMySQL import MySQLDatabase
import hashlib
def generate_md5(title, book_id):
    md5_hash = hashlib.md5()
    md5_hash.update(f"{title}{book_id}".encode('utf-8'))
    return md5_hash.hexdigest()

config_mysql1 = MYSQL_CONFIG.copy()
db2 = MySQLDatabase(MYSQL_CONFIG)
config_mysql1['database'] = 'book'
db1 = MySQLDatabase(config_mysql1)

for books in db2.fetch_iter("select title, book_id, indexes, content_html from book", [], batch_size=100):
    book_list_with_md5 = []
    for book in books:
        title = book[0]
        book_id = book[1]
        indexes = book[2]
        content = book[3]
        md5_value = generate_md5(title, book_id)
        book_list_with_md5.append((md5_value, book_id, indexes, content))
    db1.batch_insert("book_content", ["chapter_id", "book_id", "content_order", "chapter_content"], book_list_with_md5)
db1.close_all_connections()
db2.close_all_connections()
