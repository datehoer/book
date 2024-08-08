from config import MYSQL_CONFIG
from useMySQL import MySQLDatabase
import hashlib
import pyquery
import io
import requests
from minio import Minio
from minio.error import S3Error
minio_client = Minio(
        "oss.datehoer.com",
        access_key="ONIvfh2gANnmdSdt78Ut",
        secret_key="exW0gBghUuGyKnPxykWJqHoKFrTSXPmAFYl4LgBo",
        secure=True  # 如果使用HTTPS则为True，否则为False
    )
def generate_md5(data):
    md5_hash = hashlib.md5()
    md5_hash.update(f"{data}".encode('utf-8'))
    return md5_hash.hexdigest()
def download_img(url):

    try:
        response = requests.get(url)
        response.raise_for_status()
        image_data = io.BytesIO(response.content)
        
        content_type = response.headers.get('Content-Type', 'image/jpeg')
        if 'image/jpeg' in content_type:
            extension = '.jpg'
        elif 'image/png' in content_type:
            extension = '.png'
        elif 'image/jpg' in content_type:
            extension = '.jpg'
        elif 'image/webp' in content_type:
            extension = '.webp'
        elif 'image/gif' in content_type:
            extension = '.gif'
        else:
            print("Unsupported image type:", content_type)
            return None
        pic_name = generate_md5(url) + extension
        minio_client.put_object(
            "book", 
            "img/"+pic_name, 
            data=image_data, 
            length=len(response.content),
            content_type=content_type  # 根据实际图片类型调整
        )
        return pic_name
    except Exception as e:
        print(e)
        return None
    
config_mysql1 = MYSQL_CONFIG.copy()
config_mysql1['database'] = 'book'
db1 = MySQLDatabase(config_mysql1)

for books in db1.fetch_iter("select id, chapter_content from book_content", [], batch_size=100):
    for book in books:
        id = book[0]
        content = book[1]
        doc = pyquery.PyQuery(content)
        for img in doc('img').items():
            img_url = img.attr('src')
            if img_url:
                 minio_url = "https://oss.datehoer.com/book/imgs/"+download_img(img_url)
                 if minio_url:
                    img.attr('src', minio_url)
        db1.execute("update book_content set chapter_content = %s, update_time=NOW() where id = %s", [doc.outer_html(), id])
    print("change success")
db1.close_all_connections()
