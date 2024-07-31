import requests
import json
import json_repair
from useMySQL import MySQLDatabase
import pandas as pd
from config import MYSQL_CONFIG, KEY
db = MySQLDatabase(MYSQL_CONFIG)
url = "https://api.deepseek.com/chat/completions"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {KEY}"
}
with open("./book_info.json", "r", encoding="utf-8") as f:
    book_info = json.load(f)
df = pd.DataFrame(book_info)
grouped = df.groupby('id')
groups_dict = {}
for id, group in grouped:
    # 将分组 DataFrame 转换为字典列表
    groups_dict[id] = group.to_dict('records')
sql = "update book_book set book_description=%s, book_tags=%s where id=%s"
for id, group_list in groups_dict.items():
    if id < 75:
        continue
    if id > 75:
        break
    data = "书名:"+group_list[0]["book_name"]+",章节:"+",".join([i['chapter_name'] for i in group_list])
    data = data.replace(" ","")
    messages = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "user", "content": '我希望你给这本书根据他的章节写一个描述(不超过100字),并且增加标签(用,间隔,最多10个),输出json格式数据,例子:{"book_name":"零基础实战机器学习", "description": "学习机器学习","tags":"机器学习,python,零基础"}.内容:'+data}
            ],
        "stream": False
    }
    res = requests.post(url, headers=headers, json=messages)
    res_json = res.json()
    message = res_json['choices'][0]['message']['content']
    message = json.loads(json_repair.repair_json(message))
    print(message)
    db.execute(sql, params=(message["description"], message["tags"], id))
    