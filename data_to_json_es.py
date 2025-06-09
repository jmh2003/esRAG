# 将气象数据存储到 es当中去！
import os
import json
import re
import pandas as pd
from elasticsearch import helpers
from es import Es
from utils import load_yaml_conf
from datetime import datetime
import hashlib

def generate_document_id(title, text):
    """
    基于标题和内容生成唯一文档ID
    """
    unique_content = f"{title}_{text[:500]}"  # 使用前500个字符生成ID
    return hashlib.md5(unique_content.encode('utf-8')).hexdigest()

def check_document_exists(es, title, text_prefix):
    """
    检查文档是否已存在
    """
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"match_phrase": {"text": title}},
                    {"match_phrase": {"text": text_prefix[:100]}}
                ]
            }
        },
        "size": 1
    }
    
    results = es.es.search(index=es.index, body=query_body)
    return len(results['hits']['hits']) > 0


# 加载ES配置
ES_CONF_PATH = './configs/es.yaml'
es_info = load_yaml_conf(ES_CONF_PATH)
reference_es = Es(
    host=es_info['host'],
    port=es_info['port'],
    user=es_info['user'],
    password=es_info['password'],
    index=es_info['index']
)

# 清理文本函数
def clean_text(text):
    if not isinstance(text, str):
        return ""
    # 去除控制字符
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
    # 限制文本长度
    if len(text) > 30000:
        return text[:30000]
    return text

# 处理日期格式
def format_date(date_str):
    """转换日期为Elasticsearch可接受的格式"""
    if not isinstance(date_str, str):
        return None
    
    try:
        # 尝试解析为标准ISO格式
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%Y-%m-%dT00:00:00.000Z")  # 返回ISO 8601格式
    except ValueError:
        try:
            # 尝试没有前导零的格式 (如 "2025-6-8")
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            return date_obj.strftime("%Y-%m-%dT00:00:00.000Z")
        except ValueError:
            print(f"无法解析日期: {date_str}")
            return None

# 指定CSV文件路径
csv_path = "./data/weather_data/processed_markdown_documents.csv"

# 读取CSV文件
print(f"读取CSV文件: {csv_path}")
try:
    df = pd.read_csv(csv_path, encoding='utf-8')
    print(f"成功读取，共 {len(df)} 条记录")
except Exception as e:
    print(f"读取CSV失败: {str(e)}")
    exit(1)

# 清理数据
print("清理数据并格式化日期...")
df['text'] = df['text'].apply(clean_text)
df['title'] = df['title'].apply(clean_text)
# 尝试转换日期
df['original_date'] = df['date']  # 保留原始日期值
df['date'] = df['date'].apply(format_date)

# 检查日期转换情况
date_na_count = df['date'].isna().sum()
if date_na_count > 0:
    print(f"警告: {date_na_count} 条记录的日期无法解析")
    # 对于无法解析的日期使用固定值
    df.loc[df['date'].isna(), 'date'] = "2025-01-01T00:00:00.000Z"
    
# 分批导入
batch_size = 5
success_count = 0

# 替换原有的批量导入代码

print(f"开始分批导入数据，每批 {batch_size} 条记录")
duplicate_count = 0
success_count = 0

for i in range(0, len(df), batch_size):
    batch_df = df.iloc[i:i+batch_size]
    
    # 准备批量索引文档
    actions = []
    for _, row in batch_df.iterrows():
        title = clean_text(row['title'])
        text = clean_text(row['text'])
        
        # 检查文档是否已存在
        if check_document_exists(reference_es, title, text):
            print(f"跳过重复文档: {title[:30]}...")
            duplicate_count += 1
            continue
            
        doc = {
            "text": f"[title]{title}[title][text]{text}[text]",
            "url": row['url'],
            "source": os.path.basename(csv_path).split('.')[0],
            "date": row['date']
        }
        
        # 生成唯一ID
        doc_id = generate_document_id(title, text)
        
        actions.append({
            "_index": reference_es.index,
            "_id": doc_id,  # 使用生成的唯一ID
            "_source": doc
        })
    
    if not actions:
        print(f"批次 {i//batch_size + 1}/{(len(df)+batch_size-1)//batch_size}: 没有新文档")
        continue
        
    # 尝试索引这批文档
    try:
        success, failed = helpers.bulk(reference_es.es, actions, stats_only=True)
        success_count += success
        print(f"批次 {i//batch_size + 1}/{(len(df)+batch_size-1)//batch_size}: 成功 {success}, 失败 {failed}")
    except Exception as e:
        print(f"批次 {i//batch_size + 1} 导入失败: {str(e)[:200]}")
        # 尝试逐个导入
        for j, action in enumerate(actions):
            try:
                reference_es.insert(action["_source"], doc_id=action["_id"])
                success_count += 1
                print(f"  - 文档 {j+1} 单独导入成功")
            except Exception as e2:
                print(f"  - 文档 {j+1} 单独导入失败: {str(e2)[:100]}")

print(f"导入完成! 总共成功导入 {success_count}/{len(df)} 条记录, 跳过 {duplicate_count} 条重复记录")