import os
from es import Es
from utils import load_yaml_conf

# 使用绝对路径加载配置
ES_CONF_PATH = '/home/fangly/SearchEngine-main/esRAG/configs/es.yaml'
es_info = load_yaml_conf(ES_CONF_PATH)

reference_es = Es(
    host=es_info['host'],
    port=es_info['port'],
    user=es_info['user'],
    password=es_info['password'],
    index=es_info['index']
)

def remove_duplicate_documents():
    """
    删除 Elasticsearch 中的重复文档
    基于标题和内容前100个字符进行查重
    """
    print("开始查找重复文档...")
    
    # 1. 获取所有文档
    query = {
        'method': 'match_all',
        'size': 10000  # 调整为适合您数据集的大小
    }
    
    results = reference_es.search(query)
    print(f"共找到 {results['num']} 条文档")
    
    # 2. 创建一个字典来存储唯一文档
    unique_docs = {}
    duplicates = []
    
    # 3. 识别重复文档
    for item in results['items']:
        # 创建唯一键 (标题 + 内容前100字符)
        title = item.get('title', '')
        text = item.get('text', '')[:100]  # 取内容前100个字符
        unique_key = f"{title}_{text}"
        
        if unique_key in unique_docs:
            # 这是一个重复文档
            duplicates.append(item['_id'])
        else:
            # 这是一个新文档
            unique_docs[unique_key] = item['_id']
    
    print(f"发现 {len(duplicates)} 个重复文档")
    
    # 4. 删除重复文档
    if duplicates:
        for doc_id in duplicates:
            print(f"删除文档 ID: {doc_id}")
            reference_es.delete_by_id(doc_id)
        
        print(f"已成功删除 {len(duplicates)} 个重复文档")
    else:
        print("未发现重复文档")

if __name__ == "__main__":
    remove_duplicate_documents()