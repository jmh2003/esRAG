import sys
import os
# 添加当前目录到系统路径
file = os.path.dirname(os.path.abspath(__file__))

from .es import Es
from .utils import load_yaml_conf


# 加载ES配置
# 使用绝对路径
ES_CONF_PATH = '/home/fangly/SearchEngine-main/esRAG/configs/es.yaml'

es_info = load_yaml_conf(ES_CONF_PATH)
reference_es = Es(
    host=es_info['host'],
    port=es_info['port'],
    user=es_info['user'],
    password=es_info['password'],
    index=es_info['index']
)

def search_similar_articles(query_text, size=10):
    """
    搜索与输入文本内容相似的文章
    
    参数:
    query_text: 查询文本
    size: 返回结果数量
    
    返回:
    相似文章列表
    """
    # 使用 match 查询，可以找到包含相似词汇的文档
    query = {
        'method': 'match',
        'field': 'text',
        'q': query_text,
        'size': size
    }
    
    results = reference_es.search(query)
    return results

# 示例使用
if __name__ == "__main__":
    # 测试查询
    query = "天气预报 降水量"
    print(f"搜索与 '{query}' 相关的文章:")
    results = search_similar_articles(query)
    
    print(f"找到 {results['num']} 条相关文章:\n")
    for i, item in enumerate(results['items']):
        print(f"===== 结果 {i+1} =====")
        print(f"标题: {item['title']}")
        print(f"来源: {item.get('url', '未知')}")
        print(f"日期: {item.get('date', '未知')}")
        print(f"正文: {item['text'][:150]}...\n")