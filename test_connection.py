from elasticsearch import Elasticsearch

# 直接下载 elasticsearch 然后再linux下面解压，运行bin/elasticsearch，注意版本要高一些的
# 然后查看输出当中的端口、ip、密码等等，记录在python脚本中用于访问数据库


# Elasticsearch 服务的地址和端口
es_host = "100.75.32.102"
es_port = 9200

# 用户名和密码
es_user = "elastic"
es_password = "*T+uMmlqgE8aS2q6DwtS"

# 创建 Elasticsearch 客户端
es = Elasticsearch(
    f"https://{es_host}:{es_port}",
    http_auth=(es_user, es_password),
    verify_certs=False  # 如果使用自签名证书，可以禁用证书验证
)

# 测试连接
try:
    # 发送一个简单的查询请求
    response = es.ping()
    if response:
        print("连接成功！")
    else:
        print("连接失败。")
except Exception as e:
    print(f"连接失败，错误信息：{e}")