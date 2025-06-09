# esRAG
How to use Elasticsearch! Several steps on a Linux server!

## Complete Elasticsearch Configuration Deployment and Application

> Write this down to avoid forgetting.

### Linux Download

[Download Elasticsearch | Elastic](https://www.elastic.co/cn/downloads/elasticsearch)  

After downloading the tar file, extract it. It's better to choose a newer version if possible.

```bash
tar -zxvf xxx.tar.gz
```

After extraction, navigate to the `bin` folder within the extracted directory.

### Start

Start Elasticsearch by running:

```bash
./elasticsearch
```

### Configuration

Check the output from `./elasticsearch`.

You can obtain relevant information such as usernames, ports, and passwords (this is not a conventional operation).

**Note: Ensure that `./elasticsearch` is running when performing the following database operations!**

### Data Import

Place the configuration output in `configs/es.yaml`. Then run:

```bash
python data_to_json_es.py
```

This command will import the data into the database.

### Query

Run the following command to get the query results:

```bash
python search.py
```




# esRAG
How to use elasticsearch! Several steps in linux server!

## 完整的elasticsearch配置部署和应用

> 写一份避免忘了

### linux下载

[Download Elasticsearch | Elastic](https://www.elastic.co/cn/downloads/elasticsearch)

下载后的tar文件进行解压，选择新一些的可能好些

```
tar -zxvf xxx.tar.gz
```

然后解压后得到的文件夹下面，找到bin文件夹

### 启动

启动elasticsearch 即可

```bash
./elasticsearch
```



### 配置

查看./elasticsearch得到的输出

可以得到相关的用户名、端口、账号密码等（非常规操作）

**注意：在执行下列对数据库操作的时候，请保证./elasticsearch启动！**

### 导入数据

将得到的配置输出放置在  `configs/es.yaml`当中，直接运行

```bash
python data_to_json_es.py
```

这个命令将数据导入到数据库中。

### 查询

然后运行下面的命令，即可得到查询的结果：

```
python search.py
```

