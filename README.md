# rds slowlog to elk
通过API查询阿里云RDS实例慢查询日志，导入到ELK方便进行统计分析

## 环境要求
python3.6+  其他版本未测试

## 使用方法
### 安装依赖包
```shell
/usr/bin/env pip3 -r requirements.txt
```

### 修改配置
修改rds_slow_log.py开头的阿里云和ES服务器配置
配置参数说明:
```
    API_Key: 阿里云API Key，需要有RDS的只读权限
    API_Secret: 密钥
    RegionId: 阿里云区域代码，请从阿里官方查找各区域的代码
    DBName: 指定要获取慢查询的数据库名，如果需要获取实例中全部数据库的慢日志请留空
```
```
    ES_Host: ES节点地址，如果是集群，节点间以逗号分隔
    ES_Port: ES端口
    ES_Protocol: 连接ES使用的连接协议，HTTP或HTTPS
    ES_Http_Auth: ES是否开启了认证，如果开启，请在下面填写账号密码
    ES_Http_User: 连接ES的账号
    ES_Http_Pass: 密码
    ES_Index: index名称，默认会按名称每日生产一个index
```

### 运行
```shell
/usr/bin/env python3 ./rds_slow_log.py
```
运行成功提示:
```
处理成功，累计处理日志[XXX]条
```