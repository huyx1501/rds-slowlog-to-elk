#!/usr/bin/env python3
# -*- encoding=UTF8 -*-
# Author: huxy1501

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Search
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException, ServerException
from aliyunsdkrds.request.v20140815.DescribeDBInstancesRequest import DescribeDBInstancesRequest
from aliyunsdkrds.request.v20140815.DescribeSlowLogRecordsRequest import DescribeSlowLogRecordsRequest
from datetime import datetime, timedelta
import json
import traceback
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 阿里云配置
API_Key = 'ID'
API_Secret = 'Secret'
RegionId = "cn-shenzhen"  # 阿里云区域代码
DBName = "RDS DBName"  # 指定要获取慢查询的数据库名，获取全部则留空

# ES服务器信息
ES_Host = ["192.168.2.1"]  # ES节点地址，集群节点以逗号分隔
ES_Port = 9200  # ES端口
ES_Protocol = "HTTPS"  # 连接协议，HTTP或HTTPS
ES_Http_Auth = True  # ES是否开启了认证，如果开启，请在下面填写账号密码
ES_Http_User = "User"
ES_Http_Pass = "Password"
ES_Index = "index_slow_sql"


class ElkPusher(object):
    def __init__(self):
        if isinstance(ES_Host, list) and len(ES_Host) > 1:
            self.es_client = Elasticsearch(
                ES_Host,
                http_auth=[ES_Http_User, ES_Http_Pass] if ES_Http_Auth else None,
                scheme=ES_Protocol,
                port=ES_Port,
                use_ssl=True if ES_Protocol == "HTTPS" else False,
                ssl_show_warn=False,
                verify_certs=False,

                sniff_on_start=True,
                sniff_on_connection_fail=True,
                sniffer_timeout=60
            )
        else:  # 兼容单节点ES
            self.es_client = Elasticsearch(
                ES_Host,
                http_auth=[ES_Http_User, ES_Http_Pass] if ES_Http_Auth else None,
                scheme=ES_Protocol,
                port=ES_Port,
                use_ssl=True if ES_Protocol == "HTTPS" else False,
                ssl_show_warn=False,
                verify_certs=False,
            )

    def get_last_log(self, ins_id):
        """
        从ES获取指定实例最后一条插入的日志，如果当天和前一天均没有日志，则返回空
        :param str ins_id: 实例ID
        :return dict: 返回查询到的日志内容
        """
        try:
            index_name = ES_Index + datetime.now().strftime("-%Y.%m.%d")
            s = Search(using=self.es_client, index=index_name).query("match", InstanceID=ins_id) \
                .sort({"ExecutionStartTime": {"order": "desc"}})
            es_logs = s.execute()
        except NotFoundError:  # 当天的index还没创建，取前一天的最后一条数据
            try:
                index_name = ES_Index + (datetime.now() + timedelta(days=-1)).strftime("-%Y.%m.%d")
                s = Search(using=self.es_client, index=index_name).query("match", InstanceID=ins_id) \
                    .sort({"ExecutionStartTime": {"order": "desc"}})
                es_logs = s.execute()
            except NotFoundError:  # 如果前一天仍然没有产生日志，则返回空
                return None
        return es_logs["hits"]["hits"][0]

    def save_log(self, log, index_name):
        """
        保存字典格式的日志到ElasticSearch
        :param str log: 要保存的日志，应该为json格式的字符串
        :param str index_name: ES索引名称
        :return: True or False
        """
        try:
            self.es_client.index(index=index_name, doc_type="slow_sql", body=log)
            # print(log)
            # print(u"保存日志到%s成功" % index_name)
        except Exception:
            traceback.print_exc()
            return False
        else:
            return True


class RdsSlowLog(object):
    """
    获取RDS慢查询日志写入ElasticSearch
    """
    def __init__(self):
        self.api_key = API_Key
        self.api_secret = API_Secret
        self.region_id = RegionId

        self.es_handler = ElkPusher()
        self.AcsClient = AcsClient(
            self.api_key,
            self.api_secret,
            self.region_id
        )

    def get_instance_list(self, page_size=30):
        """
        获取RDS实例列表
        :return list: 返回嵌套了实例信息字典的实例列表
        """
        rds_instance_list = list()
        request = DescribeDBInstancesRequest()
        request.set_accept_format('json')
        request.set_PageSize(page_size)
        received_item = 0
        page_num = 1
        while True:
            request.set_PageNumber(page_num)
            response = self.AcsClient.do_action_with_exception(request)
            response_dic = json.loads(response)
            total_count = response_dic["TotalRecordCount"]
            instances = response_dic['Items']['DBInstance']
            for rds_instance in instances:
                rds_instance_info = dict()
                rds_instance_info["InstanceID"] = rds_instance['DBInstanceId']
                rds_instance_info["DBInstanceDescription"] = rds_instance['DBInstanceDescription']
                rds_instance_list.append(rds_instance_info)
            received_item += page_size
            if total_count > received_item:  # 翻页
                page_num += 1
            else:
                return rds_instance_list

    def log_transfer(self):
        """
        从阿里云获取日志写入ES
        :return int: 累计处理日志数量
        """
        instance_list = self.get_instance_list()
        log_length = 0
        for instance in instance_list:
            instance_id = instance["InstanceID"]
            instance_name = instance["DBInstanceDescription"]
            instance_log_list = self.get_logs(instance_id)
            print(u"[%s] 开始获取实例[%s]的慢查询日志" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), instance_id))
            for log in instance_log_list:  # 为每条日志额外加上实例信息
                log["InstanceID"] = instance_id
                log["InstanceName"] = instance_name
                log["SQLBrief"] = log["SQLText"][0:200]  # 截取头部200个字符作为摘要
                # print(log)
                log_date = self.get_cst_from_utc(log["ExecutionStartTime"])
                es_index = ES_Index + "-" + log_date
                if self.es_handler.save_log(json.dumps(log), es_index):  # 将日志写入ES
                    log_length += 1
                else:
                    exit(u"[%s] 保存日志到ES出错" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        else:
            return log_length

    def get_logs(self, ins_id, page_size=30):
        """
        从阿里云获取指定RDS实例的慢查询日志
        :param str ins_id: RDS实例ID
        :param int page_size: 单页日志条数，多余此数量的翻页处理
        :return list: 指定实例的慢查询日志列表
        """
        start_time = self.get_last_time(ins_id)  # 获取最后一条日志的时间
        end_time = datetime.strftime(datetime.utcnow() + timedelta(minutes=-1), '%Y-%m-%dT%H:%MZ')
        received_item = 0
        page_num = 1
        log_list = list()

        request = DescribeSlowLogRecordsRequest()
        request.set_accept_format("json")
        request.set_StartTime(start_time)
        request.set_EndTime(end_time)
        request.set_PageSize(page_size)
        request.set_DBInstanceId(ins_id)
        if DBName:
            request.set_DBName(DBName)

        while True:
            # print(u"正在获取实例[%s]第[%d]页日志" % (ins_id, page_num))
            request.set_PageNumber(page_num)
            response = self.AcsClient.do_action_with_exception(request)
            response_dic = json.loads(response)
            total_count = response_dic["TotalRecordCount"]
            for log in response_dic["Items"]["SQLSlowRecord"]:
                log_list.append(log)
            received_item += page_size
            if total_count > received_item:  # 翻页
                page_num += 1
            else:
                return log_list

    def get_last_time(self, ins_id):
        """
        获取上次保存的最后一条日志的时间，最多获取前一天的日志，如果前一天也没有产生日志，则取前一天的0点作为返回值
        :return str: 字符串格式的UTC时间
        """
        try:
            last_log = self.es_handler.get_last_log(ins_id)
            last_time = last_log["_source"]["ExecutionStartTime"]
            next_time = datetime.strptime(last_time, "%Y-%m-%dT%H:%M:%SZ") + timedelta(minutes=1)
            return next_time.strftime("%Y-%m-%dT%H:%MZ")
        except (KeyError, IndexError, TypeError):
            print(u"[%s] 未查询到最后一条日志，重新开始获取2天内的慢查询" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            return datetime.strftime(datetime.utcnow() + timedelta(days=-1), '%Y-%m-%dT00:00Z')

    @staticmethod
    def get_cst_from_utc(utc_time_str):
        """
        将字符串的UTC时间转换成CST时间
        :param str utc_time_str: 字符串格式的UTC时间
        :return str: 字符串格式的CST日期
        """
        cst_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=8)
        return cst_time.strftime("%Y.%m.%d")


if __name__ == "__main__":
    log_checker = RdsSlowLog()
    try:
        log_len = log_checker.log_transfer()
        print(u"[%s] 处理成功，累计处理日志[%d]条" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), log_len))
    except (ValueError, IndexError, KeyError, ClientException, ServerException):
        traceback.print_exc()
