#-*- encoding: utf-8 -*-
'''
demo_crawler.py
Created on 2018/8/415:13
Copyright (c) wangjie
'''
import weibo.action.base_producer_action as base_producer_action
import weibo.action.base_consumer_action as base_consumer_action
import weibo.action.queue_producer as queue_producer
from  weibo.util.log_util import LogUtil
from weibo.util.db_util import DBUtil
import time
import queue
import datetime
class DemoCrawlerProcuer(base_producer_action.ProducerAction):
    '''
    爬虫producer
    '''
    def __init__(self):
        '''
        初始化
        :return:
        '''
        super(self.__class__,self).__init__()
        self.rl = LogUtil().get_logger('demo_crawler_producer','demo_crawler_producer' + time.strftime("%Y%m%d", time.localtime()) + '.log')

    def queue_items(self):
        '''
        需要将消费者append到list中，返回list
        :return:
        '''
        url_sql = """select id,url from seed_url where is_run=1 and type=1"""

        dict = DBUtil().read_dict(url_sql)
        return_list = []
        for record in dict:
            id = record['id']
            url = record['url']
            c = DemoCrawlerConsumer(id,url)
            return_list.append(c)
        return return_list


class DemoCrawlerConsumer(base_consumer_action.ConsumerAction):
    def __init__(self,id,url):
        super(self.__class__,self).__init__()
        self.id = id
        self.url = url
        self.rl = LogUtil().get_logger('demo_crawler_consumer','demo_crawler_consumer' + time.strftime("%Y%m%d", time.localtime()) + '.log')

    def action(self):
        '''
        爬虫逻辑,self.result()方法继承父类，两个元素，第一个元素为爬取是否成功的标志位，第二个元素最好为爬取的url id，传给success_action/fail_action使用
        :return:
        '''
        result = True
        try:
            #爬虫逻辑
            #update_sql = 'update seed_url set url_status=%s where id=%s'
            dt=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            insert_sql = """insert into url_result (url,rs,occur_time) values ('%s','%s','%s') """ % (self.url,self.id,dt)
            DBUtil().execute(insert_sql)
        except:
            result = False
        return self.result(result,[self.id])

    def success_action(self,values):
        '''
        爬取成功的处理方法，拿到爬取操作的id进行处理和判断
        :param values:
        :return:
        '''
        url_id = values[0]
        print(type(url_id))
        dt=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_sql = """update seed_url set result=1,occur_time='%s' where id=%s""" % (dt,url_id)

        DBUtil().execute(update_sql)

    def fail_action(self,values):
        '''
        爬取失败的处理方法，拿到爬取操作的id进行处理和判断
        :param values:
        :return:
        '''
        url_id = values[0]
        dt=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_sql = """update seed_url set result=0,occur_time='%s' where id=%s""" % (dt,url_id)
        DBUtil().execute(update_sql)


if __name__ == '__main__':
    #初始化使用的队列
    q = queue.Queue()

    #初始化生产者动作
    pp = DemoCrawlerProcuer()

    #初始化生产者
    #参数说明：1/队列名；2/生产者；3/任务名称；4/消费者启动的消费线程数；5/每多长时间生产者将seed_url的爬取链接放入队列中，可实现
    #菜品每小时，评论每分钟；6/消费者线程每次工作的时间间隔，为了反爬虫；7/每个爬虫任务重试次数
    p = queue_producer.Producer(q,pp,'demo_crawler',1,10,1,4)

    #启动整个生产和消费任务
    p.start_work()
