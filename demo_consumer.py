#-*- encoding: utf-8 -*-
'''
demo_consumer.py
Created on 2018/8/314:38
Copyright (c) 2018/8/3
'''
import weibo.action.base_consumer_action as base_consumer_action
import weibo.action.base_producer_action as base_producer_action
import weibo.action.queue_producer as queue_producer
import weibo.action.queue_consumer as queue_consumer
import queue
class DemoConsumer(base_consumer_action.ConsumerAction):

    def __init__(self,text):
        '''
        初始化消费者实现类
        :param text: 消费者要处理的数据
        :return:
        '''
        super(self.__class__,self).__init__()
        self.text = text
        #self.fail_time

    def action(self):
        '''
        消费者的具体需求实现
        :return:消费动作的处理结果，用于消费者线程的日志打印
        '''
        result = True
        r_test = ''
        try:
            r_test = self.text/0
        except:
            result = False

        return self.result(result,[r_test])

    def fail_action(self,values):

        if self.try_num >= queue_consumer.Consumer._WORK_TRY_NUM:
            pass

    def success_action(self,values):
        pass


class DemoProducer(base_producer_action.ProducerAction):

    def queue_items(self):
        '''
        生成指定的消费动作
        :return:消费者动作的合集
        '''
        list = []
        for i in range(0,3):
            c = DemoConsumer(i)
            list.append(c)

        return list


if __name__ == '__main__':
    #初始化使用的队列
    q = queue.Queue()

    #初始化生产者动作
    pp = DemoProducer()

    #初始化生产者
    p = queue_producer.Producer(q,pp,'wangjie',1,100,1,4)

    #启动整个生产和消费任务
    p.start_work()
    print('----------------')

