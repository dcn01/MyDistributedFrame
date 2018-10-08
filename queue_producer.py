#-*- encoding: utf-8 -*-
'''
queue_producer.py
Created on 2018/8/313:55
Copyright (c) wangjie
'''
from weibo.util.log_util import LogUtil
import weibo.action.base_producer_action as base_producer_action
import threading,time
import weibo.action.queue_consumer as queue_consumer

class Producer(threading.Thread):
    '''
    生产者线程
    '''

    def __init__(self,queue,action,name,max_num,sleep_time,work_sleep_time,work_try_num):
        '''
        初始化生产线程
        :param queue: 使用的队列
        :param action: 生产者动作
        :param name: 生产者名称
        :param max_num: 启动的消费者的数量
        :param sleep_time: 执行下一次生产者动作时的休息时间
        :param work_sleep_time: 每个消费者的休息时间
        :param work_try_num: 每个消费动作允许失败的次数
        :return:
        '''
        super(self.__class__,self).__init__()
        self.queue = queue
        self.action = action
        self.name = name
        self.max_num = max_num
        self.sleep_time = sleep_time
        self.work_sleep_time = work_sleep_time
        self.work_try_num = work_try_num
        self.rl = LogUtil().get_logger(log_name='Producer',file_name='Producer' + self.name + time.strftime("%Y%m%d", time.localtime()) + '.log')
        if not isinstance(self.action,base_producer_action.ProducerAction):
            self.rl.exception('Action not Producer base')

    def run(self):
        #缓存生产者产生的消费任务，用于在消费者线程有空闲时进行任务的填充
        #死循环实现生产者每sleep_time时间将seed_url再次放入队列中，实现每多长时间爬取一次
        action_list = []
        while True:
            try:
                start_time = time.clock()

                #当缓存消费任务为空时，调用生产动作拿到消费任务
                if len(action_list) == 0:
                    action_list = self.action.queue_items()
                    self.rl.info("action_list : " + str(len(action_list)))

                #日志输出本次的消费任务有多少
                total_times = len(action_list)
                self.rl.info('get queue %s total items is %s' % (self.name,total_times))

                while True:
                    #当生产者的消费任务都交给了消费者线程，跳出循环
                    if len(action_list) == 0:
                        break

                    #在队列中work状态的消费动作有多少
                    unfinished_tasks = self.queue.unfinished_tasks
                    #print('unfinished_tasks:' + str(unfinished_tasks) + 'max_num:' + str(self.max_num))
                    #当work状态的消费动作小于消费者线程数时就往队列中派发一个消费任务
                    if unfinished_tasks <= self.max_num:
                        action = action_list.pop()
                        self.queue.put(action)


                end_time = time.clock()
                #计算生产者完成本次生产任务的时间和频次
                sec = int(round((end_time - start_time)))
                min = int(round(sec / float(60)))

                self.rl.info("put queue %s total items is %s,total time is %s\'s,(at %s items/min)" %\
                             (self.name,total_times,sec,
                              int(total_times) if min == 0 else round(float((total_times/float(min))),2)))
                time.sleep(self.sleep_time)
            except:
                self.rl.exception("queue producer exception")


    def start_work(self):
        '''
        启动生产者线程和根据消费者线程的数量设置启动对应数量的消费者线程数
        :return:
        '''
        for i in range(0,self.max_num):
            qc = queue_consumer.Consumer(self.queue,self.name + '_' + str(i),self.work_sleep_time,self.work_try_num)
            qc.start()


        #默认5秒钟后启动生产者
        time.sleep(5)
        self.start()