#-*- encoding: utf-8 -*-
'''
queue_consumer.py
Created on 2018/8/311:27
Copyright (c) wangjie
'''
from weibo.util.log_util import LogUtil
import threading
import weibo.action.base_consumer_action as base_consumer_action
import random
import time
#跟scala不一样，类名后边是继承
class Consumer(threading.Thread):
    '''
    消费者进程，主要任务是执行拿到的消费动作
    '''
    #消费者动作失败之后重新尝试的次数，可供外面访问
    _WORK_TRY_NUM = 0

    def __init__(self,queue,name,sleep_time,work_try_num):
        '''
        初始化消费进程
        :param queue: 使用的队列
        :param name: 消费者线程的名称，用其代表消费者的名字
        :param sleep_time: 执行下一次消费动作的休息时间
        :param work_try_num: 每个消费动作允许失败的次数
        :return:
        '''
        super(self.__class__,self).__init__()
        self.queue = queue
        self.name = name
        self.sleep_time = sleep_time
        self.work_try_num = work_try_num
        Consumer._WORK_TRY_NUM = work_try_num

        self.rl = LogUtil().get_logger(log_name='Consumer',file_name='Consumer'  + time.strftime("%Y%m%d", time.localtime()) + '.log')


    def run(self):
        '''
        死循环实现没有消费任务时，消费者线程不死
        :return:
        '''
        while True:
            try:
                #从线程中得到一个消费动作，其在队列中的状态由new转为work
                action = self.queue.get()
                print('consumer name :' + self.name + 'action:' + str(action))
                if not isinstance(action,base_consumer_action.ConsumerAction):
                    self.rl.exception('Action not Consumer base')

                #任务下一次消费动作随机休息的时长，最长不超过设置的上限sleep time
                sleep_time = random.randint(1,self.sleep_time*10)*0.1
                time.sleep(sleep_time)

                action.consumer_thread_name = self.name
                start_time = time.clock()

                #执行得到的消费动作
                re = action.action()
                end_time = time.clock()

                #计算执行消费动作的时间
                work_sec = int(round((end_time - start_time)))

                # #输出消费线程日志
                # self.rl.info("queue name %s finish,sleep time %s\'s,action time %s\'s,"
                #              "action retry %s times,result:%s" % \
                #              (self.name,sleep_time,work_sec,action.try_num,
                #               re.__str__() if re is not None else ''))

                #根据消费动作的结束和该消费动作的失败次数，决定是否再次放入队列中重新尝试
                while(not re[0] and action.try_num < self.work_try_num):
                    #self.rl.info('name' + self.name + 'action.try_num :' + str(action.try_num)+'self.work_try_num:' + str(self.work_try_num))

                    #输出消费线程日志
                    self.rl.info("queue name %s finish,sleep time %s\'s,action time %s\'s,"
                              "action retry %s times,result:%s" % \
                              (self.name,sleep_time,work_sec,action.try_num,
                               re.__str__() if re is not None else ''))
                    #该消费动作的失败次数累加
                    action.try_num += 1
                    time.sleep(self.sleep_time)
                    #向队列放入消费动作，其在队列中的状态为new
                    #self.queue.put(action)
                    #没有达到消费者最大尝试次数，将继续尝试
                    re = action.action()
                    #print(self.queue.qsize())
                #打印最后一次尝试日志
                self.rl.info("queue name %s finish,sleep time %s\'s,action time %s\'s,"
                              "action retry %s times,result:%s" % \
                              (self.name,sleep_time,work_sec,action.try_num,
                               re.__str__() if re is not None else ''))
                #把得到的消费动作的状态在队列中从work转为done,每一次都减一
                self.queue.task_done()



            except:
                self.rl.exception("consumer action exception")




