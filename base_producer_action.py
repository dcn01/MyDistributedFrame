#-*- encoding: utf-8 -*-
'''
base_producer_action.py
Created on 2018/8/311:13
Copyright (c) wangjie
'''

class ProducerAction(object):
    '''
    生产者的基类
    '''

    def queue_items(self):
        '''
        得到消费任务，用于放入到队列中，供消费者进程使用
        :return:
        '''
        pass
