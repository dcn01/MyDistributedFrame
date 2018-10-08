#-*- encoding: utf-8 -*-
'''
base_consumer_action.py
Created on 2018/8/311:01
Copyright (c) wangjie
'''
class ConsumerAction(object):
    '''
    消费者基类
    '''
    def __init__(self):
        self.try_num = 1
        self.consumer_thread_name = ''

    def action(self):
        '''
        消费者动作抽象方法，根据不同的消费需求进行实现
        :return:
        '''
        pass

    def result(self,is_success,values):
        '''
        根据消费动作（action）的结果，选择是执行success_action还是fail_action
        :param is_success:
        :param values:
        :return:
        '''
        return_value = []
        return_value.append(is_success)

        if not is_success:
            self.fail_action(values)
        else:
            self.success_action(values)

        for re in values:
            return_value.append(re)

        return return_value

    def fail_action(self,values):
        '''
        执行消费动作完成之后的成功动作
        :param values:
        :return:
        '''
        pass

    def success_action(self,values):
        '''
        执行消费动作完成之后的成功动作
        :param values:
        :return:
        '''
        pass