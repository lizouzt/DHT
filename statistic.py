# -*- coding: UTF8 -*-
'''
Created on 2015-04-01

@author: Elfer
'''
import pdb
import os,sys,time,logging,json,re 
import urllib2
import socket
import StringIO
import gzip
import hashlib
import chardet
from datetime import date
from string import Template
from threading import Timer,Thread
from utils import get_time_formatter
from dbManage import DBManage
from settings import *

OUTPUT_STATFILE = 60
END = False
MANAGE = DBManage()

class Statistic(object):
    """download data statistic"""
    def __init__(self, file_name):
        super(Statistic, self).__init__()
        self.begin_time = time.time()
        self.logMsg = {
            1: "received tcp request",
            2: "TCP invalid msg from: %s::%d",
            3: "Bdecode Failed %s",
            4: "Server couldn't fullfill the request. Error code: %s",
            5: "Failed to reach. Reason: %s",
            6: "BT download error: %s",
            7: "Meta decode error: %s",
        }
        self._count_success = 0
        self._count_receive_tcp = 0
        self._count_invalid_msg = 0
        self._count_bdecode_error = 0
        self._count_btdownload_error = 0
        self._count_decode_error = 0
        self._stat_file = file_name
        Timer(OUTPUT_STATFILE, self.output_stat).start()
        self.initLogger()

    def initLogger(self):
        self.logger = logging.getLogger('btAnalyzer')
        fh = logging.FileHandler('log-downloader-%s.log' % date.today(), 'wb')
        sh = logging.StreamHandler()

        fhFmt = logging.Formatter('%(asctime)s [line: %(lineno)d] %(levelname)s %(message)s')
        shFmt = logging.Formatter('%(levelname)s %(message)s')

        fh.setFormatter(fhFmt)
        sh.setFormatter(shFmt)

        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(fh)
        self.logger.addHandler(sh)

    def output_stat(self):
        global END
        content = ['torrents:']
        interval = time.time() - self.begin_time
        content.append('  PID: %s' % os.getpid())
        content.append('  Time: %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
        content.append('  Run time: %s' % get_time_formatter(interval))
        content.append('  Get BT nums: %d' % self._count_success)
        content.append('  Get TCP nums: %d' % self._count_receive_tcp)
        content.append('  Get invalid TCP nums: %d' % self._count_invalid_msg)
        content.append('  DownLoad error nums: %d' % self._count_btdownload_error)
        content.append('  BDecode error nums: %d'% self._count_bdecode_error)
        content.append('  Meta decode error nums: %d' % self._count_decode_error)
        content.append('\n')
        try:
            with open(self._stat_file, 'wb') as f:
                f.write('\n'.join(content))
        except Exception as err:
            self.log('output_stat error %s', str(err))

        if not END: 
            Timer(OUTPUT_STATFILE, self.output_stat).start()
        else:
            exit()

    def log(self, info, *args, **kwargs):
        t = 'info'
        if kwargs.has_key('type'):
            t = kwargs['type']
        log = self.logger.info
        if t == 'error':
            log = self.logger.error
        elif t == 'warning':
            log = self.logger.warning
        elif t == 'debug':
            log = self.logger.debug
        log(info % args)

    def record(self, t, *dic):
        if t is None:
            return -1
        elif t == 0:
            self._count_success += 1
        elif t == 1:
            self._count_receive_tcp += 1
        elif t == 2:
            self._count_invalid_msg += 1
            self.log(self.logMsg[t], dic, type='error')
        elif t == 3:
            self._count_bdecode_error += 1
            self.log(self.logMsg[t], dic, type='error')
        elif t in (5,6):
            self._count_btdownload_error += 1
            self.log(self.logMsg[t], dic, type='info')
        elif t == 4:
            self._count_btdownload_error += 1
        elif t == 7:
            self.log(self.logMsg[t], dic, type="warning")
            self._count_decode_error += 1
        else:
            pas