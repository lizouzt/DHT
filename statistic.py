# -*- coding: UTF8 -*-
'''
Created on 2015-04-01

@author: Elfer
'''
import pdb
import os,sys,time,logging,json,re 
import socket
from bencode import bdecode
from datetime import date
from threading import Timer
from utils import get_time_formatter
from settings import *

OUTPUT_STATFILE = 10
END = False

class Statistic(object):
    """download data statistic"""
    begin_time = time.time()
    _count_insert = {}
    _count_error = {}
    _count_invalid_msg = 0
    def __init__(self, file_name='dht.stat'):
        self._stat_file = file_name
        Timer(OUTPUT_STATFILE, self.output_stat).start()
        self.initLogger()
        self.start()

    def initLogger(self):
        self.logger = logging.getLogger('statistic')
        fh = logging.FileHandler('log-statistic-%s.log' % date.today(), 'wb')
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

        total_success = 0
        total_error = 0
        individualData = ''
        for host in self._count_insert:
            total_success += self._count_insert[host]
            individualData += '  Host %s Success: %d \n' % (host,self._count_insert[host])
        for host in self._count_error:
            total_error += self._count_error[host]
            individualData += '  Host %s Errors: %d \n' % (host,self._count_error[host])

        content = ['torrents:']
        interval = time.time() - self.begin_time
        content.append('  PID: %s \n' % os.getpid())
        content.append('  Time: %s \n' % time.strftime('%Y-%m-%d %H:%M:%S'))
        content.append('  Run time: %s \n' % get_time_formatter(interval))
        content.append('  Get invalid TCP nums: %d \n' % self._count_invalid_msg)
        content.append('  Get BT nums: %d \n' % total_success)
        content.append('  Error nums: %d \n' % total_error)
        content.append(individualData)
        content.append('\n')

        try:
            with open(self._stat_file, 'wb') as f:
                f.write('\n'.join(content))
        except Exception as err:
            self.log('output_stat error %s', str(err))

        if not END: 
            Timer(OUTPUT_STATFILE, self.output_stat).start()
        else:
            exit(1)

    def log(self, info, *dic, **kwargs):
        t = 'info'
        if kwargs.has_key('type'):
            t = kwargs['type']
        dic = dic[0] if dic is not () else ()

        log = self.logger.info
        if t == 'error':
            log = self.logger.error
        elif t == 'warning':
            log = self.logger.warning
        elif t == 'debug':
            log = self.logger.debug
        log(info % dic)

    def record(self, t, *dic):
        if t is None:
            return -1
        elif t == 0:
            if dic[0] not in self._count_insert:
                self._count_insert[dic[0]] = 1
            else:
                self._count_insert[dic[0]] += 1
        elif t == 1:
            if dic[0] not in self._count_error:
                self._count_error[dic[0]] = 1
            else:
                self._count_error[dic[0]] += 1
            self.log('DB Err from: %s ---> %s', dic, type='error')
        elif t == 2:
            self._count_invalid_msg += 1
            self.log('invalid tcp message from %s::%d', dic, type='error')
        else:
            pass

    def check_token(self, data, address):
        info = bdecode(data)
        if info.has_key('t') and info['t'] == TOKEN:
            if info['i'] == '0':
                self.record(0, address[0])
            elif info['i'] == '1':
                self.record(1, address[0], info['m'])
        else:
            self.record(2, address[0], address[1])

    def start(self):
        global END
        self.log('Start.')
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", DLPORT))
        while True:
            try:
                (data, address) = sock.recvfrom(256)
                if data: self.check_token(data, address)
            except KeyboardInterrupt:
                self.log('STOPPED')
                sock.close()
                END = True
                exit(1)
            except Exception,e:
                print e

if __name__ == '__main__':
    if len(sys.argv) == 3:
        Statistic(sys.argv[1])
    else:
        Statistic()