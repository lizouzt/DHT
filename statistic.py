# -*- coding: UTF8 -*-
'''
Created on 2015-04-01

@author: tao.z
'''
import pdb
import os,sys,time,logging,json,re 
import socket
import time
from bencode import bdecode
from datetime import date
from threading import Timer,Thread
from utils import get_time_formatter
from settings import *

OUTPUT_STATFILE = 60
END = False
CMD = 'sh flush.sh %s %s %s' % (MQSERVER, MQUSER, MQPWD)

class Statistic(object):
    """download data statistic"""
    FLUSHSTAMP = time.time()
    begin_time = time.time()
    _count_insert = {}
    _count_error = {}
    _count_delete = {}
    _count_repeat = {}
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
	total_repeat = 0
        individualData = ''
        for host in self._count_insert:
            total_success += self._count_insert[host]
            individualData += '  Host %s Success: %d \n' % (host,self._count_insert[host])
        for host in self._count_error:
            total_error += self._count_error[host]
            individualData += '  Host %s Errors: %d \n' % (host,self._count_error[host])
        for host in self._count_delete:
            total_error += self._count_delete[host]
            individualData += '  Host %s Delete: %d \n' % (host,self._count_delete[host])
	for host in self._count_repeat:
            total_repeat += self._count_repeat[host]
            individualData += '  Host %s Repeat: %d \n' % (host,self._count_repeat[host])
        content = ['torrents:']
        interval = time.time() - self.begin_time
        content.append('  PID: %s \n' % os.getpid())
        content.append('  Time: %s \n' % time.strftime('%Y-%m-%d %H:%M:%S'))
        content.append('  Run time: %s \n' % get_time_formatter(interval))
        content.append('  Get invalid TCP nums: %d \n' % self._count_invalid_msg)
        content.append('  Get BT nums: %d \n' % total_success)
        content.append('  Repeat nums: %d \n' % total_repeat)
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

        _log = self.logger.info
        if t == 'error':
            _log = self.logger.error
        elif t == 'warning':
            _log = self.logger.warning
        elif t == 'debug':
            _log = self.logger.debug
        _log(info % dic)

    def dht_record(self, t, *dic):
	'''
	0: 	insert
	-1:	repeat
	1:	error
	2:	invalid
	3:	delete
	'''
        if t is None:
            return -1
        elif t == '0':
            if dic[0] not in self._count_insert:
                self._count_insert[dic[0]] = 1
            else:
                self._count_insert[dic[0]] += 1
        elif t == '-1':
	    if dic[0] not in self._count_repeat:
		self._count_repeat[dic[0]] = 1
	    else:
		self._count_repeat[dic[0]] += 1
        elif t == '1':
            if dic[0] not in self._count_error:
                self._count_error[dic[0]] = 1
            else:
                self._count_error[dic[0]] += 1
            self.log('DB Err from: %s ---> %s', dic, type='error')
        elif t == '2':
            self._count_invalid_msg += 1
            self.log('invalid tcp message from %s::%d', dic, type='error')
        elif t == '3':
            if dic[0] not in self._count_delete:
                self._count_delete[dic[0]] = 1
            else:
                self._count_delete[dic[0]] += 1
        else:
            pass

    def dht_log_sys(self,info,address):
        _type = info['i']
        if _type in ['0','3','-1']:
            self.dht_record(_type, address[0])
        elif _type == '1':
            self.dht_record(_type, address[0], info['m'])
        else:
            pass

    def referer(self, ref):
        _func = self.dht_log_sys
        if ref == 'dht':
            _func = self.dht_log_sys
        elif ref == 'web':
            _func = self.web_log_sys
        elif ref == 'needrestart':
            _time = time.time()
            if _time - self.FLUSHSTAMP > 600:
                self.FLUSHSTAMP = _time
                os.system(CMD)
                self.log('Flush.')
                return False
        return _func

    def check_token(self, data, address):
        info = bdecode(data)
        if info.has_key('t') and info['t'] == TOKEN:
            _ref = info['r'] if info.has_key('r') else ''
            _func = self.referer(_ref)
            _func and _func(info,address)
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
                #if data: Thread(target=self.check_token, args=(data, address)).start()
		if data: self.check_token(data,address)
            except KeyboardInterrupt:
                sock.close()
                END = True
                self.log('STOPPED')
                exit()
            except Exception,e:
                print e

if __name__ == '__main__':
    if len(sys.argv) == 3:
        Statistic(sys.argv[1])
    else:
        Statistic()
