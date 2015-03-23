# -*- coding: UTF8 -*-
'''
Created on 2015-03-11

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
from bencode import bdecode,bencode
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
			pass

class Analyze(Statistic):
	def __init__(self,statistic_file='statistic.stat',hashlist_file=None):
		Statistic.__init__(self,statistic_file)
		if not hashlist_file:
			self.start()
		else:
			_hashlist_file = hashlist_file
			with open(_hashlist_file) as f:
				self._infohash_list = json.load(f)
			
			for infohash in self._infohash_list:
				self.analyzer(infohash)

	def analyzer(self, infohash):
		content = self.download(infohash)
		if content:
			try:
				content = bdecode(content)
			except Exception, e:
				self.record(3, str(e))
			meta = self.getTorrentInfo(content)
			self.record(0)
			MANAGE.saveTorrent(meta)

	def getTorrentInfo(self, content):
		metadata = content['info']
		encoding = None
		if 'encoding' in metadata:
			encoding = metadata['encoding']
		else:
			_encode = chardet.detect(metadata['name'])['encoding']
			encoding = _encode if _encode is not None else 'utf-8'

		meta = {
			'info_hash': hashlib.sha1(bencode(metadata)).digest().encode('hex'),
			'announce': content['announce'],
			'announce_list': 1,
			'media_type': None
		}

		try:
			meta['name'] = metadata['name'].decode(encoding)
		except Exception,e:
			self.record(7, str(e))
			meta['name'] = metadata['name'].decode('utf-8')
		
		if re.search(RVIDEO, meta['name']):
			meta['media_type'] = 'video'
		
		elif re.search(RAUDIO, meta['name']):
			meta['media_type'] = 'audio'

		if 'announce-list' in content:
			meta['announce_list'] = len(content['announce-list'])
		
		if 'creation date' in content:
			meta['creation_date'] = content['creation date']

		if 'files' in metadata:
			total_size = 0
			files = []
			_count = 26
			for fd in metadata['files']:
				if _count == 0:
					break
				_count -= 1
				_d = {'size': 0}
				_path = []
				for p in fd['path']:
					_ascii = ''
					try:
						_ascii = p.decode(encoding)
					except Exception:
						_encode = chardet.detect(p)['encoding']
						_encode = _encode if _encode is not None else 'utf-8'
						_ascii = p.decode(_encode)
					_path.append(_ascii)
				_d['path'] = os.path.join(*_path)
				if 'size' in fd:
					_d['size'] = fd['size']
				elif 'length' in fd:
					_d['size'] = fd['length']
				files.append(_d)
				total_size += _d['size']

			meta['total_size'] = total_size
			meta['num_files'] = len(metadata['files'])
			meta['files'] = json.dumps(files, ensure_ascii=False)
		else:
			_d = {}
			_d['path'] = meta['name']
			_d['size'] = metadata['length']
			meta['num_files'] = 1
			meta['files'] = json.dumps([_d], ensure_ascii=False)
			meta['total_size'] = metadata['length']
		return meta

	def download(self, infohash):
	    infohash = infohash.upper()
	    btfound = False
	    for site in BTSTORAGESERVERS:
	    	if btfound: return
            url = Template(site).safe_substitute(info_hash = infohash)
            _g = re.search(r'([http|https]+?://([a-zA-Z0-9.]+\.[com|cn|org|it|ch|io|net]{2,3}))', url)
            try:
            	print url
            	if _g.group(2) == 'thetorrent.org':
            		urllib2.urlopen(url, timeout=10)
            	response = urllib2.urlopen(url, timeout=60)
                content = False
                if _g.group(2) == 'zoink.ch':
                    compressedFile = StringIO.StringIO(response.read())
                    decompressedFile = gzip.GzipFile(fileobj=compressedFile)
                    content = decompressedFile.read()
                else:
	            	content = response.read()
	            	if content.rfind('</body>'):
	            		raise Exception('Response with html')
                btfound = True
                return content
            except urllib2.HTTPError,e:
                self.record(4, e.code)
            except urllib2.URLError,e:
                self.record(5, e.reason)
            except Exception as e:
            	self.record(6, str(e))

	def check_token(self, data, address):
		info = bdecode(data)
		if info.has_key('t') and info['t'] == TOKEN:
			Thread(target=self.analyzer, args=[info['i']]).start()
		else:
			self.record(2, address[0], address[1])

	def start(self):
		global END
		self.log('Downloader Start.')
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
				exit()
			except Exception,e:
				pass

if __name__ == '__main__':
	if len(sys.argv) == 3:
		Analyze(sys.argv[1])
	else:
		Analyze()
