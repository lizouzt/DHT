# coding: utf-8

import pdb
import os, sys, logging, json, re, requests
import libtorrent as lt
from string import Template
from bencode import bdecode

BTSTORAGESERVERS = [
    'http://d1.torrentkittycn.com/?infohash=${info_hash}',
    'http://thetorrent.org/${info_hash}.torrent',
    'https://zoink.it/torrent/${info_hash}.torrent',
    'https://torcache.net/torrent/${info_hash}.torrent'
]

class Analyze():
	_infohash_list = []
	def __init__(self,result_file=None):
		self._result_file = result_file
		with open(self._result_file) as f:
			self._infohash_list = json.load(f)				
		
		for infohash in self._infohash_list:
			self.analyzer(infohash)

	def analyzer(self, infohash):
	    content = self.download(infohash)
	    if(content == -1):
	        return
	    try:
	    	content = bdecode(content)
	    	info = self.getTorrentInfo(content)
	    	print str(info)
	    except Exception, e:
	    	print '\r\nBDecode Failed: ',e

	def getTorrentInfo(self, content):
		def getLen(list):
			size = 0
			for file in list:
				size += file['length']
			return size

		metadata = content['info']

		info = {
			'name': metadata['name'],
			'cdate': content['creation date'],
			'files': metadata['files'],
			'size': getLen(metadata['files'])
		}
		return info

	def download(self, infohash, tracker=0):
	    infohash = infohash.upper()
	    url = Template(BTSTORAGESERVERS[tracker]).safe_substitute(info_hash = infohash)
	    # url = 'http://taoz.wapp.waptest.taobao.com/Downloads/xx.torrent'
	    g = re.search(r'([http|https]+?://([a-zA-Z0-9.]+\.[com|cn|org|it|net]{2,3}))', url)
	    iheaders = {'Host': g.group(2), 'Referer': g.group(1), "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8', 'Accept-Encoding': 'gzip, deflate, sdch'}
	    try:
	        req = requests.get(url, headers = iheaders, timeout=20)
	        if req.status_code == 200:

	        	if '</html>' in req.content or len(req.content) < 20:
	        		raise Exception("It's a html")
	    		return req.content
	    except Exception as e:
	        print('BT download error: ', infohash, e)
	        if tracker < 2:
	        	print('Once again.',tracker)
	        	self.download(infohash, tracker+1)
	        return -1

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'argument err:'
        print '\tpython collector.py result.json\n'
        sys.exit(-1)

    Analyze(sys.argv[1])
