# -*- coding: UTF8 -*-
'''
Created on 2014-11-30

@author: tao.z
'''
import pdb
import os
import socket
from bencode import bencode, bdecode
from pymongo import MongoClient
from dataLog import *
from settings import *

class Torrents(object):
	pass

class DBManage(DataLog):
	SQLINSERT = "INSERT into \
				torrents(name,num_files,total_size,valid,creation_date,info_hash,media_type,files) \
				values (%s,%s,%s,%s,%s,%s,%s,%s)"
	DEFAULT_VAL = '未知'
	db = None
	_isReconning = False
	def __init__(self,log):
		DataLog.__init__(self)
		self.log = log
		self.conDB()

	def conDB(self, need_flush=False):
		if self._isReconning is True:
			return
		try:
			self._isReconning = True
			client = MongoClient('mongodb://%s:%s/?authMechanism=SCRAM-SHA-1'%(MHOST,MPORT))
			client.dht.authenticate(MUSER,MPWD)
			self.col_torrents = client.dht.torrents
			self.col_files = client.dht.files
		except Exception,e:
			print e
			# self.log.info("ConnectionError %s" % str(e))

		need_flush and self.send_log({'r': 'needrestart','i': '1'})

	def reflectMovieObject(self, data):
		pass

	def reflectTorrentObject(self, data):
		if data['name'] == None or data['info_hash'] == None:
			return None

		torrent = data
		files = {'info_hash': data['info_hash'],'files':data['files']}
		torrent['valid'] = data['valid'] if 'valid' in data else '0'
		torrent['creation_date'] = data['creation_date'] if 'creation_date' in data else ''
		torrent['files'] = data['files'].__getslice__(0,5)

		return torrent,files

	def saveTorrent(self, data):
		torrent,files = self.reflectTorrentObject(data)
		if torrent is not None:
			try:
				if self.col_torrents.find_one({'info_hash':torrent['info_hash']}) is None:
					self.col_torrents.insert_one(torrent)
					self.col_files.insert_one(files)
					print 'Inserted'
					self._isReconning = False
					self.send_log({
						'r': 'dht',
						'i': '0'
					})
				else:
					print 'Duplicate'
					self.send_log({
						'r': 'dht',
						'i': '-1'
					})
			except Exception,err:
				error = err.args[0]
				
				if error.index('[Errno 61] Connection refused') > -1:
					self.log.info('Connection Error %s'% err.message)
					self.send_log({
						'r': 'dht',
						'i': '1',
						'm': "Connection Error %s" % str(err.message)
					})
					self.conDB()
				else:
					self.log.info('Unexpected Error %s' % str(err.message))
					self.send_log({
						'r': 'dht',
						'i': '1',
						'm': "Unexpected Error %s" % str(err.message)
					})
			finally:
				pass
		else:
			print 'Nope'

	def saveMovie(self, data):
		pass

	def queryMovies(self, params):
		pass

	def getMovieDetail(self, params):
		pass

	def searchAssociateMovies(self, params):
		pass
