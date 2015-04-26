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
			self.client = client
		except Exception,e:
			print e

		need_flush and self.record({'r': 'needrestart','i': '1'})

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
				if self.col_torrents.find_one({'info_hash':torrent['info_hash']},no_cursor_timeout=True) is None:
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
				error = str(err.args[0])
				self.send_log({
					'r': 'dht',
					'i': '1',
					'm': "Connection Error %s" % error
				})
				if 'Connection refused' in error:
					self.log.info('Unexpected Error %s' % str(err))
					self.client.close()
					self.conDB()
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
