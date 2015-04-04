# -*- coding: UTF8 -*-
'''
Created on 2014-11-30

@author: Elfer
'''
import pdb
import os
import socket
from bencode import bencode, bdecode
from sqlalchemy import *
from sqlalchemy import exc as EXC
from sqlalchemy.orm import mapper,sessionmaker,create_session
from settings import *

class Movie(object):
	pass

class Torrents(object):
	pass

class DataLog(object):
	"""docstring for DataLog"""
	def __init__(self, port=9998):
		super(DataLog, self).__init__()
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.socket.bind(("0.0.0.0", port))

	def send_log(msg, address=(DLHOST,DLPORT)):
		msg['t'] = TOKEN
		try:
			self.socket.sendto(bencode(msg), address)
		except Exception,e:
			pass

class DBManage(DataLog):
	DEFAULT_VAL = '未知'
	CMD = 'sh flush.sh %s %s %s' % (MQSERVER, MQUSER, MQPWD)
	db = None
	def __init__(self):
		DataLog.__init__(self)
		self.conDB()
		metadata = MetaData(bind=self.db)
		self.table_movies = Table('movies', metadata, autoload=True)
		self.table_torrents = Table('torrents', metadata, autoload=True)

		mapper_movies = mapper(Movie, self.table_movies)
		mapper_torrent = mapper(Torrents, self.table_torrents)

	def conDB(self, need_flush=False):
		try:
			self.db = create_engine("mysql://%s:%s@%s/%s?charset=utf8" % (MQUSER, MQPWD, MQSERVER, MQDB))
			print 'Connect to mysql success.'
		except Exception,e:
			print "Connect Mysql Engine Error %s" % str(e)
			self.send_log({
				'i': 1,
				'm': "Connect Mysql Engine Error %s" % str(e)
			})
			exit(-1)

		self.Maker = sessionmaker()
		self.Maker.configure(bind=self.db)

		need_flush and os.system(self.CMD)

	def reflectMovieObject(self, data):
		movie = Movie()
		movie.id = data['id']
		movie.type = data['type']
		movie.image = data['image']
		movie.title = data['title'] or self.DEFAULT_VAL
		movie.area = data['area'] or self.DEFAULT_VAL
		movie.directors = ';'.join(data['directors']) or self.DEFAULT_VAL
		movie.actors = ';'.join(data['actors']) or self.DEFAULT_VAL
		movie.year = data['year'] or self.DEFAULT_VAL
		movie.abstract = data['abstract'] or self.DEFAULT_VAL
		if movie.id == None or movie.id == '' or movie.id == 'null' or movie.type == None or movie.type == '' or movie.type == 'null':
			return None

		return movie

	def reflectTorrentObject(self, data):
		torrent = Torrents()
		torrent.name = data['name']
		torrent.num_files = data['num_files']
		torrent.total_size = data['total_size']
		torrent.info_hash = data['info_hash']
		torrent.media_type = data['media_type']
		torrent.files = data['files']
		
		if 'valid' in data:
			torrent.valid = data['valid']

		if 'announce' in data:
			torrent.announce = data['announce']
			torrent.announce_list = data['announce_list']

		if 'creation_date' in data:
			torrent.creation_date = data['creation_date']

		if torrent.name == None or torrent.info_hash == None:
			return None

		return torrent


	def saveTorrent(self, data):
		torrent = self.reflectTorrentObject(data)

		if torrent is not None:
			try:
				session = self.Maker()
				if session.query(Torrents).filter_by(info_hash=torrent.info_hash).scalar() == None:
					session.add(torrent)
					session.flush()
					session.commit()
					print 'Inserted'
					self.send_log({
						'i': 0,
						'm': "DisconnectionError %s" % str(e)
					})
			except EXC.DisconnectionError,e:
				print 'DisconnectionError: ',e
				self.send_log({
					'i': 1,
					'm': "DisconnectionError %s" % str(e)
				})
				self.conDB(True)
			except Exception,e:
				print 'Insert Error',str(e)
				self.send_log({
					'i': 1,
					'm': "Insert Error %s" % str(e)
				})
		else:
			print 'Nope'

	def saveMovie(self, data):
		movie = self.reflectMovieObject(data)

		if movie is not None:
			Maker = sessionmaker()
			Maker.configure(bind=self.db)
			session = Maker()

			if session.query(Movie).filter_by(id=movie.id).scalar() == None:
				session.add(movie)
				session.flush()

			session.commit()
		else:
			print 'Nope'

	def queryMovies(self, params):
		pass

	def getMovieDetail(self, params):
		pass

	def searchAssociateMovies(self, params):
		pass