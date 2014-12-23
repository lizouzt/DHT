# -*- coding: UTF8 -*-
'''
Created on 2014-11-30

@author: Elfer
'''
import pdb
from sqlalchemy import *
from sqlalchemy.orm import mapper,sessionmaker,create_session

class Movie(object):
	pass

class DBManage():
	DEFAULT_VAL = '未知'
	def __init__(self):
		####
		#sqlalchemy.create_engine('mysql://user:password@127.0.0.1/test?charset=utf8')
		####
		self.db = create_engine("mysql://localhost/test")
		metadata = MetaData(bind=self.db)
		self.table_movies = Table('movies', metadata, autoload=True)
		mapper_movies = mapper(Movie, self.table_movies)

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
			print 'Inserted'
		else:
			print 'Nope'
