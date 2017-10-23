#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys 
import os

from multiprocessing import Pool, cpu_count
from multiprocessing.util import register_after_fork

import argparse

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Integer, Column, ForeignKey, Sequence, String
from sqlalchemy.orm import relationship,scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import exists
from sqlalchemy import event
from sqlalchemy import exc

import imdb

Base = declarative_base()
session = sessionmaker(autoflush=False)
dburl = None

def parse_args(args):
    parser = argparse.ArgumentParser()    
    parser.add_argument("--dburl", help="postgresql connection string here.")
    parser.add_argument("--setup", help="postgresql connection string here.", action="store_true")
    return parser.parse_args()

def main(args):
    args = parse_args(args)
    global dburl
    if args.dburl and args.setup:
        dburl = args.dburl
        engine = setup_engine(dburl)
        print('Initialze database...')
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)        
        setup_genre()     
        process_film()
        process_keyword()
        
genre_film = Table('genre_film', Base.metadata,
    Column('genre_id', Integer, ForeignKey('genre.id')),
    Column('film_id', Integer, ForeignKey('film.id'))
)

class Film_Keyword(Base):
    __tablename__ = 'film_keyword'
    film_id = Column(Integer, ForeignKey('film.id'), primary_key=True)
    keyword_id = Column(Integer, ForeignKey('keyword.id'), primary_key=True)
    relevant = Column(String(50))
    film = relationship("Film", back_populates="keywords")
    keyword = relationship("Keyword", back_populates="films")
        
class Genre(Base):
    __tablename__ = 'genre'
    id = Column(Integer, Sequence('genre_id_seq'), primary_key=True, nullable= False)
    name = Column(String(20))
    url_name = Column(String(20))
    films = relationship("Film", secondary=genre_film, backref="genres")

    def __repr__(self):
        return "<Genre(id='%s', name='%s', url_name='%s')>" % (self.id, self.name, self.url_name)

class Film(Base):
    __tablename__ = 'film'
    id = Column(Integer, Sequence('film_id_seq'), primary_key=True, nullable= False)
    imdb_id = Column(String(9))
    name = Column('name', String(100))
    keywords = relationship("Film_Keyword", back_populates="film")    

    def __repr__(self):
        return "<Film(id='%s', imdb_id='%s', name='%s')>" % (self.id, self.imdb_id, self.name)
    
class Keyword(Base):
    __tablename__ = 'keyword'
    id = Column(Integer, Sequence('keyword_id_seq'), primary_key=True, nullable= False)
    name = Column(String(100))
    rank = Column(Integer)
    films = relationship("Film_Keyword", back_populates="keyword") 

    def __repr__(self):
        return "<Keyword(id='%s', film_id='%s', name='%s', rank='%s')>" % (self.id, self.film_id, self.name, self.rank) 

def setup_engine(dburl):
    engine = create_engine(dburl, client_encoding='utf8')
    add_process_guards(engine)
    register_after_fork(engine, engine.dispose)
    return engine

def setup_genre():
    print('Setup GENRE data...')
    engine = setup_engine(dburl)
    session_factory = session(bind=engine)
    genres = imdb.get_genres()
    for genre in genres: 
        session_factory.add(Genre(name= genre['name'], url_name= genre['url_name']))    
    session_factory.commit()
       
def process_film(num=1):
    print('Generating FILM sample...')  
    cpu = cpu_count()
    pool = Pool(processes=cpu)
    engine = setup_engine(dburl)
    session_factory = session(bind=engine)
    result = []
    for genre in session_factory.query(Genre).all():        
        process = pool.apply_async(imdb.get_title_by_genre, args=(genre, num), callback= write_film)  
        result.append(process)
    for process in result:
        process.wait()
    pool.close();
    pool.join
    
def write_film(titles):    
    engine = setup_engine(dburl)
    session_factory = session(bind=engine)  
    for title in titles:        
        if not session_factory.query(Film).filter(Film.imdb_id == title['imdb_id']).count(): 
            genre_instance = session_factory.query(Genre).filter(Genre.id == title['genre_id']).first()
            film_instance = Film(imdb_id = title['imdb_id'], name= title['name'])
            session_factory.add(film_instance)
            session_factory.commit() 
            genre_instance.films.append(film_instance)
            session_factory.commit()
            print(title)
            
def process_keyword(num=1):
    print('Generating Keyword sample...')  
    cpu = cpu_count()
    pool = Pool(processes=cpu)
    engine = setup_engine(dburl)
    session_factory = session(bind=engine)
    result = []
    films = session_factory.query(Film).filter(Film.keywords == None).limit(num).all()
    for film in films:    
        process = pool.apply_async(imdb.get_keyword_by_film, (film, ), callback= write_keyword)  
        result.append(process)
    for process in result:
        process.wait()
    pool.close();
    pool.join

def write_keyword(keywords):
    engine = setup_engine(dburl)
    session_factory = session(bind=engine) 
    for keyword in keywords:
        if not session_factory.query(Film).filter(Film.keywords.any(Keyword.name == keyword['name'])).count(): 
            film_instance = session_factory.query(Film).filter(Film.id == keyword['film_id']).first()
            keyword_instance = Keyword(name = keyword['name'])
            session_factory.add(keyword_instance)
            session_factory.commit()
            film_keyword = Film_Keyword(relevant = keyword['relevant'])
            film_keyword.keyword = keyword_instance
            film_instance.keywords.append(film_keyword)
            session_factory.commit()            
            
def add_process_guards(engine):
    @sqlalchemy.event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    @sqlalchemy.event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            LOG.debug(_LW(
                "Parent process %(orig)s forked (%(newproc)s) with an open "
                "database connection, "
                "which is being discarded and recreated."),
                {"newproc": pid, "orig": connection_record.info['pid']})
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                "Connection record belongs to pid %s, "
                "attempting to check out in pid %s" %
                (connection_record.info['pid'], pid)
            )   

if __name__ == '__main__':      
    main(sys.argv[1:])
