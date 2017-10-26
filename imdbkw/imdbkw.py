#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys 
import os

from multiprocessing import Pool, cpu_count

import argparse

from sqlalchemy import create_engine, Table, Integer, Column, ForeignKey, Sequence, String
from sqlalchemy.orm import relationship,scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import imdb

import logging

Base = declarative_base()
Session = None
engine = None

def parse_args(args):
    parser = argparse.ArgumentParser()    
    parser.add_argument("--dburl", help="postgresql connection string here.")
    parser.add_argument("--setup", help="postgresql connection string here.", action="store_true")
    return parser.parse_args()

def main(args):
    args = parse_args(args)
    if args.dburl and args.setup:
        dburl = args.dburl
        setup_engine(dburl)
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
        return "<Keyword(id='%s', name='%s', rank='%s')>" % (self.id, self.name, self.rank) 

def setup_engine(dburl):
    global engine, Session
    engine = create_engine(dburl, pool_size=0 , max_overflow=-1, pool_recycle=1200)
    session_factory = sessionmaker(bind=engine, autoflush=False)
    Session = scoped_session(session_factory)

def setup_genre():
    print('Setup GENRE data...')
    session = Session()
    genres = imdb.get_genres()
    for genre in genres: 
        session.add(Genre(name= genre['name'], url_name= genre['url_name']))    
    session.commit()
    session.close()
       
def process_film(num=1):
    print('Generating FILM sample...')  
    cpu = cpu_count()
    pool = Pool(processes=cpu)
    session = Session()
    result = []
    for genre in session.query(Genre).all():        
        process = pool.apply_async(imdb.get_title_by_genre, args=(genre, num), callback= write_film)  
        result.append(process)
    for process in result:
        process.wait()
    pool.close();
    pool.join
    session.close()
    
def write_film(titles):
    try:
        session = Session()  
        for title in titles:        
            if not session.query(Film).filter(Film.imdb_id == title['imdb_id']).count(): 
                genre_instance = session.query(Genre).filter(Genre.id == title['genre_id']).first()
                film_instance = Film(imdb_id = title['imdb_id'], name= title['name'])
                session.add(film_instance)
                session.commit() 
                genre_instance.films.append(film_instance)
                session.commit()
                session.close()
    except Exception as e:
        logging.exception("message")
    finally:
        session.close()
            
def process_keyword(num=1):
    print('Generating Keyword sample...')  
    cpu = cpu_count()
    pool = Pool(processes=cpu)
    session = Session()
    result = []
    films = session.query(Film).outerjoin(Film.keywords).filter(Film.keywords == None).all()
    if len(films) >= num:
        films = films[:num]
    for film in films:
        process = pool.apply_async(imdb.get_keyword_by_film, (film, ), callback= write_keyword)  
        result.append(process)
    for process in result:
        process.wait()
    pool.close();
    pool.join
    session.close()

def write_keyword(keywords):
    try:
        session = Session()
      
        for keyword in keywords:
            keyword_instance = session.query(Keyword).filter(Keyword.name == keyword['name']).first()
            film_instance = session.query(Film).filter(Film.id == keyword['film_id']).first()
            film_keyword = Film_Keyword(relevant = keyword['relevant'])
            if keyword_instance is None:
                keyword_instance = Keyword(name = keyword['name'])
                session.add(keyword_instance)
                session.commit()                
                film_keyword.keyword = keyword_instance
                film_instance.keywords.append(film_keyword)
                session.commit()
            elif not session.query(Film_Keyword).filter(Film_Keyword.film_id == film_instance.id, Film_Keyword.keyword_id == keyword_instance.id).count():
                film_keyword.keyword = keyword_instance
                film_instance.keywords.append(film_keyword)
                session.commit()                

    except Exception as e:
        logging.exception("message")
    finally:
        session.close()
            

if __name__ == '__main__':      
    main(sys.argv[1:])
