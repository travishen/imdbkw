#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import bs4
import requests
import math

def get_genres():
    genres = []
    url = 'http://www.imdb.com/genre/?ref_=nv_ch_gr_3'
    res = requests.get(url=url)
    soap = bs4.BeautifulSoup(res.text,'lxml')
    select = soap.select('.genre-table td > h3 > a')
    for i in select:
        name = parse_letters(i.get_text())
        url_name = parse_url_name(i['href'])
        genres.append({'name':name, 'url_name': url_name}) 
    return genres
        
def parse_letters(input):
    return ''.join([c for c in input if c.isalpha()]).lower()
    
def parse_url_name(input):
    return re.split('//|\/', input)[3]

def parse_imdb_id(input):
    return re.split('/', input)[2]

def parse_int(value):
    try:
        return int(value), True
    except ValueError:
        return value, False

def get_title_by_genre(genre, num):
    url = 'http://www.imdb.com/search/title?title_type=feature&sort=moviemeter,asc'
    page = 1
    titles = []
    while(True):
        params = {
            'genres' : genre.url_name, 
            'page' : page
        }
        res = requests.get(url, params)
        soap = bs4.BeautifulSoup(res.text,'lxml')
        select = soap.select('.lister-item-header a')
        for a in select:     
            if len(titles) < num:
                imdb_id = parse_imdb_id(a['href'])
                name = a.get_text()
                titles.append({'imdb_id': imdb_id, 'name': name, 'genre_id': genre.id})        
        if math.ceil(num / 50) <= page:
            break
        else
            page = page + 1
    return titles

def get_keyword_by_film(film):
    imdb_id = film.imdb_id
    url = f"http://www.imdb.com/title/{imdb_id}/keywords"
    res = requests.get(url)    
    soap = bs4.BeautifulSoup(res.text, 'lxml')
    select = soap.select('.soda.sodavote')
    keywords = []
    for item in select:
        name = item.find('div','sodatext').find('a').get_text()
        relevant = item.find('div','interesting-count-text').find('a').get_text().strip()[:1]
        if not parse_int(relevant)[1]:
            relevant = 0
        keywords.append({'name':name, 'relevant':relevant, 'film_id': film.id})
    return keywords
        
