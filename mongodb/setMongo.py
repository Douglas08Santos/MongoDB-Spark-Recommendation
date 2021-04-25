'''
Classe importar os arquivos .dat e/ou csv para o mongoDB

usage: python setmongo.py <name_db> <file movies> <file ratings>
    + nome do banco de dados criado no mongo para salva os documento
    + <movies file>
        + (.dat/.csv)
            + MovieID::Title::Genres / movieId,title,genres
    + <ratings file>
       + (.dat/.csv)
            + UserID::MovieID::Rating::Timestamp / userId,movieId,rating,timestamp
'''

from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime
import pandas as pd
import sys, re

def create_genres(db):
    docs = list(db.movies.aggregate([
        {'$unwind' : '$genres'},
        {'$group':{
            '_id':'$genres',
            'count':{'$sum':1}
        }},
    ], cursor={}))

    genres = [
        {'_id': idx, 'name': doc['_id'], 'count': doc['count']}
        for idx, doc in enumerate(docs)
    ]
    db.command('insert', 'genres', documents=genres, ordered=False)

def insert_movies_dat(db, file):
    # REGEX responsavel por capturar os dados do movies.dat
    #(?P<name>regex)
    #MovieID::Title::Genres
    regex1 = re.compile('([0-9]+)::\"(.+?)\"::(.+)')
    regex2 = re.compile("([0-9]+)::(.*)::(.+?)\n")
   
    movies = []
    count = 0
    for line in file:
        print(line[:-1])
        match = regex1.search(line)
        if(match == None):
            match = regex2.search(line)
        groups = match.groups()
        
        movie = {
            'movieid': int(groups[0]),
            'title': groups[1],
            'genres': groups[2].split('|')
        }
        movies.append(movie)
        count +=1 
        if(count % 500 == 0):
            db.command('insert', 'movies', documents=movies, ordered=False)
            print(count, 'movies inserted')
            
            movies = []
    #Insere o restante, já que ele adicionava de 2000 em 2000        
    db.command('insert', 'movies', documents=movies, ordered=False)
    print(count, 'movies inserted')

def insert_ratings_dat(db, file):
    #UserID::MovieID::Rating::Timestamp
    regex = re.compile("(?P<userid>[0-9]+)::(?P<movieid>[0-9]+)::(?P<rating>.*?)::(?P<ts>.*?)\n")

    users = []
    movies = []
    ratings = []

    count = 0

    for line in file:
        match = regex.search(line)
        groups = match.groupdict()
        #print('ratings: ', int(groups['userid']))
        #documento rating
        rating = {
            'userid': int(groups['userid']),
            'movieid': int(groups['movieid']),            
            'rating':float(groups['rating']),
            'ts':datetime.fromtimestamp(float(groups['ts']))
        }
        ratings.append(rating)
        #documento rating
        user = {
            'q':{'userid': int(groups['userid'])},
            'u':{'$inc':{
                    'ratings' : 1
                }
            },
            'upsert':True
        }
        users.append(user)
        #documento movie
        movie = {
            'q':{'movieid': int(groups['movieid'])},
            'u':{'$incs':{
                'ratings': 1,
                'total_rating': float(groups['rating'])
                }
            }
        }
        movies.append(movie)
        count += 1
        if(count % 1000 == 0):
            db.command('insert', 'ratings', documents=ratings, ordered=False)
            db.command('update', 'users', updates=users, ordered=False)
            db.command('update', 'movies', updates=movies, ordered=False)
            print(count, 'ratings inserted')
            ratings, movies, users = [], [], []
    #Insere o restante, já que ele adicionava de 2000 em 2000  
    db.command('insert', 'ratings', documents=ratings, ordered=False)
    db.command('update', 'users', updates=users, ordered=False)
    db.command('update', 'movies', updates=movies, ordered=False)
    print(count, 'ratings inserted')
    ratings, movies, users = [], [], []

# Ler arquivos em .csv
def insert_movies_csv(db, file):
    # movieId,title,genres
    #Regex usado caso o nome do filme tenha aspas
    regex1 = re.compile('([0-9]+),\"(.+?)\",(.+)\n')
    #Outros casos
    regex2 = re.compile('([0-9]+),(.+?),(.+)\n')
    
    movies = []
    count = 0
    __ = file.readline()
    for line in file:
        match = regex1.search(line)
        if(match == None):
            match = regex2.search(line)
        groups = match.groups()
        #print('movie:', int(groups['movieid']))
        movie = {
            'movieid': int(groups[0]),
            'title': groups[1],
            'genres': groups[2].split('|')
        }
        movies.append(movie)
    
        count +=1 
        if(count % 500 == 0):
            db.command('insert', 'movies', documents=movies, ordered=False)
            print(count, 'movies inserted')
            #create_genres(db)
            movies = []
    #Insere o restante, já que ele adicionava de 2000 em 2000        
    db.command('insert', 'movies', documents=movies, ordered=False)
    print(count, 'movies inserted')

def insert_ratings_csv(db, file):
    # userId,movieId,rating,timestamp
    regex = re.compile("(?P<userid>[0-9]+),(?P<movieid>[0-9]+),(?P<rating>.*?),(?P<ts>.*?)\n")

    users = []
    movies = []
    ratings = []
    count = 0
    __ = file.readline()
    for line in file:
        match = regex.search(line)
        groups = match.groupdict()
        #print('ratings: ', int(groups['userid']))
        #documento rating
        rating = {
            'userid': int(groups['userid']),
            'movieid': int(groups['movieid']),
            'rating':float(groups['rating']),
            'ts':datetime.fromtimestamp(float(groups['ts']))
        }
        ratings.append(rating)
        #documento rating
        user = {
            'q':{'userid': int(groups['userid'])},
            'u':{'$inc':{
                    'ratings' : 1
                }
            },
            'upsert':True
        }
        users.append(user)
        #documento movie
        movie = {
            'q':{'movieid': int(groups['movieid'])},
            'u':{'$incs':{
                'ratings': 1,
                'total_rating': float(groups['rating'])
                }
            }
        }
        movies.append(movie)

        count += 1
        if(count % 1000 == 0):
            db.command('insert', 'ratings', documents=ratings, ordered=False)
            db.command('update', 'users', updates=users, ordered=False)
            db.command('update', 'movies', updates=movies, ordered=False)
            print(count, 'ratings inserted')
            ratings, movies, users = [], [], []
    #Insere o restante, já que ele adicionava de 2000 em 2000  
    db.command('insert', 'ratings', documents=ratings, ordered=False)
    db.command('update', 'users', updates=users, ordered=False)
    db.command('update', 'movies', updates=movies, ordered=False)
    print(count, 'ratings inserted')
    ratings, movies, users = [], [], []

def main():
    # indica o host e a porta do Mongodb
    db = MongoClient('localhost', 27017)[sys.argv[1]]
        
    #Verifica qual tipo de arquivo foi passado
    if(len(sys.argv) == 4):
        if(sys.argv[2][-3:] == 'dat' and sys.argv[3][-3:] == 'dat'):
            fmovies = open(sys.argv[2], encoding = "ISO-8859-1")
            frating = open(sys.argv[3])

            #insert_movies_dat(db, fmovies)
            insert_ratings_dat(db, frating)
            #create_genres(db)
        elif(sys.argv[2][-3:] == 'csv' and sys.argv[3][-3:] == 'csv'):
            fmovies = open(sys.argv[2], encoding = "ISO-8859-1")
            frating = open(sys.argv[3])

            insert_movies_csv(db, fmovies)
            insert_ratings_csv(db, frating)
            create_genres(db)
        
        else:
            print("Arquivo incorreto")
    else:
        print('usage: python setmongo.py <name_db> <file movies> <file ratings>')


if __name__ == '__main__':
    main()
    