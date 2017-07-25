import pandas as pd
import requests
import datetime
import pymysql
import nltk
from nltk.stem.porter import *
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import re


def YMD(x):
    return str(x)[:10]


def MD(x):
    return x[5:]


stemmer = PorterStemmer()


def stem_tokens(tokens, stemmer):
    stemmed = []
    for item in tokens:
        stemmed.append(stemmer.stem(item))
    return stemmed


def tokenizer(doc):
    eng = re.compile('[^ A-Za-z]+')
    tokens = word_tokenize(eng.sub(' ', doc.lower()))
    filtered = [w for w in tokens if not w in stopwords.words('english') and len(w) > 1]
    stemmed_tokens = stem_tokens(filtered, stemmer)
    POS = []
    for x in nltk.pos_tag(stemmed_tokens):
        if x[1] in ['NN', 'NNS', 'VBN', 'VB', 'VBP', 'VBZ', 'VBD', 'JJ', 'JJS', 'JJR', 'RB', 'RBR', 'RP']:
            POS.append(x[0])
    return ','.join(POS)


app = [{'ios': 'id=1053012308'}, {'android': 'p=com.supercell.clashroyale'}]
# lang = ['ko', 'en']
lang = ['en']


def job():
    now = datetime.datetime.now()
    #     print('----- Start :' + str(datetime.datetime.now())[:16] + '-----')
    #     today = str(datetime.datetime.now())[:10]
    #     date = datetime.datetime.strptime(today, "%Y-%m-%d")
    #     date += datetime.timedelta(days= -1)
    #     yesterday = str(date)[:10]
    success_count = 0

    for con in app:
        access = []
        for soft, acc in con.items():
            if soft == 'android':
                access.append('android')
                access.append(acc)
            elif soft == 'ios':
                access.append('ios')
                access.append(acc)

        for lan in lang:
            page = 1
            tot_page = 10000

            for i in range(1, tot_page):
                try:
                    print(lan, access[0])
                    movieIdListURL = "https://data.42matters.com/api/v2.0/" + access[0] + "/apps/reviews.json?\
                                        " + access[1] + "&\
                                        access_token=fdcdb4c9ec8c81e2ba0b4d00ab2eb0e80e310fd4&\
                                        days=1&\
                                        lang=" + lan + "&\
                                        page=" + str(page)

                    moviewIdPage = requests.post(movieIdListURL)
                    json = moviewIdPage.json()
                    connection = pymysql.connect(user='root', passwd='0000', db='app', charset='utf8')
                    reviews = json['reviews']
                    tot_page = json['total_pages']
                    total_reviews = json['total_reviews']
                    print('tot_page: ', tot_page, 'page: ', page, 'total_reviews: ', total_reviews)
                except Exception as e:
                    print('API connection error_message: ' + str(e))
                    break

                if tot_page + 1 == page:
                    page = 1
                    break

                if access[0] == 'ios':
                    for rev in reviews:
                        try:
                            with connection.cursor() as cur:

                                rev['date'] = str(
                                    pd.to_datetime(rev['date']) + datetime.timedelta(seconds=success_count))

                                data = (access[0], rev['app_version'], rev['author_id'], rev['title'], rev['content'],
                                        rev['date'], rev['rating'], rev['lang'])
                                sql = """insert into 
                                            app.app (app, version, id, title, content, date, rating, lang)
                                            values (%s, %s, %s, %s, %s, %s, %s, %s)"""
                                cur.execute(sql, data)

                                data = (rev['author_id'], tokenizer(rev['title'] + ' ' + rev['content']))
                                sql = """insert into
                                            app.app_text (id, context)
                                            values(%s, %s)"""
                                cur.execute(sql, data)

                                connection.commit()
                                success_count += 1
                        except Exception as e:
                            print('SQL error_message: ' + str(e))
                            if 'total_pages' in str(e):
                                break
                            pass

                    page += 1




                elif access[0] == 'android':
                    for rev in reviews:
                        try:
                            with connection.cursor() as cur:
                                data = (access[0], rev['app_version'], rev['author_id'], rev['content'], rev['date'],
                                        rev['rating'], rev['lang'])

                                sql = """insert into 
                                            app.app (app, version, id, content, date, rating, lang)
                                            values (%s, %s, %s, %s, %s, %s, %s)"""
                                cur.execute(sql, data)

                                data = (rev['author_id'], tokenizer(rev['content']))
                                sql = """insert into
                                            app.app_text (id, context)
                                            values(%s, %s)"""
                                cur.execute(sql, data)

                                connection.commit()
                                success_count += 1

                        except Exception as e:
                            print('SQL error_message: ' + str(e))
                            if 'total_pages' in str(e):
                                break
                            pass

                    page += 1
    print("Complete Stacking, " + str(success_count) + "건 성공, " + str(now))
job()