import pandas as pd
import requests
import datetime
import pymysql
import nltk
from nltk.stem.porter import *
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import re
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger


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
                                        access_token=62cac95732da67ddee97f5f8e4f1635b66ac3661&\
                                        days=30&\
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


class Scheduler():
    # 클래스 생성시 스케쥴러 데몬을 생성합니다.
    def __init__(self):
        self.sched = BlockingScheduler()

    # 스케쥴러입니다. 스케쥴러가 실행되면서 hello를 실행시키는 쓰레드가 생성되어집니다.
    # 그리고 다음 함수는 type 인수 값에 따라 cron과 interval 형식으로 지정할 수 있습니다.
    # 인수값이 cron일 경우, 날짜, 요일, 시간, 분, 초 등의 형식으로 지정하여,
    # 특정 시각에 실행되도록 합니다.(cron과 동일)
    # interval의 경우, 설정된 시간을 간격으로 일정하게 실행실행시킬 수 있습니다.
    def scheduler(self):
        #         trigger = IntervalTrigger(hours=1)
        trigger = CronTrigger(day_of_week='mon-fri', hour='8', minute='41')
        self.sched.add_job(job, trigger)
        self.sched.start()


if __name__ == '__main__':
    scheduler = Scheduler()
    scheduler.scheduler()