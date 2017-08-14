from selenium import webdriver
from bs4 import BeautifulSoup
import time
import datetime
import pandas as pd
import re
import pymysql

import nltk
from nltk.stem.porter import *
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
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


def job():
    # 크롬 드라이버 호출
    driver = webdriver.Chrome()
    time.sleep(1)

    # 자동화 웹페이지 호출
    driver.get("https://play.google.com/store/apps/details?id=net.delusionstudio.castleburn&hl=en")
    time.sleep(1)

    # 웹페이지 긁어오기
    html = driver.page_source
    time.sleep(1)

    # 확장 버튼 클릭
    driver.find_element_by_xpath("//div[@class='details-section reviews']//button[@aria-label='See More']").click()
    time.sleep(1)

    driver.find_element_by_xpath("//div[@class='details-section reviews']//button[@class='dropdown-menu']").click()
    time.sleep(1)

    driver.find_element_by_xpath("//div[@class='details-section reviews']//button[@class='dropdown-child']").click()
    time.sleep(1)

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    custom_date = datetime.date.today() - datetime.timedelta(days=2)

    temp_date = []
    temp_auth = []

    for x in soup.find_all(class_='review-date'):
        temp_date.append(str(pd.to_datetime(x.get_text()))[:10])

    while True:
        try:
            for x in range(1):
                driver.find_element_by_xpath(
                    "//div[@class='details-section reviews']//button[@aria-label='See More']").click()
                time.sleep(1.5)
        except:
            continue
            print(temp_date)

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        for x in soup.find_all(class_='review-date'):
            temp_date.append(str(pd.to_datetime(x.get_text()))[:10])

        for x in soup.find_all(class_='author-name'):
            temp_auth.append(x.get_text().strip())

        # if 'Junnel Arabiana' in temp_auth:
        #     break

        if str(custom_date) in temp_date:
            break

    auth = []
    date = []
    rate = []
    rev = []

    for elem in soup.find_all(class_='single-review'):

        auth.append(elem.find(class_='author-name').get_text())

        date.append(str(pd.to_datetime(elem.find(class_='review-date').get_text()))[:10])

        rev.append(elem.find(class_='review-body with-review-wrapper').get_text()[:-15])

        m = re.search(r'[0-9]+', elem.find(class_='current-rating')['style'])
        num = m.group()
        if int(num) < 30:
            rate.append(1)
        elif int(num) > 30 and int(num) < 50:
            rate.append(2)
        elif int(num) > 50 and int(num) <= 70:
            rate.append(3)
        elif int(num) > 70 and int(num) <= 90:
            rate.append(4)
        elif int(num) > 90:
            rate.append(5)

    df = pd.concat([pd.DataFrame(auth, columns=['author']), pd.DataFrame(date, columns=['date']),
                    pd.DataFrame(rev, columns=['review']), pd.DataFrame(rate, columns=['rating'])], axis=1)

    connection = pymysql.connect(user='root', passwd='0000', db='app', charset='utf8')

    success_count = 0
    for row in df.iterrows():
        try:
            with connection.cursor() as cur:
                row[1]['date'] = str(pd.to_datetime(row[1]['date']) + datetime.timedelta(seconds=success_count))
                if row[1]['author'] == '':
                    data = (
                    'android', re.sub('[-:]', '', row[1]['date']), row[1]['date'], row[1]['review'], row[1]['rating'],
                    'en',
                    'android', re.sub('[-:]', '', row[1]['date']), row[1]['date'], row[1]['review'], row[1]['rating'],
                    'en')
                    sql = """insert into 
                                app.castleburn (app, id, date, content, rating, lang)
                                values (%s, %s, %s, %s, %s, %s)
                                on duplicate key update app=%s, id=%s, date=%s, content=%s, rating=%s, lang=%s"""

                    cur.execute(sql, data)

                    data = (re.sub('[-:]', '', row[1]['date']), tokenizer(row[1]['review']),
                            re.sub('[-:]', '', row[1]['date']), tokenizer(row[1]['review']))
                    sql = """insert into
                                app.castleburn_text (id, context)
                                values(%s, %s) on duplicate key update id=%s, context=%s"""
                    cur.execute(sql, data)

                    connection.commit()
                    success_count += 1
                else:
                    data = ('android', row[1]['author'], row[1]['date'], row[1]['review'], row[1]['rating'], 'en',
                            'android', row[1]['author'], row[1]['date'], row[1]['review'], row[1]['rating'], 'en')
                    sql = """insert into 
                                app.castleburn (app, id, date, content, rating, lang)
                                values (%s, %s, %s, %s, %s, %s)
                                on duplicate key update app=%s, id=%s, date=%s, content=%s, rating=%s, lang=%s"""

                    cur.execute(sql, data)

                    data = (row[1]['author'], tokenizer(row[1]['review']),
                            row[1]['author'], tokenizer(row[1]['review']))
                    sql = """insert into
                                app.castleburn_text (id, context)
                                values(%s, %s) on duplicate key update id=%s, context=%s"""
                    cur.execute(sql, data)

                    connection.commit()
                    success_count += 1
        except Exception as e:
            print('SQL error_message: ' + str(e))
            pass
    connection.close()
    print("Complete Stacking, " + str(success_count) + "건 성공, " + str(datetime.datetime.now()))
    driver.quit()


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
        trigger = CronTrigger(day_of_week='mon-sun', hour='15', minute='31')
        self.sched.add_job(job, trigger)
        self.sched.start()


if __name__ == '__main__':
    scheduler = Scheduler()
    scheduler.scheduler()