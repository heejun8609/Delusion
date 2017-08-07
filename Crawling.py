from selenium import webdriver
from bs4 import BeautifulSoup
import time
import datetime
import pandas as pd
import re

import nltk
from nltk.stem.porter import *
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

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
    driver.get("https://play.google.com/store/apps/details?id=com.epicactiononline.ffxv.ane&hl=en")
    time.sleep(1)

    # 웹페이지 긁어오기
    html = driver.page_source
    time.sleep(1)

    # 확장 버튼 클릭
    driver.find_element_by_xpath("//div[@class='details-section reviews']//button[@aria-label='See More']").click()
    time.sleep(1)

    # 최신 순 정렬
    driver.find_element_by_xpath("//div[@class='details-section reviews']//button[@class='dropdown-menu']").click()
    time.sleep(1)
    driver.find_element_by_xpath("//div[@class='details-section reviews']//button[@class='dropdown-child']").click()
    time.sleep(1)

    # 페이지 긁어오기
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    # 목표 날짜
    custom_date = datetime.date.today() - datetime.timedelta(days=1)

    temp_date = []

    for x in soup.find_all(class_='review-date'):
        temp_date.append(str(pd.to_datetime(x.get_text()))[:10])

    # 목표 날짜까지 확장버튼 클릭
    while True:
        try:
            for x in range(10):
                driver.find_element_by_xpath(
                    "//div[@class='details-section reviews']//button[@aria-label='See More']").click()
                time.sleep(1)
        except:
            continue
            print(temp_date)

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        for x in soup.find_all(class_='review-date'):
            temp_date.append(str(pd.to_datetime(x.get_text()))[:10])

        if str(custom_date) in temp_date:
            break

    # 항목별 리스트 만들기
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

    import pymysql
    connection = pymysql.connect(user='root', passwd='0000', db='app', charset='utf8')
    success_count = 0
    for row in df.iterrows():
        try:
            with connection.cursor() as cur:
                row[1]['date'] = str(pd.to_datetime(row[1]['date']) + datetime.timedelta(seconds=success_count))
                if row[1]['author'] != '':
                    data = (
                    'android', re.sub('[-:]', '', row[1]['date']), row[1]['date'], row[1]['review'], row[1]['rating'],
                    'en')
                    sql = """insert into 
                                app.fantasy (app, id, date, content, rating, lang)
                                values (%s, %s, %s, %s, %s, %s)"""

                    cur.execute(sql, data)

                    data = (re.sub('[-:]', '', row[1]['date']), tokenizer(row[1]['review']))
                    sql = """insert into
                                app.fantasy_text (id, context)
                                values(%s, %s)"""
                    cur.execute(sql, data)

                    connection.commit()
                    success_count += 1
                else:
                    data = ('android', row[1]['author'], row[1]['date'], row[1]['review'], row[1]['rating'], 'en',
                            'android', row[1]['author'], row[1]['date'], row[1]['review'], row[1]['rating'], 'en')
                    sql = """insert into 
                                app.fantasy (app, id, date, content, rating, lang)
                                values (%s, %s, %s, %s, %s, %s)
                                on duplicate key update app=%s, id=%s, date=%s, content=%s, rating=%s, lang=%s"""

                    cur.execute(sql, data)

                    data = (row[1]['author'], tokenizer(row[1]['review']),
                            row[1]['author'], tokenizer(row[1]['review']))
                    sql = """insert into
                                app.fantasy_text (id, context)
                                values(%s, %s) on duplicate key update id=%s, context=%s"""
                    cur.execute(sql, data)

                    connection.commit()
                    success_count += 1
        except Exception as e:
            print('SQL error_message: ' + str(e))
            pass
    connection.close()
    print("Complete Stacking, " + str(success_count) + "건 성공, " + str(datetime.datetime.now()))