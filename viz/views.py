import pandas as pd
import datetime

from django.shortcuts import render
from django.template.loader import get_template
from django.http.response import HttpResponse
from viz.models import Raw
from django.db import connection

import pygal
from pygal.style import Style

from gensim import models

import django_tables2 as tables

# test
def test(request):
    template = get_template('test.html')
    return HttpResponse(template.render())


# Preprocessing Function
def YMD(x):
    return str(x)[:10]

def MD(x):
    return x[5:]


# DB 데이터 호출
def db():
    # con = (app, version, lang, days1, days2)
    try:
        with connection.cursor() as curs:
            sql = "select * From app.app_text"
            curs.execute(sql)
            text_all = curs.fetchall()

            sql = "select * FROM app.app"
            curs.execute(sql)
            res = curs.fetchall()

            sql = "select * FROM app.app where rating =%s"
            curs.execute(sql, [1])
            star_1 = curs.fetchall()
            curs.execute(sql, [2])
            star_2 = curs.fetchall()
            curs.execute(sql, [3])
            star_3 = curs.fetchall()
            curs.execute(sql, [4])
            star_4 = curs.fetchall()
            curs.execute(sql, [5])
            star_5 = curs.fetchall()

            sql = "select * FROM app.app_word"
            curs.execute(sql)
            res_all = curs.fetchall()

    finally:
        connection.close()

    return res, star_1, star_2, star_3, star_4, star_5, res_all, text_all

def preprocess(res, star_1, star_2, star_3, star_4, star_5, res_all, text_all):
    text = []
    for x in text_all:
        text.append(x)
    df_text = pd.DataFrame(text, columns=['id', 'text'])

    row = []
    for x in res:
        row.append(x)
    df = pd.DataFrame(row, columns=['app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'])
    df['date'] = df['date'].apply(YMD)
    df = pd.merge(df, df_text, on='id')

    rating_1 = []
    for x in star_1:
        rating_1.append(x)
    rating_1 = pd.DataFrame(rating_1,
                            columns=['app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'])
    rating_1['date'] = rating_1['date'].apply(YMD)
    rating_1 = pd.merge(rating_1, df_text, on='id')

    rating_2 = []
    for x in star_2:
        rating_2.append(x)
    rating_2 = pd.DataFrame(rating_2,
                            columns=['app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'])
    rating_2['date'] = rating_2['date'].apply(YMD)
    rating_2 = pd.merge(rating_2, df_text, on='id')

    rating_3 = []
    for x in star_3:
        rating_3.append(x)
    rating_3 = pd.DataFrame(rating_3,
                            columns=['app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'])
    rating_3['date'] = rating_3['date'].apply(YMD)
    rating_3 = pd.merge(rating_3, df_text, on='id')

    rating_4 = []
    for x in star_4:
        rating_4.append(x)
    rating_4 = pd.DataFrame(rating_4,
                            columns=['app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'])
    rating_4['date'] = rating_4['date'].apply(YMD)
    rating_4 = pd.merge(rating_4, df_text, on='id')

    rating_5 = []
    for x in star_5:
        rating_5.append(x)
    rating_5 = pd.DataFrame(rating_5,
                            columns=['app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'])
    rating_5['date'] = rating_5['date'].apply(YMD)
    rating_5 = pd.merge(rating_5, df_text, on='id')

    # 일별 리뷰수, 별점수
    reviews = pd.DataFrame(df.groupby('date')['content'].size()).reset_index()
    reviews.columns = ['date', 'reviews']
    rat_1 = pd.DataFrame(rating_1.groupby('date')['rating'].size()).reset_index()
    rat_1.columns = ['date', 'star_1']
    rat_2 = pd.DataFrame(rating_2.groupby('date')['rating'].size()).reset_index()
    rat_2.columns = ['date', 'star_2']
    rat_3 = pd.DataFrame(rating_3.groupby('date')['rating'].size()).reset_index()
    rat_3.columns = ['date', 'star_3']
    rat_4 = pd.DataFrame(rating_4.groupby('date')['rating'].size()).reset_index()
    rat_4.columns = ['date', 'star_4']
    rat_5 = pd.DataFrame(rating_5.groupby('date')['rating'].size()).reset_index()
    rat_5.columns = ['date', 'star_5']

    # 리뷰수, 별점수 병합
    def mer(x, y):
        return pd.merge(x, y, on='date')

    dfs = [rat_1, rat_2, rat_3, rat_4, rat_5]
    for d in dfs:
        reviews = mer(reviews, d)

    # 별점 평균
    reviews['star_avg'] = (reviews['star_1'] + reviews['star_2'] * 2 + reviews['star_3'] * 3 + reviews['star_4'] * 4 +
                           reviews['star_5'] * 5) / reviews['reviews']

    # 별점 합계
    reviews['star_total'] = reviews['star_1'] + reviews['star_2'] + reviews['star_3'] + reviews['star_4'] + reviews[
        'star_5']

    # 연도 제거
    reviews['date'] = reviews['date'].apply(MD)

    # Word Count and Top Word
    word_list = []
    for x in res_all:
        word_list.append(x)
    word_count = pd.DataFrame(word_list,
                              columns=['date_word', 'date', 'word', 'total_count', 'r1_count', 'r2_count', 'r3_count',
                                       'r4_count', 'r5_count'])

    word_total_count = pd.DataFrame(word_count.groupby('word')[
                                        'total_count', 'r1_count', 'r2_count', 'r3_count', 'r4_count', 'r5_count'].sum().reset_index())


    # 긍정, 부정 구분
    pos_df = pd.concat([rating_4, rating_5])
    neg_df = pd.concat([rating_1, rating_2, rating_3])

    return {'reviews': reviews,
            'word_total_count': word_total_count,
            'df': df,
            'pos_df': pos_df,
            'neg_df': neg_df,
            'word_count': word_count
            }

# INDEX

def index(request):
	return render(request, 'index.html')


# REVIEWS

class DateColumn(tables.Column):
    def render(self, value):
        return str(value)[:10]

class SimpleTable(tables.Table):
    date = DateColumn()
    class Meta:
        model = Raw
        attrs = {'class': 'paleblue'}
        orderable = True

def simple_list(request):
    queryset = Raw.objects.all().values('app', 'version', 'title', 'content', 'date', 'rating', 'lang')
    version_list = Raw.objects.order_by().values('version').distinct()

    query = ''
    app = ''
    version = ''
    lang =''

    if 'q' in request.GET :
        query = request.GET['q']
    if 'app' in request.GET:
        app = request.GET['app']
    if 'version' in request.GET:
        version = request.GET['version']
    if 'lang' in request.GET:
        lang = request.GET['lang']

    r_min = 0
    r_max = 5
    rating = (r_min, r_max)
    L = []
    if '1s' in request.GET:
        r1 = request.GET['1s']
        L.append(r1)
        r_min = min(x for x in L if x is not '')
        r_max = max(x for x in L if x is not '')
        rating = (r_min, r_max)
    if '2s' in request.GET:
        r2 = request.GET['2s']
        L.append(r2)
        r_min = min(x for x in L if x is not '')
        r_max = max(x for x in L if x is not '')
        rating = (r_min, r_max)
    if '3s' in request.GET:
        r3 = request.GET['3s']
        L.append(r3)
        r_min = min(x for x in L if x is not '')
        r_max = max(x for x in L if x is not '')
        rating = (r_min, r_max)
    if '4s' in request.GET:
        r4 = request.GET['4s']
        L.append(r4)
        r_min = min(x for x in L if x is not '')
        r_max = max(x for x in L if x is not '')
        rating = (r_min, r_max)
    if '5s' in request.GET:
        r5 = request.GET['5s']
        L.append(r5)
        r_min = min(x for x in L if x is not '')
        r_max = max(x for x in L if x is not '')
        rating = (r_min, r_max)

    days1 = datetime.date.today()
    days2 = datetime.date.today() - datetime.timedelta(days=99999)
    if 'days' in request.GET:
        days = request.GET['days']
        if days == '7d':
            days2 = datetime.date.today() - datetime.timedelta(days=7)
        elif days == '14d':
            days2 = datetime.date.today() - datetime.timedelta(days=14)
        elif days == '1m':
            days2 = datetime.date.today() - datetime.timedelta(days=30)
        elif days == '3m':
            days2 = datetime.date.today() - datetime.timedelta(days=90)
        elif days == '6m':
            days2 = datetime.date.today() - datetime.timedelta(days=180)
        elif days == '1y':
            days2 = datetime.date.today() - datetime.timedelta(days=365)
    if 'sta_date' in request.GET:
        days2 = request.GET['sta_date']
        if str(days2) == '':
            days2 = datetime.date.today() - datetime.timedelta(days=99999)
        days2 = pd.to_datetime(days2)
    if 'end_date' in request.GET:
        days1 = request.GET['end_date']
        if str(days1) == '':
            days1 = datetime.date.today()
        days1 = pd.to_datetime(days1)

    if request.GET:
        queryset = Raw.objects.filter(content__icontains=query, app__contains=app, version__contains=version, lang__contains=lang, rating__range=rating, date__gte=days2, date__lte=days1)

    table = SimpleTable(queryset)
    table.paginate(page=request.GET.get('page', 1), per_page=10)
    return render(request, 'simple_list.html', {'table': table, 'query':query, 'version':version_list })


# 차트 기본 디자인
line_chart_style = Style(
    tooltip_font_family='googlefont:Raleway',
    major_label_font_size=14,
    label_font_size=20,
    legend_font_size=18,
    title_font_size=30,
    opacity='.6',
    opacity_hover='.9',
    colors=['#0b9ce0'],
    background='transparent',
    plot_background='transparent')

stack_bar_chart_style = Style(
    tooltip_font_family='googlefont:Raleway',
    major_label_font_size=14,
    label_font_size=15,
    legend_font_size=18,
    title_font_size=30,
    background='transparent',
    plot_background='transparent',
    colors=('#FF0000', '#FF540D', '#F2BF27', '#F26699', '#E80C7A'))

bar_chart_style = Style(
    tooltip_font_family='googlefont:Raleway',
    major_label_font_size=18,
    label_font_size=20,
    legend_font_size=18,
    title_font_size=30,
    background='transparent',
    plot_background='transparent',
    colors=('#FF0000', '#FF540D', '#F2BF27', '#E80C7A', '#F26699', '#000000'))

pie_chart_style = Style(
    tooltip_font_family='googlefont:Raleway',
    major_label_font_size=18,
    label_font_size=20,
    legend_font_size=18,
    title_font_size=30,
    background='transparent',
    plot_background='transparent')



def review_trend(request):
    template = get_template('review_trend.html')
    version_list = Raw.objects.order_by().values('version').distinct()
    app = ''
    version = ''
    rating = ''
    lang =''
    days1 = datetime.date.today()
    days2 = datetime.date.today() - datetime.timedelta(days=99999)

    if 'app' in request.GET:
        app = request.GET['app']
    if 'version' in request.GET:
        version = request.GET['version']
    if 'rating' in request.GET:
        rating = request.GET['rating']
    if 'lang' in request.GET:
        lang = request.GET['lang']
    if 'days' in request.GET:
        days = request.GET['days']
        if days == '7d':
            days2 = datetime.date.today() - datetime.timedelta(days=7)
        elif days == '14d':
            days2 = datetime.date.today() - datetime.timedelta(days=14)
        elif days == '1m':
            days2 = datetime.date.today() - datetime.timedelta(days=30)
        elif days == '3m':
            days2 = datetime.date.today() - datetime.timedelta(days=90)
        elif days == '6m':
            days2 = datetime.date.today() - datetime.timedelta(days=180)
        elif days == '1y':
            days2 = datetime.date.today() - datetime.timedelta(days=365)
    if 'sta_date' in request.GET:
        days2 = request.GET['sta_date']
        if str(days2) == '':
            days2 = datetime.date.today() - datetime.timedelta(days=99999)
        days2 = pd.to_datetime(days2)
    if 'end_date' in request.GET:
        days1 = request.GET['end_date']
        if str(days1) == '':
            days1 = datetime.date.today()
        days1 = pd.to_datetime(days1)

    res, star_1, star_2, star_3, star_4, star_5, res_all, text_all = db()

    if request.GET:
        res = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_1 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=1, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_2 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=2, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_3 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=3, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_4 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=4, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_5 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=5, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))

    reviews = preprocess(res, star_1, star_2, star_3, star_4, star_5, res_all, text_all)['reviews']


    # Reviews 데이터
    R_line = pygal.Line(style=line_chart_style,
                        dots_size=5,
                        max_scale=1,
                        show_legend=False,
                        tooltip_border_radius=20)
    R_line.title = 'Reviews Volume'
    R_line.x_labels = map(str, reviews['date'])
    R_line.add('Reviws Timeline', reviews['reviews'],
               stroke_style={'width': 3, 'dasharray': '3, 6', 'linecap': 'round', 'linejoin': 'round'})
    R_line = R_line.render_data_uri()

    # Average Ratings 데이터
    S_line = pygal.Line(style=line_chart_style,
                        fill=True,
                        dots_size=5,
                        max_scale=1,
                        range=(1, 5),
                        show_legend=False,
                        tooltip_border_radius=20)

    S_line.title = 'Average Stars'
    S_line.x_labels = map(str, reviews['date'])
    S_line.add('Stars', round(reviews['star_avg'], 1))
    S_line = S_line.render_data_uri()

    # Ratings 데이터(개)
    SC_line = pygal.Line(style=bar_chart_style,
                         dots_size=5,
                         max_scale=1,
                         legend_box_size=18,
                         legend_at_bottom=True,
                         legend_at_bottom_columns=5,
                         stroke_style={'width': 3, 'dasharray': '3, 6', 'linecap': 'round', 'linejoin': 'round'},
                         tooltip_border_radius=20)

    SC_line.title = 'Stars Timeline'
    SC_line.x_labels = map(str, reviews['date'])
    SC_line.add('star 1', reviews['star_1'])
    SC_line.add('star 2', reviews['star_2'])
    SC_line.add('star 3', reviews['star_3'])
    SC_line.add('star 4', reviews['star_4'])
    SC_line.add('star 5', reviews['star_5'])

    SC_line = SC_line.render_data_uri()

    # Ratings 데이터(%)
    SR_bar = pygal.StackedBar(range=(0, 100),
                              stack_from_top=True,
                              style=stack_bar_chart_style,
                              legend_at_bottom=True,
                              legend_at_bottom_columns=5,
                              max_scale=1,
                              inverse_y_axis=True,
                              legend_box_size=18,
                              value_formatter=lambda x: '{}%'.format(x),
                              tooltip_border_radius=20)

    SR_bar.title = 'Stars Rating (%)'
    SR_bar.x_labels = map(str, reviews['date'])
    SR_bar.add('star 1', round(reviews['star_1'] / reviews['star_total'] * 100, 1))
    SR_bar.add('star 2', round(reviews['star_2'] / reviews['star_total'] * 100, 1))
    SR_bar.add('star 3', round(reviews['star_3'] / reviews['star_total'] * 100, 1))
    SR_bar.add('star 4', round(reviews['star_4'] / reviews['star_total'] * 100, 1))
    SR_bar.add('star 5', round(reviews['star_5'] / reviews['star_total'] * 100, 1))

    SR_bar = SR_bar.render_data_uri()

    return HttpResponse(template.render({'R_line': R_line, 'S_line': S_line, 'SC_line': SC_line, 'SR_bar': SR_bar, 'version':version_list}))


def word_table(request):
    template = get_template('word_table.html')
    version_list = Raw.objects.order_by().values('version').distinct()

    app = ''
    version = ''
    lang =''
    days1 = datetime.date.today()
    days2 = datetime.date.today() - datetime.timedelta(days=99999)

    if 'app' in request.GET:
        app = request.GET['app']
    if 'version' in request.GET:
        version = request.GET['version']
    if 'rating' in request.GET:
        rating = request.GET['rating']
    if 'lang' in request.GET:
        lang = request.GET['lang']
    if 'days' in request.GET:
        days = request.GET['days']
        if days == '7d':
            days2 = datetime.date.today() - datetime.timedelta(days=7)
        elif days == '14d':
            days2 = datetime.date.today() - datetime.timedelta(days=14)
        elif days == '1m':
            days2 = datetime.date.today() - datetime.timedelta(days=30)
        elif days == '3m':
            days2 = datetime.date.today() - datetime.timedelta(days=90)
        elif days == '6m':
            days2 = datetime.date.today() - datetime.timedelta(days=180)
        elif days == '1y':
            days2 = datetime.date.today() - datetime.timedelta(days=365)
    if 'sta_date' in request.GET:
        days2 = request.GET['sta_date']
        if str(days2) == '':
            days2 = datetime.date.today() - datetime.timedelta(days=99999)
        days2 = pd.to_datetime(days2)
    if 'end_date' in request.GET:
        days1 = request.GET['end_date']
        if str(days1) == '':
            days1 = datetime.date.today()
        days1 = pd.to_datetime(days1)

    sort_by = 'total_count'
    if 'rating' in request.GET:
        sort_by = request.GET['rating']

    res, star_1, star_2, star_3, star_4, star_5, res_all, text_all = db()
    if request.GET:
        res = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_1 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=1, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_2 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=2, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_3 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=3, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_4 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=4, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_5 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=5, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))


    word_total_count = preprocess(res, star_1, star_2, star_3, star_4, star_5, res_all, text_all)['word_total_count']
    word_total_count = word_total_count.sort_values(by=sort_by, ascending=False)
    top_word = word_total_count[:10].reset_index().drop('index', axis=1)

    word_pie = pygal.Pie(style=pie_chart_style, inner_radius=.4, print_values=True)
    word_pie.title = 'Word Top 10(%)'
    word_pie.add(top_word['word'][0], round(top_word['total_count'][0] / sum(top_word['total_count']) * 100, 1))
    word_pie.add(top_word['word'][1], round(top_word['total_count'][1] / sum(top_word['total_count']) * 100, 1))
    word_pie.add(top_word['word'][2], round(top_word['total_count'][2] / sum(top_word['total_count']) * 100, 1))
    word_pie.add(top_word['word'][3], round(top_word['total_count'][3] / sum(top_word['total_count']) * 100, 1))
    word_pie.add(top_word['word'][4], round(top_word['total_count'][4] / sum(top_word['total_count']) * 100, 1))
    word_pie.add(top_word['word'][5], round(top_word['total_count'][5] / sum(top_word['total_count']) * 100, 1))
    word_pie.add(top_word['word'][6], round(top_word['total_count'][6] / sum(top_word['total_count']) * 100, 1))
    word_pie.add(top_word['word'][7], round(top_word['total_count'][7] / sum(top_word['total_count']) * 100, 1))
    word_pie.add(top_word['word'][8], round(top_word['total_count'][8] / sum(top_word['total_count']) * 100, 1))
    word_pie.add(top_word['word'][9], round(top_word['total_count'][9] / sum(top_word['total_count']) * 100, 1))
    word_pie.value_formatter = lambda x: '%.1f%%' % x if x is not None else '∅'
    word_pie = word_pie.render_data_uri()

    word_bar = pygal.StackedBar(range=(0, 100),
                                stack_from_top=True,
                                style=stack_bar_chart_style,
                                legend_at_bottom=True,
                                legend_at_bottom_columns=5,
                                max_scale=1,
                                inverse_y_axis=True,
                                legend_box_size=18,
                                value_formatter=lambda x: '{}%'.format(x),
                                tooltip_border_radius=20,
                                x_label_rotation=20)
    word_bar.title = 'Top Word Stars (%)'
    word_bar.x_labels = map(str, top_word['word'])
    word_bar.add('1 star', top_word['r1_count'] / top_word['total_count'] * 100)
    word_bar.add('2 star', top_word['r2_count'] / top_word['total_count'] * 100)
    word_bar.add('3 star', top_word['r3_count'] / top_word['total_count'] * 100)
    word_bar.add('4 star', top_word['r4_count'] / top_word['total_count'] * 100)
    word_bar.add('5 star', top_word['r5_count'] / top_word['total_count'] * 100)
    word_bar.value_formatter = lambda x: '%.1f%%' % x if x is not None else '∅'
    word_bar = word_bar.render_data_uri()

    return HttpResponse(template.render({'word_pie': word_pie, 'word_bar': word_bar, 'version':version_list}))


def word_2_vec(request):
    template = get_template('word_2_vec.html')
    version_list = Raw.objects.order_by().values('version').distinct()

    app = ''
    version = ''
    lang = ''
    days1 = datetime.date.today()
    days2 = datetime.date.today() - datetime.timedelta(days=99999)

    if 'app' in request.GET:
        app = request.GET['app']
    if 'version' in request.GET:
        version = request.GET['version']
    if 'lang' in request.GET:
        lang = request.GET['lang']
    if 'days' in request.GET:
        days = request.GET['days']
        if days == '7d':
            days2 = datetime.date.today() - datetime.timedelta(days=7)
        elif days == '14d':
            days2 = datetime.date.today() - datetime.timedelta(days=14)
        elif days == '1m':
            days2 = datetime.date.today() - datetime.timedelta(days=30)
        elif days == '3m':
            days2 = datetime.date.today() - datetime.timedelta(days=90)
        elif days == '6m':
            days2 = datetime.date.today() - datetime.timedelta(days=180)
        elif days == '1y':
            days2 = datetime.date.today() - datetime.timedelta(days=365)
    if 'sta_date' in request.GET:
        days2 = request.GET['sta_date']
        if str(days2) == '':
            days2 = datetime.date.today() - datetime.timedelta(days=99999)
        days2 = pd.to_datetime(days2)
    if 'end_date' in request.GET:
        days1 = request.GET['end_date']
        if str(days1) == '':
            days1 = datetime.date.today()
        days1 = pd.to_datetime(days1)


    res, star_1, star_2, star_3, star_4, star_5, res_all, text_all = db()

    if request.GET:
        # print('app:',app, 'version:',version, 'lang:',lang, 'days:', days2, days1)
        res = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_1 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=1, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_2 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=2, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_3 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=3, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_4 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=4, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))
        star_5 = list(Raw.objects.filter(app__contains=app, version__contains=version, lang__contains=lang, rating=5, date__gte=days2, date__lte=days1).values('app', 'version', 'id', 'title', 'content', 'date', 'rating', 'lang'))

    pos_df = preprocess(res, star_1, star_2, star_3, star_4, star_5, res_all, text_all)['pos_df']
    neg_df = preprocess(res, star_1, star_2, star_3, star_4, star_5, res_all, text_all)['neg_df']
    word_total_count = preprocess(res, star_1, star_2, star_3, star_4, star_5, res_all, text_all)['word_total_count']
    word_count = preprocess(res, star_1, star_2, star_3, star_4, star_5, res_all, text_all)['word_count']
    df = preprocess(res, star_1, star_2, star_3, star_4, star_5, res_all, text_all)['df']

    # word2vec 검색 단어
    word_total_count = word_total_count.sort_values(by='total_count', ascending=False)
    top_word = word_total_count[:10].reset_index().drop('index', axis=1)
    search_word = top_word['word'][0]

    if 'q' in request.GET:
        query = request.GET['q']
        search_word = query

    # 랭킹 상위 순위
    topn = 20

    # 전체단어 word2vec
    dic_w2v = []
    for x in df['text']:
        dic_w2v.append(list(x.split(',')))
    w2v = models.Word2Vec(dic_w2v, size=len(word_count['word'].unique()), min_count=2)

    top20 = w2v.similar_by_word(search_word, topn=topn)

    upper_word = []
    for x in top20:
        upper_word.append(x[0])

    # 긍정단어 word2vec
    pos_dic = {}
    for x in pos_df['text']:
        for y in list(x.split(',')):
            pos_dic[y] = y

    pos_dic_w2v = []
    for x in pos_df['text']:
        pos_dic_w2v.append(list(x.split(',')))
    pos_w2v = models.Word2Vec(pos_dic_w2v, size=len(pos_dic), min_count=2)

    pos_top20 = pos_w2v.similar_by_word(search_word, topn=topn)

    pos_upper_word = []
    for x in pos_top20:
        pos_upper_word.append(x[0])

    # 부정단어 word2vec
    neg_dic = {}
    for x in neg_df['text']:
        for y in list(x.split(',')):
            neg_dic[y] = y

    neg_dic_w2v = []
    for x in neg_df['text']:
        neg_dic_w2v.append(list(x.split(',')))
    neg_w2v = models.Word2Vec(neg_dic_w2v, size=len(neg_dic), min_count=2)

    neg_top20 = neg_w2v.similar_by_word(search_word, topn=topn)

    neg_upper_word = []
    for x in neg_top20:
        neg_upper_word.append(x[0])

    # 검색단어 Count
    search_w = word_count[word_count['word'] == search_word]
    search_w['date'] = search_w['date'].apply(MD)

    W_line = pygal.Line(style=bar_chart_style,
                        dots_size=5,
                        max_scale=1,
                        legend_box_size=18,
                        legend_at_bottom=True,
                        legend_at_bottom_columns=6,
                        stroke_style={'width': 3, 'dasharray': '3, 6', 'linecap': 'round', 'linejoin': 'round'},
                        tooltip_border_radius=20)

    W_line.title = search_word.upper() + ' Trend'
    W_line.x_labels = map(str, search_w['date'])
    W_line.add('star 1', search_w['r1_count'])
    W_line.add('star 2', search_w['r2_count'])
    W_line.add('star 3', search_w['r3_count'])
    W_line.add('star 4', search_w['r4_count'])
    W_line.add('star 5', search_w['r5_count'])
    W_line.add('Total', search_w['total_count'])

    W_line = W_line.render_data_uri()

    context = {'W_line': W_line, 'range': range(1, topn + 1), 'upper_word': upper_word,
               'pos_upper_word': pos_upper_word, 'neg_upper_word': neg_upper_word, 'version':version_list}

    return HttpResponse(template.render(context))