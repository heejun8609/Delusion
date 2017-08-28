import pandas as pd
import datetime

from django.shortcuts import render
from django.template.loader import get_template
from django.http.response import HttpResponse
from viz.models import CastleBurn, CastleBurn_count
from django.db import connection
import Crawling

import pygal
from pygal.style import Style

import django_tables2 as tables

# 차트 기본 디자인

line_chart_style = Style(
    tooltip_font_family='googlefont:Raleway',
    major_label_font_size=18,
    label_font_size=15,
    legend_font_size=14,
    title_font_size=30,
    opacity='.6',
    opacity_hover='.9',
    colors=['#0b9ce0'],
    background='transparent',
    plot_background='transparent')

stack_bar_chart_style = Style(
    tooltip_font_family='googlefont:Raleway',
    major_label_font_size=18,
    label_font_size=15,
    legend_font_size=14,
    title_font_size=30,
    background='transparent',
    plot_background='transparent',
    colors=('#FF0000', '#FF540D', '#F2BF27', '#F26699', '#E80C7A'))

star_chart_style = Style(
    tooltip_font_family='googlefont:Raleway',
    major_label_font_size=18,
    label_font_size=15,
    legend_font_size=14,
    title_font_size=30,
    background='transparent',
    plot_background='transparent',
    colors=('#0095FF', '#FF70F5', '#FF0000', '#F2BF27', '#000000'))

card_chart_style = Style(
    tooltip_font_family='googlefont:Raleway',
    major_label_font_size=18,
    label_font_size=15,
    legend_font_size=14,
    title_font_size=30,
    background='transparent',
    plot_background='transparent',
    colors=('#0095FF', '#FF70F5', '#000000'))

issue_chart_style = Style(
    tooltip_font_family='googlefont:Raleway',
    major_label_font_size=18,
    label_font_size=15,
    legend_font_size=14,
    title_font_size=30,
    background='transparent',
    plot_background='transparent',
    colors=('#FF0000', '#0E8A07', '#4D078A', '#F2BF27'))

pie_chart_style = Style(
    tooltip_font_family='googlefont:Raleway',
    major_label_font_size=18,
    label_font_size=15,
    legend_font_size=14,
    title_font_size=30,
    background='transparent',
    plot_background='transparent')

env_word = ['system', 'error', 'devic', 'account', 'log', 'login', 'server', 'load', 'lag', 'network',
            'internet', 'connect', 'disconnect', 'glitch', 'crash', 'hacker', 'hack', 'delay', 'screen',
            'bug', 'unstable', 'ban', 'wifi', 'sign', 'lte', 'latenc', 'ping', 'freez', 'stuck',
            'respons', 'respond', 'irrespons', 'batteri', 'wait', 'fail', 'failur', 'unabl', 'cant', 'cheat',
            'cheater', 'elaps', 'drain', 'shutdown']
pay_word = ['pay', 'payment', 'paytowin', 'payoff', 'refund', 'money', 'cash', 'ruby', 'currenc',
            'transact', 'price', 'lost', 'bill', 'fraud', 'IAP', 'purchas', 'inapp']
neg_word = env_word + pay_word + ['complain', 'complaint', 'request', 'fix', 'work', 'claim', 'suggest', 'wrong',
                                  'wors', 'worst', 'bad', 'issu', 'disappoint', 'ignor', 'annoy', 'suck', 'fuck',
                                  'motherfuck',
                                  'hate', 'terribl', 'horribl', 'anger', 'angri', 'stupid', 'absurd', 'frustrat',
                                  'disgust', 'idiot',
                                  'ridicul', 'screw', 'bitch', 'scam', 'pathet', 'garbag', 'wast', 'delet', 'support',
                                  'care',
                                  'imposs', 'hell', 'serious', 'bull', 'uninstal', 'worthless', 'rob', 'bias', 'poor',
                                  'flaw',
                                  'dislik', 'unplay', 'rubbish', 'obnoxi', 'crap', 'mad', 'disgrac', 'horrend', 'gross',
                                  'atrocious',
                                  'greedi', 'greed', 'futil', 'asap', 'wtf', 'useless', 'quit', 'abandon', 'problem',
                                  'solv',
                                  'resolv', 'remov', 'confus', 'rip', 'ripoff', 'trash', 'unsatisf', 'stop', 'awful',
                                  'bore',
                                  'churn', 'stale', 'troubl', 'silli', 'difficult', 'difficulti', 'unacceptabl', 'sick',
                                  'piss', 'desperat',
                                  'dumb', 'forc', 'spend', 'spent', 'repeat', 'too', 'balanc', 'imbalanc', 'unbalanc',
                                  'outbalanc',
                                  'rebalanc', 'match', 'matchmak', 'matchup', 'fair', 'unfair', 'fairli', 'win', 'lose',
                                  'lopsid', 'nerf',
                                  'overpow', 'op', 'odd', 'rig', 'chanc', 'uneven', 'buff', 'smurf', 'broken', 'broke', 'close']


# Preprocessing Function
def YMD(x):
    return str(x)[:10]

def MD(x):
    return x[5:]

def mer_date(x, y):
    return pd.merge(x, y,  how='outer', on='date')

# 리뷰수, 별점수 병합
def mer_date_word(x, y):
    return pd.merge(x, y, how='outer', on=['date', 'word'])

# DB 데이터 호출
def db():
    # con = (app, version, lang, days1, days2)
    try:
        with connection.cursor() as curs:
            sql = "select * From app.castleburn_text"
            curs.execute(sql)
            text_all = curs.fetchall()

            sql = "select * FROM app.castleburn"
            curs.execute(sql)
            res = curs.fetchall()

            sql = "select * FROM app.castleburn where rating =%s"
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

            sql = "select * From app.app_patch"
            curs.execute(sql)
            pat = curs.fetchall()


    finally:
        connection.close()

    return res, star_1, star_2, star_3, star_4, star_5, text_all, pat

def preprocess(res, star_1, star_2, star_3, star_4, star_5, text_all, pat):

    patch = []
    for x in pat:
        patch.append(x)
    df_patch = pd.DataFrame(patch, columns=['date', 'patch'])
    df_patch['date'] = df_patch['date'].apply(YMD)

    text = []
    for x in text_all:
        text.append(x)
    df_text = pd.DataFrame(text, columns=['date', 'text'])

    row = []
    for x in res:
        row.append(x)
    df = pd.DataFrame(row, columns=['app', 'id', 'date', 'title', 'content', 'rating', 'lang'])
    df = pd.merge(df, df_text, on='date')
    df['date'] = df['date'].apply(YMD)

    rating_1 = []
    for x in star_1:
        rating_1.append(x)
    rating_1 = pd.DataFrame(rating_1,
                            columns=['app', 'id', 'date', 'title', 'content', 'rating', 'lang'])
    rating_1 = pd.merge(rating_1, df_text, on='date')
    rating_1['date'] = rating_1['date'].apply(YMD)

    rating_2 = []
    for x in star_2:
        rating_2.append(x)
    rating_2 = pd.DataFrame(rating_2,
                            columns=['app', 'id', 'date', 'title', 'content', 'rating', 'lang'])
    rating_2 = pd.merge(rating_2, df_text, on='date')
    rating_2['date'] = rating_2['date'].apply(YMD)

    rating_3 = []
    for x in star_3:
        rating_3.append(x)
    rating_3 = pd.DataFrame(rating_3,
                            columns=['app', 'id', 'date', 'title', 'content', 'rating', 'lang'])
    rating_3 = pd.merge(rating_3, df_text, on='date')
    rating_3['date'] = rating_3['date'].apply(YMD)

    rating_4 = []
    for x in star_4:
        rating_4.append(x)
    rating_4 = pd.DataFrame(rating_4,
                            columns=['app', 'id', 'date', 'title', 'content', 'rating', 'lang'])
    rating_4 = pd.merge(rating_4, df_text, on='date')
    rating_4['date'] = rating_4['date'].apply(YMD)

    rating_5 = []
    for x in star_5:
        rating_5.append(x)
    rating_5 = pd.DataFrame(rating_5,
                            columns=['app', 'id', 'date', 'title', 'content', 'rating', 'lang'])
    rating_5 = pd.merge(rating_5, df_text, on='date')
    rating_5['date'] = rating_5['date'].apply(YMD)

    # 긍정, 부정 구분
    pos_df = pd.concat([rating_4, rating_5])
    neg_df = pd.concat([rating_1, rating_2, rating_3])

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

    # 단어 별점 수
    df_word = [{'date': 0, 'word': 0}]
    for x, y in df.iterrows():
        for x in y['text'].split(','):
            df_word.append({'date': str(y['date'])[:10], 'word': x})
    df_word = pd.DataFrame(df_word)[1:]
    df_count = df_word.groupby(['date', 'word']).size().reset_index()
    df_count.columns = ['date', 'word', 'total_count']

    rating_1_word = [{'date': 0, 'word': 0}]
    for x, y in rating_1.iterrows():
        for x in y['text'].split(','):
            rating_1_word.append({'date': str(y['date'])[:10], 'word': x})
    rating_1_word = pd.DataFrame(rating_1_word)[1:]
    r1_count = rating_1_word.groupby(['date', 'word']).size().reset_index()
    r1_count.columns = ['date', 'word', 'r1_count']

    rating_2_word = [{'date': 0, 'word': 0}]
    for x, y in rating_2.iterrows():
        for x in y['text'].split(','):
            rating_2_word.append({'date': str(y['date'])[:10], 'word': x})
    rating_2_word = pd.DataFrame(rating_2_word)[1:]
    r2_count = rating_2_word.groupby(['date', 'word']).size().reset_index()
    r2_count.columns = ['date', 'word', 'r2_count']

    rating_3_word = [{'date': 0, 'word': 0}]
    for x, y in rating_3.iterrows():
        for x in y['text'].split(','):
            rating_3_word.append({'date': str(y['date'])[:10], 'word': x})
    rating_3_word = pd.DataFrame(rating_3_word)[1:]
    r3_count = rating_3_word.groupby(['date', 'word']).size().reset_index()
    r3_count.columns = ['date', 'word', 'r3_count']

    rating_4_word = [{'date': 0, 'word': 0}]
    for x, y in rating_4.iterrows():
        for x in y['text'].split(','):
            rating_4_word.append({'date': str(y['date'])[:10], 'word': x})
    rating_4_word = pd.DataFrame(rating_4_word)[1:]
    r4_count = rating_4_word.groupby(['date', 'word']).size().reset_index()
    r4_count.columns = ['date', 'word', 'r4_count']

    rating_5_word = [{'date': 0, 'word': 0}]
    for x, y in rating_5.iterrows():
        for x in y['text'].split(','):
            rating_5_word.append({'date': str(y['date'])[:10], 'word': x})
    rating_5_word = pd.DataFrame(rating_5_word)[1:]
    r5_count = rating_5_word.groupby(['date', 'word']).size().reset_index()
    r5_count.columns = ['date', 'word', 'r5_count']

    # 리뷰수, 별점수 병합
    dfs = [rat_1, rat_2, rat_3, rat_4, rat_5]
    for d in dfs:
        reviews = mer_date(reviews, d)
    reviews = reviews.fillna(0)

    # 평점 평균
    reviews['star_avg'] = (reviews['star_1'] + reviews['star_2'] * 2 + reviews['star_3'] * 3 + reviews['star_4'] * 4 +
                           reviews['star_5'] * 5) / reviews['reviews']

    # 총 평점 개수
    reviews['star_total'] = reviews['star_1'] + reviews['star_2'] + reviews['star_3'] + reviews['star_4'] + reviews[
        'star_5']

    r_counts = [r1_count, r2_count, r3_count, r4_count, r5_count]
    for r in r_counts:
        df_count = mer_date_word(df_count, r)
    df_count = df_count.fillna(0)

    word_list = []
    for x in df_count.iterrows():
        word_list.append(x[1])
    word_count = pd.DataFrame(word_list,
                              columns=['date', 'word', 'total_count', 'r1_count', 'r2_count', 'r3_count',
                                       'r4_count', 'r5_count'])
    word_count['date'] = word_count['date'].apply(YMD)

    word_total_count = pd.DataFrame(word_count.groupby('word')[
                                        'total_count', 'r1_count', 'r2_count', 'r3_count', 'r4_count', 'r5_count'].sum().reset_index())

    return {'reviews': reviews,
            'word_total_count': word_total_count,
            'df': df,
            'pos_df': pos_df,
            'neg_df': neg_df,
            'word_count': word_count,
            'df_patch': df_patch
            }

# INDEX
def index(request):
	return render(request, 'index.html')


# Issue trend
def issue_trend(request):

    template = get_template('issue_trend.html')
    app = ''
    lang =''
    days1 = datetime.date.today() - datetime.timedelta(days=1)
    days2 = days1 - datetime.timedelta(days=30)

    if 'app' in request.GET:
        app = request.GET['app']
    if 'rating' in request.GET:
        rating = request.GET['rating']
    if 'lang' in request.GET:
        lang = request.GET['lang']
    if 'sta_date' in request.GET:
        d2 = request.GET['sta_date']
        days2 = pd.to_datetime(d2)
        if d2 == '':
            days2 = datetime.date.today() - datetime.timedelta(days=30)
    if 'end_date' in request.GET:
        d1 = request.GET['end_date']
        days1 = pd.to_datetime(d1)
        if d1 == '':
             days1 = datetime.date.today() - datetime.timedelta(days=1)

    res, star_1, star_2, star_3, star_4, star_5, text_all, pat = db()

    # res = list(CastleBurn.objects.filter(date__gte=days2, date__lte=days1).values('app', 'id', 'title', 'content', 'date', 'rating', 'lang'))

    if request.GET:
        print('app:',app, 'lang:',lang, 'days:', days2, days1)
        res = list(CastleBurn.objects.filter(app__contains=app, lang__contains=lang, date__gte=days2, date__lte=days1).values('app', 'id', 'date', 'title', 'content', 'rating', 'lang'))
        star_1 = list(CastleBurn.objects.filter(app__contains=app, lang__contains=lang, rating=1, date__gte=days2, date__lte=days1).values('app', 'id', 'date', 'title', 'content', 'rating', 'lang'))
        star_2 = list(CastleBurn.objects.filter(app__contains=app, lang__contains=lang, rating=2, date__gte=days2, date__lte=days1).values('app', 'id', 'date', 'title', 'content', 'rating', 'lang'))
        star_3 = list(CastleBurn.objects.filter(app__contains=app, lang__contains=lang, rating=3, date__gte=days2, date__lte=days1).values('app', 'id', 'date', 'title', 'content', 'rating', 'lang'))
        star_4 = list(CastleBurn.objects.filter(app__contains=app, lang__contains=lang, rating=4, date__gte=days2, date__lte=days1).values('app', 'id', 'date', 'title', 'content', 'rating', 'lang'))
        star_5 = list(CastleBurn.objects.filter(app__contains=app, lang__contains=lang, rating=5, date__gte=days2, date__lte=days1).values('app', 'id', 'date', 'title', 'content', 'rating', 'lang'))

    df_patch = preprocess(res, star_1, star_2, star_3, star_4, star_5, text_all, pat)['df_patch']
    word_count = preprocess(res, star_1, star_2, star_3, star_4, star_5, text_all, pat)['word_count']

    # 이슈 Count

    neg_dic = {}
    for x in neg_word:
        neg_count = word_count[word_count['word'] == x]
        neg_dic[x] = neg_count

    env_dic = {}
    for x in env_word:
        env_count = word_count[word_count['word'] == x]
        env_dic[x] = env_count

    pay_dic = {}
    for x in pay_word:
        pay_count = word_count[word_count['word'] == x]
        pay_dic[x] = pay_count

    neg = neg_dic[neg_word[0]][['date', 'total_count']]
    for n in neg_word[1:]:
        neg = mer_date(neg, neg_dic[n][['date', 'total_count']])
    neg['N_total'] = neg.sum(axis=1)
    total_neg = pd.merge(word_count.groupby('date')['total_count'].sum().reset_index(),
                           pd.DataFrame(neg[['date', 'N_total']]), on='date', how='outer')
    total_neg = mer_date(total_neg, df_patch)
    neg_max = round(total_neg['total_count']/100, 2).max()
    patch_max = -neg_max
    total_neg['patch'] = total_neg['patch'].fillna(
        patch_max)
    total_neg = total_neg.fillna(0)
    total_neg['N_total'] = total_neg['N_total'].apply(int)
    total_neg = total_neg.sort_values(by='date')

    env = env_dic[env_word[0]][['date', 'total_count']]
    for n in env_word[1:]:
        env = mer_date(env, neg_dic[n][['date', 'total_count']])
    env['E_total'] = env.sum(axis=1)
    total_env = pd.merge(word_count.groupby('date')['total_count'].sum().reset_index(),
                               pd.DataFrame(env[['date', 'E_total']]), on='date', how='outer')
    total_env = mer_date(total_env, df_patch)
    total_env['patch'] = total_env['patch'].fillna(
        patch_max)
    total_env = total_env.fillna(0)
    total_env['E_total'] = total_env['E_total'].apply(int)
    total_env = total_env.sort_values(by='date')

    pay = pay_dic[pay_word[0]][['date', 'total_count']]
    for n in pay_word[1:]:
        pay = mer_date(pay, neg_dic[n][['date', 'total_count']])
    pay['P_total'] = pay.sum(axis=1)
    total_pay = pd.merge(word_count.groupby('date')['total_count'].sum().reset_index(),
                           pd.DataFrame(pay[['date', 'P_total']]), on='date', how='outer')
    total_pay = mer_date(total_pay, df_patch)
    total_pay['patch'] = total_pay['patch'].fillna(
        patch_max)
    total_pay = total_pay.fillna(0)
    total_pay['P_total'] = total_pay['P_total'].apply(int)
    total_pay = total_pay.sort_values(by='date')

    total_dic = {}
    for x in neg_word:
        x_count = word_count[word_count['word'] == x]
        total_dic[x] = x_count

    for x in neg_dic:
        for x, y in total_dic[x].iterrows():
            try:
                with connection.cursor() as cur:
                    date_word = str(y['date'])[:10] + ' ' + y['word']
                    data = (
                    date_word, y['date'], y['word'], y['total_count'], y['r1_count'], y['r2_count'], y['r3_count'], y['r4_count'], y['r5_count'])
                    sql = "insert into app.castleburn_count (date_word, date, word, total_count, r1_count, r2_count, r3_count, r4_count, r5_count) \
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cur.execute(sql, data)
                    connection.commit()

            except Exception as e:
                print('SQL error_message: ' + str(e))
                pass
    connection.close()


    L_line = pygal.Line(style=issue_chart_style,
                        dots_size=2,
                        max_scale=1,
                        legend_at_bottom=True,
                        legend_at_bottom_columns=6,
                        tooltip_border_radius=20,
                        interpolate='cubic',
                        stroke_style={'width': 1, 'dasharray': '3', 'linecap': 'round', 'linejoin': 'round'},
                        show_minor_x_labels=False,
                        truncate_label=-1,
                        truncate_legend=15,
                        x_label_rotation = 5)

    L_line.title = 'Issue Keyword Timeline (%)'
    L_line.y_labels = 0, neg_max*3/8, neg_max*3/4
    L_line.x_labels = map(str, total_neg['date'])
    L_line.x_labels_major = [total_neg['date'].values[0],
                             total_neg['date'].values[int((len(total_neg['date']) - 1) / 2)],
                             total_neg['date'].values[-1]]

    L_line.add('Negative', round(total_neg['N_total'] / total_neg['total_count'] * 100, 2))
    L_line.add('Environment', round(total_env['E_total'] / total_env['total_count'] * 100, 2))
    L_line.add('Payments', round(total_pay['P_total'] / total_pay['total_count'] * 100, 2))
    L_line.add('Patch', total_neg['patch'], dots_size=4)
    L_line = L_line.render_data_uri()

    context = {'L_line': L_line}

    return HttpResponse(template.render(context))

class DateColumn(tables.Column):
    def render(self, value):
        return str(value)[:10]

class IssueTable(tables.Table):
    date = DateColumn()
    class Meta:
        model = CastleBurn_count
        fields = ('date', 'word', 'total_count')
        attrs = {
                "th":{"align":"center"},
                "td": {"align": "center"}
                 }

# Issue Count
def issue_table(request):

    queryset = CastleBurn_count.objects.all().values('date', 'word', 'total_count')

    i_date = datetime.date.today() - datetime.timedelta(days=1)

    if 'app' in request.GET:
        app = request.GET['app']
    if 'rating' in request.GET:
        rating = request.GET['rating']
    if 'lang' in request.GET:
        lang = request.GET['lang']
    if 'issue_date' in request.GET:
        d3 = request.GET['issue_date']
        i_date = pd.to_datetime(d3)

    if 'issue' in request.GET:
        if 'neg' == request.GET['issue']:
            queryset = CastleBurn_count.objects.filter(word__in=neg_word, date__gte=i_date, date__lte=i_date).values('date',
                                                                                                                 'word',
                                                                                                                 'total_count')
        elif 'env' == request.GET['issue']:
            queryset = CastleBurn_count.objects.filter(word__in=env_word, date__gte=i_date, date__lte=i_date).values('date',
                                                                                                                 'word',
                                                                                                                 'total_count')
        else:
            queryset = CastleBurn_count.objects.filter(word__in=pay_word, date__gte=i_date, date__lte=i_date).values('date',
                                                                                                                 'word',
                                                                                                                 'total_count')

    table = IssueTable(queryset.order_by('-date').order_by("-total_count"))
    table.paginate(page=request.GET.get('page', 1), per_page=20)

    return render(request, 'issue_table.html', {'table': table})

# REVIEWS

class DateColumn(tables.Column):
    def render(self, value):
        return str(value)[:10]

class SimpleTable(tables.Table):
    date = DateColumn()
    class Meta:
        order_by = '-date'
        model = CastleBurn
        attrs = {
                "td": {"align": "left"}
                 }


def simple_list(request):
    queryset = CastleBurn.objects.all().values('app', 'id', 'date', 'title', 'content',  'rating', 'lang')

    query = ''
    app = ''
    lang =''

    if 'q' in request.GET :
        query = request.GET['q']
    if 'app' in request.GET:
        app = request.GET['app']
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
        print(query, days2, days1)
        queryset = CastleBurn.objects.filter(content__icontains=query, app__contains=app, lang__contains=lang, rating__range=rating, date__gte=days2, date__lte=days1).values('app', 'id', 'date', 'title', 'content',  'rating', 'lang')

    table = SimpleTable(queryset.order_by('-date').order_by('rating'))
    table.paginate(page=request.GET.get('page', 1), per_page=10)
    return render(request, 'simple_list.html', {'table': table, 'query':query, })

Crawling.Scheduler()