import pymysql
import pandas as pd
import numpy as np
import datetime
import re

import os
import sys
import smtplib
# For guessing MIME type based on file name extension
import mimetypes

from argparse import ArgumentParser

from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

def job():
    today = datetime.date.today()
    yester = str(today-datetime.timedelta(days=1))
    sta_date = datetime.datetime.strptime('2017-08-11', "%Y-%m-%d")

    def master_acc():
        connection = pymysql.connect(host='cb-real-master-seoul-db-master-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_master', charset='utf8')
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM rts_master._account")
            fet = cursor.fetchall()
        connection.close()
        columns = []
        for x in cursor.description:
            columns.append(x[0])
        df = []
        for x in fet:
            df.append(x)
        df = pd.DataFrame(df, columns=columns)
        return df

    def user_game_log():
        connection0 = pymysql.connect(host='cb-real-singapore-db-user0-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0_log', charset='utf8')
        with connection0.cursor() as cursor0:
            cursor0.execute("SELECT * FROM rts_user0_log._user_game_log")
            fet0 = cursor0.fetchall()
        connection0.close()
        columns0 = []
        for x in cursor0.description:
            columns0.append(x[0])
        df0 = []
        for x in fet0:
            df0.append(x)
        df0 = pd.DataFrame(df0, columns=columns0)

        connection1 = pymysql.connect(host='cb-real-singapore-db-user1-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user1_log', charset='utf8')
        with connection1.cursor() as cursor1:
            cursor1.execute("SELECT * FROM rts_user1_log._user_game_log")
            fet1 = cursor1.fetchall()
        connection1.close()
        columns1 = []
        for x in cursor1.description:
            columns1.append(x[0])
        df1 = []
        for x in fet1:
            df1.append(x)
        df1 = pd.DataFrame(df1, columns=columns1)

        connection2 = pymysql.connect(host='cb-real-california-db-user0-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0_log', charset='utf8')
        with connection2.cursor() as cursor2:
            cursor2.execute("SELECT * FROM rts_user0_log._user_game_log")
            fet2 = cursor2.fetchall()
        connection2.close()
        columns2 = []
        for x in cursor2.description:
            columns2.append(x[0])
        df2 = []
        for x in fet2:
            df2.append(x)
        df2 = pd.DataFrame(df2, columns=columns2)

        connection3 = pymysql.connect(host='cb-real-london-db-user0-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0_log', charset='utf8')
        with connection3.cursor() as cursor3:
            cursor3.execute("SELECT * FROM rts_user0_log._user_game_log")
            fet3 = cursor3.fetchall()
        connection3.close()
        columns3 = []
        for x in cursor3.description:
            columns3.append(x[0])
        df3 = []
        for x in fet3:
            df3.append(x)
        df3 = pd.DataFrame(df3, columns=columns3)

        connection4 = pymysql.connect(host='cb-real-seoul-db-user0-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0_log', charset='utf8')
        with connection4.cursor() as cursor4:
            cursor4.execute("SELECT * FROM rts_user0_log._user_game_log")
            fet4 = cursor4.fetchall()
        connection4.close()
        columns4 = []
        for x in cursor4.description:
            columns4.append(x[0])
        df4 = []
        for x in fet4:
            df4.append(x)
        df4 = pd.DataFrame(df4, columns=columns4)

        df5 = df0.append([df1, df2, df3, df4], ignore_index=True)
        return df5

    def user_game():
        connection0 = pymysql.connect(host='cb-real-singapore-db-user0-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0', charset='utf8')
        with connection0.cursor() as cursor0:
            cursor0.execute("SELECT * FROM rts_user0._user_game")
            fet0 = cursor0.fetchall()
        connection0.close()
        columns0 = []
        for x in cursor0.description:
            columns0.append(x[0])
        df0 = []
        for x in fet0:
            df0.append(x)
        df0 = pd.DataFrame(df0, columns=columns0)

        connection1 = pymysql.connect(host='cb-real-singapore-db-user1-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user1', charset='utf8')
        with connection1.cursor() as cursor1:
            cursor1.execute("SELECT * FROM rts_user1._user_game")
            fet1 = cursor1.fetchall()
        connection1.close()
        columns1 = []
        for x in cursor1.description:
            columns1.append(x[0])
        df1 = []
        for x in fet1:
            df1.append(x)
        df1 = pd.DataFrame(df1, columns=columns1)

        connection2 = pymysql.connect(host='cb-real-california-db-user0-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0', charset='utf8')
        with connection2.cursor() as cursor2:
            cursor2.execute("SELECT * FROM rts_user0._user_game")
            fet2 = cursor2.fetchall()
        connection2.close()
        columns2 = []
        for x in cursor2.description:
            columns2.append(x[0])
        df2 = []
        for x in fet2:
            df2.append(x)
        df2 = pd.DataFrame(df2, columns=columns2)

        connection3 = pymysql.connect(host='cb-real-london-db-user0-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0', charset='utf8')
        with connection3.cursor() as cursor3:
            cursor3.execute("SELECT * FROM rts_user0._user_game")
            fet3 = cursor3.fetchall()
        connection3.close()
        columns3 = []
        for x in cursor3.description:
            columns3.append(x[0])
        df3 = []
        for x in fet3:
            df3.append(x)
        df3 = pd.DataFrame(df3, columns=columns3)

        connection4 = pymysql.connect(host='cb-real-seoul-db-user0-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0', charset='utf8')
        with connection4.cursor() as cursor4:
            cursor4.execute("SELECT * FROM rts_user0._user_game")
            fet4 = cursor4.fetchall()
        connection4.close()
        columns4 = []
        for x in cursor4.description:
            columns4.append(x[0])
        df4 = []
        for x in fet4:
            df4.append(x)
        df4 = pd.DataFrame(df4, columns=columns4)

        df5 = df0.append([df1, df2, df3, df4], ignore_index=True)
        return df5

    def user_match_log_begin():
        connection0 = pymysql.connect(host='cb-real-singapore-db-user0-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0_log', charset='utf8')
        with connection0.cursor() as cursor0:
            cursor0.execute("SELECT * FROM rts_user0_log._user_match_log_begin")
            fet0 = cursor0.fetchall()
        connection0.close()
        columns0 = []
        for x in cursor0.description:
            columns0.append(x[0])
        df0 = []
        for x in fet0:
            df0.append(x)
        df0 = pd.DataFrame(df0, columns=columns0)

        connection1 = pymysql.connect(host='cb-real-singapore-db-user1-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user1_log', charset='utf8')
        with connection1.cursor() as cursor1:
            cursor1.execute("SELECT * FROM rts_user1_log._user_match_log_begin")
            fet1 = cursor1.fetchall()
        connection1.close()
        columns1 = []
        for x in cursor1.description:
            columns1.append(x[0])
        df1 = []
        for x in fet1:
            df1.append(x)
        df1 = pd.DataFrame(df1, columns=columns1)

        connection2 = pymysql.connect(host='cb-real-california-db-user0-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0_log', charset='utf8')
        with connection2.cursor() as cursor2:
            cursor2.execute("SELECT * FROM rts_user0_log._user_match_log_begin")
            fet2 = cursor2.fetchall()
        connection2.close()
        columns2 = []
        for x in cursor2.description:
            columns2.append(x[0])
        df2 = []
        for x in fet2:
            df2.append(x)
        df2 = pd.DataFrame(df2, columns=columns2)

        connection3 = pymysql.connect(host='cb-real-london-db-user0-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0_log', charset='utf8')
        with connection3.cursor() as cursor3:
            cursor3.execute("SELECT * FROM rts_user0_log._user_match_log_begin")
            fet3 = cursor3.fetchall()
        connection3.close()
        columns3 = []
        for x in cursor3.description:
            columns3.append(x[0])
        df3 = []
        for x in fet3:
            df3.append(x)
        df3 = pd.DataFrame(df3, columns=columns3)

        connection4 = pymysql.connect(host='cb-real-seoul-db-user0-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0_log', charset='utf8')
        with connection4.cursor() as cursor4:
            cursor4.execute("SELECT * FROM rts_user0_log._user_match_log_begin")
            fet4 = cursor4.fetchall()
        connection4.close()
        columns4 = []
        for x in cursor4.description:
            columns4.append(x[0])
        df4 = []
        for x in fet4:
            df4.append(x)
        df4 = pd.DataFrame(df4, columns=columns4)

        df5 = df0.append([df1, df2, df3, df4], ignore_index=True)
        return df5

    def user_match_log_end():
        connection0 = pymysql.connect(host='cb-real-singapore-db-user0-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0_log', charset='utf8')
        with connection0.cursor() as cursor0:
            cursor0.execute("SELECT * FROM rts_user0_log._user_match_log_end")
            fet0 = cursor0.fetchall()
        connection0.close()
        columns0 = []
        for x in cursor0.description:
            columns0.append(x[0])
        df0 = []
        for x in fet0:
            df0.append(x)
        df0 = pd.DataFrame(df0, columns=columns0)

        connection1 = pymysql.connect(host='cb-real-singapore-db-user1-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user1_log', charset='utf8')
        with connection1.cursor() as cursor1:
            cursor1.execute("SELECT * FROM rts_user1_log._user_match_log_end")
            fet1 = cursor1.fetchall()
        connection1.close()
        columns1 = []
        for x in cursor1.description:
            columns1.append(x[0])
        df1 = []
        for x in fet1:
            df1.append(x)
        df1 = pd.DataFrame(df1, columns=columns1)

        connection2 = pymysql.connect(host='cb-real-california-db-user0-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0_log', charset='utf8')
        with connection2.cursor() as cursor2:
            cursor2.execute("SELECT * FROM rts_user0_log._user_match_log_end")
            fet2 = cursor2.fetchall()
        connection2.close()
        columns2 = []
        for x in cursor2.description:
            columns2.append(x[0])
        df2 = []
        for x in fet2:
            df2.append(x)
        df2 = pd.DataFrame(df2, columns=columns2)

        connection3 = pymysql.connect(host='cb-real-london-db-user0-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0_log', charset='utf8')
        with connection3.cursor() as cursor3:
            cursor3.execute("SELECT * FROM rts_user0_log._user_match_log_end")
            fet3 = cursor3.fetchall()
        connection3.close()
        columns3 = []
        for x in cursor3.description:
            columns3.append(x[0])
        df3 = []
        for x in fet3:
            df3.append(x)
        df3 = pd.DataFrame(df3, columns=columns3)

        connection4 = pymysql.connect(host='cb-real-seoul-db-user0-log-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0_log', charset='utf8')
        with connection4.cursor() as cursor4:
            cursor4.execute("SELECT * FROM rts_user0_log._user_match_log_end")
            fet4 = cursor4.fetchall()
        connection4.close()
        columns4 = []
        for x in cursor4.description:
            columns4.append(x[0])
        df4 = []
        for x in fet4:
            df4.append(x)
        df4 = pd.DataFrame(df4, columns=columns4)

        df5 = df0.append([df1, df2, df3, df4], ignore_index=True)
        return df5

    def user_match_log():
        connection0 = pymysql.connect(host='cb-real-singapore-db-user0-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0', charset='utf8')
        with connection0.cursor() as cursor0:
            cursor0.execute("SELECT * FROM rts_user0._user_match_log")
            fet0 = cursor0.fetchall()
        connection0.close()
        columns0 = []
        for x in cursor0.description:
            columns0.append(x[0])
        df0 = []
        for x in fet0:
            df0.append(x)
        df0 = pd.DataFrame(df0, columns=columns0)

        connection1 = pymysql.connect(host='cb-real-singapore-db-user1-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user1', charset='utf8')
        with connection1.cursor() as cursor1:
            cursor1.execute("SELECT * FROM rts_user1._user_match_log")
            fet1 = cursor1.fetchall()
        connection1.close()
        columns1 = []
        for x in cursor1.description:
            columns1.append(x[0])
        df1 = []
        for x in fet1:
            df1.append(x)
        df1 = pd.DataFrame(df1, columns=columns1)

        connection2 = pymysql.connect(host='cb-real-california-db-user0-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0', charset='utf8')
        with connection2.cursor() as cursor2:
            cursor2.execute("SELECT * FROM rts_user0._user_match_log")
            fet2 = cursor2.fetchall()
        connection2.close()
        columns2 = []
        for x in cursor2.description:
            columns2.append(x[0])
        df2 = []
        for x in fet2:
            df2.append(x)
        df2 = pd.DataFrame(df2, columns=columns2)

        connection3 = pymysql.connect(host='cb-real-london-db-user0-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0', charset='utf8')
        with connection3.cursor() as cursor3:
            cursor3.execute("SELECT * FROM rts_user0._user_match_log")
            fet3 = cursor3.fetchall()
        connection3.close()
        columns3 = []
        for x in cursor3.description:
            columns3.append(x[0])
        df3 = []
        for x in fet3:
            df3.append(x)
        df3 = pd.DataFrame(df3, columns=columns3)

        connection4 = pymysql.connect(host='cb-real-seoul-db-user0-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0', charset='utf8')
        with connection4.cursor() as cursor4:
            cursor4.execute("SELECT * FROM rts_user0._user_match_log")
            fet4 = cursor4.fetchall()
        connection4.close()
        columns4 = []
        for x in cursor4.description:
            columns4.append(x[0])
        df4 = []
        for x in fet4:
            df4.append(x)
        df4 = pd.DataFrame(df4, columns=columns4)

        df5 = df0.append([df1, df2, df3, df4], ignore_index=True)
        return df5

    def user_league():
        connection0 = pymysql.connect(host='cb-real-singapore-db-user0-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0', charset='utf8')
        with connection0.cursor() as cursor0:
            cursor0.execute("SELECT * FROM rts_user0._user_league")
            fet0 = cursor0.fetchall()
        connection0.close()
        columns0 = []
        for x in cursor0.description:
            columns0.append(x[0])
        df0 = []
        for x in fet0:
            df0.append(x)
        df0 = pd.DataFrame(df0, columns=columns0)

        connection1 = pymysql.connect(host='cb-real-singapore-db-user1-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user1', charset='utf8')
        with connection1.cursor() as cursor1:
            cursor1.execute("SELECT * FROM rts_user1._user_league")
            fet1 = cursor1.fetchall()
        connection1.close()
        columns1 = []
        for x in cursor1.description:
            columns1.append(x[0])
        df1 = []
        for x in fet1:
            df1.append(x)
        df1 = pd.DataFrame(df1, columns=columns1)

        connection2 = pymysql.connect(host='cb-real-california-db-user0-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0', charset='utf8')
        with connection2.cursor() as cursor2:
            cursor2.execute("SELECT * FROM rts_user0._user_league")
            fet2 = cursor2.fetchall()
        connection2.close()
        columns2 = []
        for x in cursor2.description:
            columns2.append(x[0])
        df2 = []
        for x in fet2:
            df2.append(x)
        df2 = pd.DataFrame(df2, columns=columns2)

        connection3 = pymysql.connect(host='cb-real-london-db-user0-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0', charset='utf8')
        with connection3.cursor() as cursor3:
            cursor3.execute("SELECT * FROM rts_user0._user_league")
            fet3 = cursor3.fetchall()
        connection3.close()
        columns3 = []
        for x in cursor3.description:
            columns3.append(x[0])
        df3 = []
        for x in fet3:
            df3.append(x)
        df3 = pd.DataFrame(df3, columns=columns3)

        connection4 = pymysql.connect(host='cb-real-seoul-db-user0-replica.c730awg3qklt.ap-northeast-2.rds.amazonaws.com',
                                     user='cb_user', passwd='cb!23viewer11', db='rts_user0', charset='utf8')
        with connection4.cursor() as cursor4:
            cursor4.execute("SELECT * FROM rts_user0._user_league")
            fet4 = cursor4.fetchall()
        connection4.close()
        columns4 = []
        for x in cursor4.description:
            columns4.append(x[0])
        df4 = []
        for x in fet4:
            df4.append(x)
        df4 = pd.DataFrame(df4, columns=columns4)

        df5 = df0.append([df1, df2, df3, df4], ignore_index=True)
        return df5

    master_acc = master_acc()
    user_game_log = user_game_log()
    user_game = user_game()
    user_match_log_begin = user_match_log_begin()
    user_match_log_end = user_match_log_end()
    user_match_log = user_match_log()
    user_league = user_league()

    ### 캐슬별 리그별 분포

    # 캐슬 레벨 분류
    def CL(row):
        if row['castle_level'] == 1:
            return 15
        elif row['castle_level'] == 2:
            return 40
        elif row['castle_level'] == 3:
            return 80
        elif row['castle_level'] == 4:
            return 150
        elif row['castle_level'] == 5:
            return 300
        elif row['castle_level'] == 6:
            return 600
        elif row['castle_level'] == 7:
            return 1200
        elif row['castle_level'] == 8:
            return 2400
        elif row['castle_level'] == 9:
            return 4500
        elif row['castle_level'] == 10:
            return 9000
        elif row['castle_level'] == 11:
            return 18000
        elif row['castle_level'] == 12:
            return 35000
        elif row['castle_level'] == 13:
            return 65000
        elif row['castle_level'] == 14:
            return 120000
        else:
            return 0


    # 캐슬 경험치 분류
    def CE(row):
        if row['castle_exp'] <= row['castle_max_exp'] / 4:
            return 0
        elif row['castle_exp'] > row['castle_max_exp'] / 4 and row['castle_exp'] <= row['castle_max_exp'] / 2:
            return .25
        elif row['castle_exp'] > row['castle_max_exp'] / 2 and row['castle_exp'] <= row['castle_max_exp'] * 3 / 4:
            return .5
        elif row['castle_exp'] > row['castle_max_exp'] * 3 / 4:
            return .75
        else:
            return 1


    # CP 분류
    def CP(row):
        if row['cur_point'] == 0:
            return '01) 0'
        elif row['cur_point'] >= 1 and row['cur_point'] < 50:
            return '02) 1~49'
        elif row['cur_point'] >= 50 and row['cur_point'] < 100:
            return '03) 50~99'
        elif row['cur_point'] >= 100 and row['cur_point'] < 150:
            return '04)100~149'
        elif row['cur_point'] >= 150 and row['cur_point'] < 200:
            return '05) 150~199'
        elif row['cur_point'] >= 200 and row['cur_point'] < 250:
            return '06) 200~249'
        elif row['cur_point'] >= 250 and row['cur_point'] < 300:
            return '07) 250~299'
        elif row['cur_point'] >= 300 and row['cur_point'] < 350:
            return '08) 300~349'
        elif row['cur_point'] >= 350 and row['cur_point'] < 400:
            return '09) 350~399'
        elif row['cur_point'] >= 400 and row['cur_point'] < 450:
            return '10) 400~449'
        elif row['cur_point'] >= 450 and row['cur_point'] < 500:
            return '11) 450~499'
        elif row['cur_point'] >= 500 and row['cur_point'] < 550:
            return '12) 500~549'
        elif row['cur_point'] >= 550 and row['cur_point'] < 600:
            return '13) 550~599'
        elif row['cur_point'] >= 600 and row['cur_point'] < 650:
            return '14) 600~649'
        elif row['cur_point'] >= 650 and row['cur_point'] < 700:
            return '15) 650~699'
        elif row['cur_point'] >= 700 and row['cur_point'] < 750:
            return '16) 700~749'
        elif row['cur_point'] >= 750 and row['cur_point'] < 800:
            return '17) 750~799'
        elif row['cur_point'] >= 800 and row['cur_point'] < 850:
            return '18) 800~849'
        elif row['cur_point'] >= 850 and row['cur_point'] < 900:
            return '19) 850~899'
        elif row['cur_point'] >= 900 and row['cur_point'] < 950:
            return '20) 900~949'
        elif row['cur_point'] >= 950 and row['cur_point'] < 1000:
            return '21) 950~999'
        elif row['cur_point'] >= 1000 and row['cur_point'] < 1050:
            return '22) 1000~1049'
        elif row['cur_point'] >= 1050 and row['cur_point'] < 1100:
            return '23) 1050~1099'
        elif row['cur_point'] >= 1100 and row['cur_point'] < 1150:
            return '24) 1100~1149'
        elif row['cur_point'] >= 1150 and row['cur_point'] < 1200:
            return '25) 1150~1199'
        elif row['cur_point'] >= 1200 and row['cur_point'] < 1250:
            return '26) 1200~1249'
        elif row['cur_point'] >= 1250 and row['cur_point'] < 1300:
            return '27) 1250~1299'
        elif row['cur_point'] >= 1300 and row['cur_point'] < 1350:
            return '28) 1300~1349'
        elif row['cur_point'] >= 1350 and row['cur_point'] < 1400:
            return '29) 1350~1399'
        elif row['cur_point'] >= 1400 and row['cur_point'] < 1450:
            return '30) 1400~1449'
        elif row['cur_point'] >= 1450 and row['cur_point'] < 1500:
            return '31) 1450~1499'
        elif row['cur_point'] >= 1500 and row['cur_point'] < 1550:
            return '32) 1500~1549'
        elif row['cur_point'] >= 1550 and row['cur_point'] < 1600:
            return '33) 1550~1599'
        elif row['cur_point'] >= 1600 and row['cur_point'] < 1650:
            return '34) 1600~1649'
        elif row['cur_point'] >= 1650 and row['cur_point'] < 1700:
            return '35) 1650~1699'
        elif row['cur_point'] >= 1700 and row['cur_point'] < 1750:
            return '36) 1700~1749'
        elif row['cur_point'] >= 1750 and row['cur_point'] < 1800:
            return '37) 1750~1799'
        elif row['cur_point'] >= 1800 and row['cur_point'] < 1850:
            return '38) 1800~1849'
        elif row['cur_point'] >= 1850 and row['cur_point'] < 1900:
            return '39) 1850~1899'
        elif row['cur_point'] >= 1900 and row['cur_point'] < 1950:
            return '40) 1900~1949'
        elif row['cur_point'] >= 1950 and row['cur_point'] < 2000:
            return '41) 1950~1999'
        elif row['cur_point'] >= 2000 and row['cur_point'] < 2050:
            return '42) 2000~2049'
        elif row['cur_point'] >= 2050 and row['cur_point'] < 2100:
            return '43) 2050~2099'
        elif row['cur_point'] >= 2100 and row['cur_point'] < 2150:
            return '44) 2100~2149'
        elif row['cur_point'] >= 2150 and row['cur_point'] < 2200:
            return '45) 2150~2199'
        elif row['cur_point'] >= 2200 and row['cur_point'] < 2250:
            return '46) 2200~2249'
        elif row['cur_point'] >= 2250 and row['cur_point'] < 2300:
            return '47) 2250~2299'
        elif row['cur_point'] >= 2300 and row['cur_point'] < 2350:
            return '48) 2300~2349'
        elif row['cur_point'] >= 2350 and row['cur_point'] < 2400:
            return '49) 2350~2399'
        elif row['cur_point'] >= 2400 and row['cur_point'] < 2450:
            return '50) 2400~2449'
        elif row['cur_point'] >= 2450 and row['cur_point'] < 2500:
            return '51) 2450~2499'
        else:
            return row['cur_point']


    # CP 분류
    def CP_league(row):
        if row['cur_point'] == 0:
            return '01) 0'
        elif row['cur_point'] >= 1 and row['cur_point'] < 150:
            return '02) 1~149'
        elif row['cur_point'] >= 150 and row['cur_point'] < 300:
            return '03) 150~299'
        elif row['cur_point'] >= 300 and row['cur_point'] < 600:
            return '04) 300~599'
        elif row['cur_point'] >= 600 and row['cur_point'] < 900:
            return '05) 600~899'
        elif row['cur_point'] >= 900 and row['cur_point'] < 1200:
            return '06) 900~1199'
        elif row['cur_point'] >= 1200 and row['cur_point'] < 1500:
            return '07) 1200~1499'
        elif row['cur_point'] >= 1500 and row['cur_point'] < 1800:
            return '08) 1500~1799'
        elif row['cur_point'] >= 1800 and row['cur_point'] < 2100:
            return '09) 1800~2099'
        elif row['cur_point'] >= 2100 and row['cur_point'] < 2400:
            return '10) 2100~2399'
        elif row['cur_point'] >= 2400 and row['cur_point'] < 2700:
            return '11) 2400~2699'
        elif row['cur_point'] >= 2700 and row['cur_point'] < 3000:
            return '12) 2700~2999'
        elif row['cur_point'] >= 3000 and row['cur_point'] < 3300:
            return '13) 3000~3299'
        elif row['cur_point'] >= 3300 and row['cur_point'] < 3600:
            return '14) 3300~3599'
        elif row['cur_point'] >= 3600 and row['cur_point'] < 3900:
            return '15) 3600~3899'
        elif row['cur_point'] >= 3900 and row['cur_point'] < 4200:
            return '16) 3900~4199'
        elif row['cur_point'] >= 4200 and row['cur_point'] < 4500:
            return '17) 4200~4499'
        elif row['cur_point'] >= 4500 and row['cur_point'] < 4800:
            return '18) 4500~4799'
        elif row['cur_point'] >= 4800 and row['cur_point'] < 5100:
            return '19) 4800~5099'
        elif row['cur_point'] >= 5100 and row['cur_point'] < 5400:
            return '20) 5100~5399'
        else:
            return row['cur_point']


    # 유저 분류
    all_user = user_game_log[(user_game_log['event_time'] < str(today)) &
                             (user_game_log['event_type'] == 1)].loc[:, ['user_id']]

    day1_act_user = user_game_log[(user_game_log['event_time'] >= yester) &
                                  (user_game_log['event_time'] < str(today)) &
                                  (user_game_log['event_type'] == 1)].loc[:, ['user_id']]

    day3_act_user = user_game_log[(user_game_log['event_time'] >= str(today - datetime.timedelta(days=3))) &
                                  (user_game_log['event_time'] < str(today)) &
                                  (user_game_log['event_type'] == 1)].loc[:, ['user_id']]

    day2_deact_user = user_game_log[(user_game_log['event_time'] <= str(today - datetime.timedelta(days=2))) &
                                    (user_game_log['event_type'] == 1)].loc[:, ['user_id']]

    fri_weekend_act_user = user_game_log[(user_game_log['event_time'] >= str(today - datetime.timedelta(days=3))) &
                                         (user_game_log['event_time'] < str(today)) &
                                         (user_game_log['event_type'] == 1)].loc[:, ['user_id']]

    # 캐슬 경험치 분류
    user_game['castle_max_exp'] = user_game.apply(CL, axis=1)
    castle_df = user_game.loc[:, ['user_id', 'castle_level', 'castle_exp', 'castle_max_exp']]
    castle_df['castle_exp_cat'] = castle_df.apply(CE, axis=1)
    castle_df['castle_cat'] = castle_df['castle_level'] + castle_df['castle_exp_cat']
    castle_cat = castle_df.loc[:, ['user_id', 'castle_cat']]

    # 리그 50포인트 분류
    league = user_league.loc[:, ['user_id', 'cur_point']]
    league['cur_point_cat'] = league.apply(CP, axis=1)
    league_dr = league.drop('cur_point', axis=1)

    # 캐슬 분류
    user_game['castle_max_exp'] = user_game.apply(CL, axis=1)
    castle_lv = user_game.loc[:, ['user_id', 'castle_level']]

    # 리그 분류
    league = user_league.loc[:, ['user_id', 'cur_point']]
    league['cur_point_cat'] = league.apply(CP_league, axis=1)
    league_cat = league.drop('cur_point', axis=1)


    # 유저별 테이블 분류
    def cat_le_time(con, league):
        if con == 'all':
            cat_le_res = all_user.merge(castle_cat, on='user_id')
            cat_le_res = league.merge(cat_le_res, on='user_id')
        elif con == 'day1_act':
            cat_le_res = day1_act_user.merge(castle_cat, on='user_id')
            cat_le_res = league.merge(cat_le_res, on='user_id')
        elif con == 'day3_act':
            cat_le_res = day3_act_user.merge(castle_cat, on='user_id')
            cat_le_res = league.merge(cat_le_res, on='user_id')
        elif con == 'day2_deact':
            cat_le_res = day2_deact_user.merge(castle_cat, on='user_id')
            cat_le_res = league.merge(cat_le_res, on='user_id')
        elif con == 'weekend':
            cat_le_res = fri_weekend_act_user.merge(castle_cat, on='user_id')
            cat_le_res = league.merge(cat_le_res, on='user_id')

        def cat_le_result(x):
            cat_le_count = x.groupby(['castle_cat', 'cur_point_cat'])['user_id'].unique().apply(len).reset_index()
            y = cat_le_count.pivot_table(index='cur_point_cat', columns='castle_cat', values='user_id').fillna(0).astype(
                int)
            y['Total'] = y.sum(axis=1)
            y.ix['Total'] = y.sum(axis=0)

            # cat_le_cum = cat_le_count.pivot_table(index='cur_point_cat', columns='castle_cat', values='user_id').fillna(0).astype(int)[::-1].cumsum(axis=0)

            y.to_excel('./1. league_castle/1.1) ' + yester + '_catle_league_' + con + '.xlsx')
            # cat_le_res.to_excel(str(today)+'_catle_league_day2_deact.xlsx')
            # cat_le_cum[::-1].to_excel(str(today)+'_catle_league_cumsum.xlsx')

        cat_le_result(cat_le_res)

        return cat_le_res

    # all, day1_act, day3_act, day2_deact, weekend
    all_cat_le = cat_le_time('all', league_dr)
    d3_cat_le = cat_le_time('day3_act', league_cat)
    d3_cat_le_50 = cat_le_time('day3_act', league_dr)

    d3_user_cat = d3_cat_le.groupby('user_id')['cur_point_cat'].all().reset_index()
    le_cat = d3_user_cat.groupby('cur_point_cat')['user_id'].size().reset_index().set_index('cur_point_cat')
    le_cat = le_cat.reindex(index=le_cat.index[::-1])
    le_cat.loc['Total'] = le_cat[0].sum(axis=0)
    total_user = le_cat.ix['Total'][0]
    def percent(row):
        return row/row[7]
    le_cat[yester] = le_cat.apply(percent)
    le_cat = le_cat.drop(0, axis=1)
    le_cat.loc['D-day'] = str(today - sta_date.date())[:7]
    le_cat = le_cat.reindex(index=le_cat.index[::-1])

    cas_lv = castle_lv.groupby('castle_level')['user_id'].size().reset_index().set_index('castle_level')
    cas_lv.loc['Total'] = cas_lv[0].sum(axis=0)
    def percent(row):
        return row/row['Total']
    cas_lv[yester] = cas_lv.apply(percent)
    cas_lv[yester] = cas_lv.apply(percent)
    cas_lv = cas_lv.drop(0, axis=1)
    cas_lv = cas_lv.drop('Total')

    le_cas_lv = le_cat.append(cas_lv).transpose().rename({0:yester})
    le_cas_lv['Total'] = total_user

    le_cas_lv.to_excel('./1. league_castle/1.2) '+yester + '_3day_league_castle_base.xlsx')

    ### 평균 매칭 대기 시간 & 평균 점수 차이(랭크)

    #### 매칭 대기시간
    wait_time_sec = user_match_log_begin[(user_match_log_begin['event_time'] >= yester) &
                                         (user_match_log_begin['event_time'] < str(today)) &
                                         (user_match_log_begin['match_type'] == 1)].loc[:,
                    ['user_id', 'opponent_id', 'point', 'wait_time_sec']]
    wait_time_sec = wait_time_sec.rename(columns={'point': 'cur_point'})
    wait_time_sec['cur_point_cat'] = wait_time_sec.apply(CP, axis=1)

    le_wait_ai = wait_time_sec[wait_time_sec['opponent_id'] == 0]
    le_wait_user = wait_time_sec[wait_time_sec['opponent_id'] != 0]

    all_wait = round(wait_time_sec.groupby('cur_point_cat')['wait_time_sec'].mean(), 1).reset_index()
    ai_wait = round(le_wait_ai.groupby('cur_point_cat')['wait_time_sec'].mean(), 1).reset_index()
    user_wait = round(le_wait_user.groupby('cur_point_cat')['wait_time_sec'].mean(), 1).reset_index()

    all_wait.columns = ['cur_point_cat', 'wait_time_sec_all']
    ai_wait.columns = ['cur_point_cat', 'wait_time_sec_ai']
    user_wait.columns = ['cur_point_cat', 'wait_time_sec_user']

    all_wait = all_wait.merge(ai_wait, on='cur_point_cat', how='outer')
    ai_user_all = all_wait.merge(user_wait, on='cur_point_cat', how='outer')

    #### 매칭 성공률
    match_try = pd.DataFrame(user_game_log[(user_game_log['event_time'] >= str(today - datetime.timedelta(days=2))) &
                                           (user_game_log['event_time'] < str(today)) &
                                           (user_game_log['event_type'] == 10) &
                                           (user_game_log['param1'] == 1)].loc[:, ['user_id', 'event_time']])

    match_succ = pd.DataFrame(
        user_match_log_begin[(user_match_log_begin['event_time'] >= str(today - datetime.timedelta(days=2))) &
                             (user_match_log_begin['event_time'] < str(today)) &
                             (user_match_log_begin['match_type'] == 1)].loc[:, ['user_id', 'point', 'event_time']])
    match_succ = match_succ.rename(columns={'point': 'cur_point'})

    match_succ['match'] = 1
    match_try['match'] = 0

    match_all = match_succ.append(match_try)
    match_all = match_all.sort_values(['user_id', 'event_time'])

    u_list = []
    cp = ''
    for x, y in match_all.iterrows():
        if y['user_id'] not in u_list and y.isnull().any():
            match_all.loc[x, 'cur_point'] = 0
            cp = match_all.loc[x, 'cur_point']
        elif y.isnull().any():
            match_all.loc[x, 'cur_point'] = cp
            cp = match_all.loc[x, 'cur_point']
        else:
            cp = y['cur_point']
        u_list.append(y['user_id'])

    match_all['cur_point_cat'] = match_all.apply(CP, axis=1)

    match = match_all[(match_all['event_time'] >= yester) &
                      (match_all['event_time'] < str(today))].loc[:, ['match', 'cur_point_cat']]
    match_success = round((match[match['match'] == 1].groupby('cur_point_cat')['match'].size() /
                           match[match['match'] == 0].groupby('cur_point_cat')['match'].size()).reset_index(), 2)

    #### CP 차이
    point = user_match_log_begin[(user_match_log_begin['event_time'] >= yester) &
                                 (user_match_log_begin['event_time'] < str(today)) &
                                 (user_match_log_begin['match_type'] == 1)].loc[:,
            ['user_id', 'opponent_id', 'point', 'opponent_point']]
    point['point_diff'] = abs(point['point'] - point['opponent_point'])
    point = point.loc[:, ['user_id', 'point', 'opponent_id', 'point_diff']]
    point = point.rename(columns={'point': 'cur_point'})
    point['cur_point_cat'] = point.apply(CP, axis=1)
    point_ai = point[point['opponent_id'] == 0]
    point_user = point[point['opponent_id'] != 0]

    point_diff = pd.DataFrame(point.groupby('cur_point_cat')['point_diff'].mean().round(1))
    point_diff_ai = pd.DataFrame(point_ai.groupby('cur_point_cat')['point_diff'].mean().round(1))
    point_diff_user = pd.DataFrame(point_user.groupby('cur_point_cat')['point_diff'].mean().round(1))

    point_diff.columns = ['point_diff_all']
    point_diff_ai.columns = ['point_diff_ai']
    point_diff_user.columns = ['point_diff_user']

    point_dif = point_diff.join([point_diff_ai, point_diff_user], ).reset_index()

    second_cp_diff = ai_user_all.merge(match_success, on='cur_point_cat', how='outer')
    final_cp_diff = second_cp_diff.merge(point_dif, on='cur_point_cat', how='outer')
    final_cp_diff.columns = ['총 평균 매칭 시간', '평균 AI 매칭 시간', '평균 유저 매칭 시간',
                             '매칭 성공률(%)', '총 평균 점수 차이', '평균 AI 점수 차이', '평균 유저 점수 차이']
    final_cp_diff = ai_user_all.merge(point_dif, on='cur_point_cat', how='outer')
    final_cp_diff.set_index('cur_point_cat', inplace=True)
    # final_cp_diff.columns = ['총 평균 매칭 시간', '평균 AI 매칭 시간', '평균 유저 매칭 시간',
    #                          '총 평균 점수 차이', '평균 AI 점수 차이', '평균 유저 점수 차이']

    final_cp_diff.to_excel('./2. wait_cp/2) '+yester+'_wait_cpdiff.xlsx')

    ### 매칭비율
    def type_name(x):
        if x['match_type'] == 1:
            return 'rank'
        elif x['match_type'] == 2:
            return 'casual'
        elif x['match_type'] == 3:
            return 'tutorial'
        elif x['match_type'] == 4:
            return 'train'
        elif x['match_type'] == 5:
            return 'friend'


    def match_rat():
        user_match_log_end_temp = user_match_log_end[(user_match_log_end['event_time'] >= yester)
                                                     & (user_match_log_end['event_time'] < str(today))]
        user_match_log_begin_temp = user_match_log_begin[(user_match_log_begin['event_time'] >= yester)
                                                         & (user_match_log_begin['event_time'] < str(today))]

        #     user_match_log_end_temp = user_match_log_end[(user_match_log_end['event_time'] >= str(today-datetime.timedelta(days=2)))
        #                                              & (user_match_log_end['event_time'] < yester)]
        #     user_match_log_begin_temp = user_match_log_begin[(user_match_log_begin['event_time'] >= str(today-datetime.timedelta(days=2)))
        #                                                      & (user_match_log_begin['event_time'] < yester)]

        #     user_match_log_end_temp = user_match_log_end[(user_match_log_end['event_time'] >= str(today-datetime.timedelta(days=3)))
        #                                                  & (user_match_log_end['event_time'] < str(today-datetime.timedelta(days=2)))]
        #     user_match_log_begin_temp = user_match_log_begin[(user_match_log_begin['event_time'] >= str(today-datetime.timedelta(days=3)))
        #                                                      & (user_match_log_begin['event_time'] < str(today-datetime.timedelta(days=2)))]

        match_win = user_match_log_end_temp.loc[:, ['user_id', 'match_id', 'win', 'elapsed_sec', 'end_reason']]

        match_type = user_match_log_begin_temp.loc[:, ['user_id', 'match_id', 'opponent_id', 'match_type']]

        match_df = match_type.merge(match_win, on=['user_id', 'match_id'])

        # 1(타운홀 파괴, 항복), 3(접속종료)
        #     if end_reason == 1:
        match_df = match_df[(match_df['end_reason'] == 2) | (match_df['end_reason'] == 6)]
        #     elif end_reason == 3:
        #         match_df = match_df[(match_df['end_reason'] == 3)]

        match_ai = match_df[(match_df['opponent_id'] == 0)]
        match_user = match_df[(match_df['opponent_id'] != 0)]

        # 무승부
        match_draw_all = match_df[match_df['elapsed_sec'] >= 360]
        match_draw_ai = match_df[(match_df['elapsed_sec'] >= 360) & (match_df['opponent_id'] == 0)]
        match_draw_user = match_df[(match_df['elapsed_sec'] >= 360) & (match_df['opponent_id'] != 0)]

        # 전체 매칭 비율
        match_df_all = match_df.groupby('match_type')['match_id'].size().reset_index()
        match_df_all.columns = ['match_type', 'All_count']

        match_df_all['All(%)'] = round(match_df_all['All_count'] / (match_df_all['All_count'].sum()), 2)

        match_draw_all = match_draw_all.groupby('match_type')['match_id'].size().reset_index()
        match_draw_all.columns = ['match_type', 'draw_count']
        match_table = match_df_all.merge(match_draw_all, how='outer', on='match_type')

        match_table['All_draw(%)'] = round(match_table['draw_count'] / match_table['All_count'], 2)
        match_table = match_table.drop('draw_count', axis=1)

        # 유저 매칭비율
        match_user_all = match_user.groupby('match_type')['match_id'].size().reset_index()
        match_user_all.columns = ['match_type', 'User_count']

        match_user_all['User(%)'] = round(match_user_all['User_count'] / match_df_all['All_count'], 2)

        match_draw_user = match_draw_user.groupby('match_type')['match_id'].size().reset_index()
        match_draw_user.columns = ['match_type', 'user_draw_count']
        match_user_table = match_user_all.merge(match_draw_user, on='match_type', how='outer')

        match_user_table['User_draw(%)'] = round(match_user_table['user_draw_count'] / match_user_table['User_count'], 2)
        match_user_table = match_user_table.drop('user_draw_count', axis=1)

        # AI 매칭 비율
        match_ai_all = match_ai.groupby('match_type')['match_id'].size().reset_index()
        match_ai_all.columns = ['match_type', 'AI_count']

        match_ai_all['AI(%)'] = round(match_ai_all['AI_count'] / match_df_all['All_count'], 2)

        match_ai_win = match_ai[match_ai['win'] == 1]
        match_ai_win = match_ai_win.groupby('match_type')['win'].size().reset_index()
        match_ai_win.columns = ['match_type', 'ai_win_count']
        match_ai_win['AI_win(%)'] = round(match_ai_win['ai_win_count'] / match_ai_all['AI_count'], 2)

        match_ai_table = match_ai_all.merge(match_ai_win, on='match_type', how='outer')
        match_ai_table = match_ai_table.drop('ai_win_count', axis=1)

        match_draw_ai = match_draw_ai.groupby('match_type')['match_id'].size().reset_index()
        match_draw_ai.columns = ['match_type', 'ai_draw_count']
        match_ai_table = match_ai_table.merge(match_draw_ai, on='match_type', how='outer')

        match_ai_table['AI_draw(%)'] = round(match_ai_table['ai_draw_count'] / match_ai_all['AI_count'], 2)
        match_ai_table = match_ai_table.drop('ai_draw_count', axis=1)

        # 테이블 결합
        match_table = match_table.merge(match_user_table, how='outer', on='match_type')
        match_table = match_table.merge(match_ai_table, how='outer', on='match_type')
        match_table['match_type'] = match_table.apply(type_name, axis=1)
        match_table = match_table.fillna(0)

        # 1(타운홀 파괴, 항복), 3(접속종료)
        #     if end_reason == 1:
        match_table.to_excel('./3. match_ratio/3) ' + yester + '_match_ratio.xlsx')
        #     elif end_reason == 3:
        #         match_table.to_excel(yester+'_match_ratio_discon.xlsx')

    match_rat()

    ### 카드 픽률 및 승률

    match_end = user_match_log_end.loc[:, ['user_id', 'match_id', 'event_time', 'end_reason', 'win',
                               'unlock_card_ref_id_1', 'unlock_card_ref_id_2', 'unlock_card_ref_id_3',
                               'unlock_card_ref_id_4', 'unlock_card_ref_id_5', 'unlock_card_ref_id_6']]
    match_begin = user_match_log_begin.loc[:, ['user_id', 'match_id', 'opponent_id', 'castle_level', 'match_type', 'point', 'hero']]
    ml_temp = match_begin.merge(match_end, on=['user_id', 'match_id'])

    card_temp = ml_temp[(ml_temp['match_type'] == 1) & # 랭크
                        ((ml_temp['end_reason'] == 2) | (ml_temp['end_reason'] == 6)) & # 타운홀 파괴(2), 항복(6)
                        (ml_temp['point'] >= 600) & # 600점 이상
                        (ml_temp['castle_level'] >= 3) &
                        (ml_temp['event_time'] >= yester) &
                        (ml_temp['event_time'] < str(today))].loc[:, ['user_id', 'win', 'opponent_id', 'castle_level', 'hero',
                                                                      'unlock_card_ref_id_1', 'unlock_card_ref_id_2', 'unlock_card_ref_id_3',
                                                                      'unlock_card_ref_id_4', 'unlock_card_ref_id_5', 'unlock_card_ref_id_6']]

    card_ai = card_temp[card_temp['opponent_id'] == 0]
    card_user = card_temp[card_temp['opponent_id'] != 0]
    card_all = [card_user, card_temp]

    #### 영웅
    hero_all = pd.DataFrame()
    for card in card_all:
        jea_dic = {}
        mer_dic = {}
        leo_dic = {}
        ael_dic = {}
        gru_dic = {}
        lan_dic = {}

        for x in card.iterrows():
            if 1000 in x[1].values[4:]:
                jea_dic[x[0]] = x[1]
            elif 1001 in x[1].values[4:]:
                mer_dic[x[0]] = x[1]
            elif 1002 in x[1].values[4:]:
                leo_dic[x[0]] = x[1]
            elif 1003 in x[1].values[4:]:
                ael_dic[x[0]] = x[1]
            elif 1004 in x[1].values[4:]:
                gru_dic[x[0]] = x[1]
            elif 1005 in x[1].values[4:]:
                lan_dic[x[0]] = x[1]

        jea = pd.DataFrame(jea_dic).transpose()
        leo = pd.DataFrame(leo_dic).transpose()
        ael = pd.DataFrame(ael_dic).transpose()
        gru = pd.DataFrame(gru_dic).transpose()

        jea_win = round(len(jea[jea['win'] == 1]) / len(jea), 2)
        leo_win = round(len(leo[leo['win'] == 1]) / len(leo), 2)
        ael_win = round(len(ael[ael['win'] == 1]) / len(ael), 2)
        gru_win = round(len(gru[gru['win'] == 1]) / len(gru), 2)

        hero_df = pd.DataFrame({'Win Ratio(%)': [jea_win, leo_win, ael_win, gru_win, np.nan],
                                'Battle Count': [len(jea), len(leo), len(ael), len(gru),
                                                 len(jea) + len(leo) + len(ael) + len(gru)],
                                'User Count': [len(jea.groupby('user_id')['win'].size()),
                                               len(leo.groupby('user_id')['win'].size()),
                                               len(ael.groupby('user_id')['win'].size()),
                                               len(gru.groupby('user_id')['win'].size()),
                                               len(jea.groupby('user_id')['win'].size()) +
                                               len(leo.groupby('user_id')['win'].size()) +
                                               len(ael.groupby('user_id')['win'].size()) +
                                               len(gru.groupby('user_id')['win'].size())]},
                               index=['Jeanne', 'Leon', 'Aella', 'Gruvo', 'Sum'])
        if len(hero_all) == 0:
            hero_all = hero_df
        else:
            hero_all = hero_all.join(hero_df, lsuffix='_User', rsuffix='_All')

    #### 포탑
    tower_all = pd.DataFrame()
    for card in card_all:
        tower_dic = {}
        archer_dic = {}
        cannon_dic = {}
        no_tower_dic = {}

        for x in card.iterrows():
            tower_dic[np.nan] = {'user_id': np.nan, 'win': np.nan, 'opponent_id': np.nan, 'castle_level': np.nan,
                                 'hero': np.nan,
                                 'unlock_card_ref_id_1': np.nan, 'unlock_card_ref_id_2': np.nan,
                                 'unlock_card_ref_id_3': np.nan,
                                 'unlock_card_ref_id_4': np.nan, 'unlock_card_ref_id_5': np.nan,
                                 'unlock_card_ref_id_6': np.nan}
            if 10 in x[1].values[4:] and 11 in x[1].values[4:]:
                tower_dic[x[0]] = x[1]
            elif 10 in x[1].values[4:] and 11 not in x[1].values[4:]:
                archer_dic[x[0]] = x[1]
            elif 10 not in x[1].values[4:] and 11 in x[1].values[4:]:
                cannon_dic[x[0]] = x[1]
            elif 10 not in x[1].values[4:] and 11 not in x[1].values[4:] and 0 != x[1].values[4:].sum():
                no_tower_dic[x[0]] = x[1]

        tower = pd.DataFrame(tower_dic).transpose()
        archer = pd.DataFrame(archer_dic).transpose()
        cannon = pd.DataFrame(cannon_dic).transpose()
        no_tower = pd.DataFrame(no_tower_dic).transpose()

        tower_win = round(len(tower[tower['win'] == 1]) / len(tower), 2)
        archer_win = round(len(archer[archer['win'] == 1]) / len(archer), 2)
        cannon_win = round(len(cannon[cannon['win'] == 1]) / len(cannon), 2)
        no_tower_win = round(len(no_tower[no_tower['win'] == 1]) / len(no_tower), 2)

        tower_df = pd.DataFrame({'Win Ratio(%)': [no_tower_win, archer_win, cannon_win, tower_win, np.nan],
                                 'Battle Count': [len(no_tower), len(archer), len(cannon), len(tower) - 1,
                                                  len(no_tower) + len(archer) + len(cannon) + len(tower) - 1],
                                 'User Count': [len(no_tower.groupby('user_id')['win'].size()),
                                                len(archer.groupby('user_id')['win'].size()),
                                                len(cannon.groupby('user_id')['win'].size()),
                                                len(tower.groupby('user_id')['win'].size()),
                                                len(no_tower.groupby('user_id')['win'].size()) +
                                                len(archer.groupby('user_id')['win'].size()) +
                                                len(cannon.groupby('user_id')['win'].size()) +
                                                len(tower.groupby('user_id')['win'].size())]},
                                index=['No Tower', 'Archer', 'Cannon', 'Towers', 'Sum'])

        if len(tower_all) == 0:
            tower_all = tower_df
        else:
            tower_all = tower_all.join(tower_df, lsuffix='_User', rsuffix='_All')

    #### 스펠
    spel_all = pd.DataFrame()
    for card in card_all:
        spel_dic = {}
        boom_dic = {}
        fire_dic = {}
        no_spel_dic = {}
        # zero_dic = {}


        for x in card.iterrows():
            spel_dic[np.nan] = {'user_id': np.nan, 'win': np.nan, 'opponent_id': np.nan, 'castle_level': np.nan,
                                'hero': np.nan,
                                'unlock_card_ref_id_1': np.nan, 'unlock_card_ref_id_2': np.nan,
                                'unlock_card_ref_id_3': np.nan,
                                'unlock_card_ref_id_4': np.nan, 'unlock_card_ref_id_5': np.nan,
                                'unlock_card_ref_id_6': np.nan}
            if 400 in x[1].values[4:] and 401 in x[1].values[4:]:
                spel_dic[x[0]] = x[1]
            elif 400 in x[1].values[4:] and 401 not in x[1].values[4:]:
                boom_dic[x[0]] = x[1]
            elif 400 not in x[1].values[4:] and 401 in x[1].values[4:]:
                fire_dic[x[0]] = x[1]
            elif 400 not in x[1].values[4:] and 401 not in x[1].values[4:] and 0 != x[1].values[4:].sum():
                no_spel_dic[x[0]] = x[1]
        # elif 0 == x[1].values[2:].sum():
        #         zero_dic[x[0]] = x[1]

        spel = pd.DataFrame(spel_dic).transpose()
        boom = pd.DataFrame(boom_dic).transpose()
        fire = pd.DataFrame(fire_dic).transpose()
        no_spel = pd.DataFrame(no_spel_dic).transpose()
        # zero = pd.DataFrame(zero_dic).transpose()

        spel_win = round(len(spel[spel['win'] == 1]) / len(spel), 2)
        boom_win = round(len(boom[boom['win'] == 1]) / len(boom), 2)
        fire_win = round(len(fire[fire['win'] == 1]) / len(fire), 2)
        no_spel_win = round(len(no_spel[no_spel['win'] == 1]) / len(no_spel), 2)
        # zero_win = round(len(zero[zero['win'] == 1])/len(zero), 2)

        spel_df = pd.DataFrame({'Win Ratio(%)': [no_spel_win, boom_win, fire_win, spel_win, np.nan],
                                'Battle Count': [len(no_spel), len(boom), len(fire), len(spel) - 1,
                                                 len(no_spel) + len(boom) + len(fire) + len(spel) - 1],
                                'User Count': [len(no_spel.groupby('user_id')['win'].size()),
                                               len(boom.groupby('user_id')['win'].size()),
                                               len(fire.groupby('user_id')['win'].size()),
                                               len(spel.groupby('user_id')['win'].size()),
                                               len(no_spel.groupby('user_id')['win'].size()) +
                                               len(boom.groupby('user_id')['win'].size()) +
                                               len(fire.groupby('user_id')['win'].size()) +
                                               len(spel.groupby('user_id')['win'].size())]},
                               index=['No Spell', 'Boom', 'Fire', 'Spells', 'Sum'])
        if len(spel_all) == 0:
            spel_all = spel_df
        else:
            spel_all = spel_all.join(spel_df, lsuffix='_User', rsuffix='_All')

    hero_all.append([tower_all, spel_all]).to_excel('./4. card_use/4) '+yester+'_pick_win_ratio(hero, tower, spell).xlsx')

    #### 마법사
    witch = user_game_log[(user_game_log['param_type'] == 20) &
                          (user_game_log['param1'] == 90) &
                          (user_game_log['param2'] == 202)].loc[:, ['user_id','event_time']].sort_values('event_time')
    witch = witch.groupby('user_id')['event_time'].apply(lambda x: str(x)[6:19].strip()).reset_index()


    wit_mat = ml_temp[(ml_temp['match_type'] == 1) &
                      ((ml_temp['end_reason'] == 2) | (ml_temp['end_reason'] == 6)) &
                      (ml_temp['point'] >= 600) &
                      (ml_temp['event_time'] >= yester) &
                      (ml_temp['event_time'] < str(today))].loc[:, ['user_id', 'win',
                                                                                     'unlock_card_ref_id_1', 'unlock_card_ref_id_2',
                                                                                     'unlock_card_ref_id_3', 'unlock_card_ref_id_4',
                                                                                     'unlock_card_ref_id_5', 'unlock_card_ref_id_6']]

    witch_card = wit_mat.merge(witch, on ='user_id')
    witch_card  = witch_card .drop('event_time', axis=1)

    witch_dic = {}

    for x in witch_card.iterrows():
        if x[1].values[2:].sum() != 0:
            witch_dic[x[0]] = x[1]

    witch_df = pd.DataFrame(witch_dic).transpose()

    pick_card = ['unlock_card_ref_id_1', 'unlock_card_ref_id_2', 'unlock_card_ref_id_3',
                 'unlock_card_ref_id_4', 'unlock_card_ref_id_5', 'unlock_card_ref_id_6']

    for card in pick_card:
        witch_df[card] = np.where(witch_df[card] == 202, 1, 0)

    witch_df = witch_df.rename(columns = {'win':'마법사'})
    witch_df  = witch_df.drop('user_id', axis=1)


    second = [witch_df]
    s_dic = {}
    for x in second:
        s_dic[x.columns[0]] = {'pick_rate(%)' : round(x.drop(x.columns[0], axis=1).sum(axis=0).sum() / len(x), 2),
                               'win_rate(%)' : round(x[x[x.columns[0]] == 1].drop(x.columns[0], axis=1).sum(axis=0).sum() / x.drop(x.columns[0], axis=1).sum(axis=0).sum(), 2)}

    pd.DataFrame(s_dic).transpose().to_excel('./5. witch_use/5) '+yester+'_witch_pic_win_ratio.xlsx')


    # 이메일 발송
    # !/usr/bin/env python3

    """Send the contents of a directory as a MIME message."""


    parser = ArgumentParser(description="""\
    Send the contents of a directory as a MIME message.
    Unless the -o option is given, the email is sent by forwarding to your local
    SMTP server, which then does the normal delivery process.  Your local machine
    must be running an SMTP server.
    """)
    parser.add_argument('-d', '--directory',
                        help="""Mail the contents of the specified directory,
                        otherwise use the current directory.  Only the regular
                        files in the directory are sent, and we don't recurse to
                        subdirectories.""")
    parser.add_argument('-o', '--output',
                        metavar='FILE',
                        help="""Print the composed message to FILE instead of
                        sending the message to the SMTP server.""")
    parser.add_argument('-s', '--sender', required=True,
                        help='The value of the From: header (required)')
    parser.add_argument('-r', '--recipient', required=True,
                        action='append', metavar='RECIPIENT',
                        default=[], dest='recipients',
                        help='A To: header value (at least one required)')

    text = "Hi!"
    part1 = MIMEText(text, 'plain')
    outer = MIMEMultipart()
    outer['Subject'] = str(today) + ' Daily Statistics'
    outer['To'] = 'vmurmurv@naver.com'
    # outer['To'] = '곽웅섭 <palmblad@delusionstudio.net>,\
    #             조대윤 <alsum@delusionstudio.net>, \
    #             박신찬 <gatou@delusionstudio.net>, \
    #             강문철 <smith@delusionstudio.net>, \
    #             이재호 <jhdlee920@delusionstudio.net>, \
    #             곽호신 <gorapa90@delusionstudio.net>, \
    #             박희준 <heejun8609@delusionstudio.net'
    outer['From'] = 'vmurmurv@naver.com'
    outer.preamble = 'You will not see this in a MIME-aware mail reader.\n'

    filepath_list = ['./1. league_castle/1.1) ' + yester + '_catle_league_all.xlsx',
                     './1. league_castle/1.1) ' + yester + '_catle_league_day3_act.xlsx',
                     './1. league_castle/1.2) ' + yester + '_3day_league_castle_base.xlsx',
                     './2. wait_cp/2) ' + yester + '_wait_cpdiff.xlsx',
                     './3. match_ratio/3) ' + yester + '_match_ratio.xlsx',
                     './4. card_use/4) ' + yester + '_pick_win_ratio(hero, tower, spell).xlsx',
                     './5. witch_use/5) ' + yester + '_witch_pic_win_ratio.xlsx']

    text = '''
    1. 유저 League & Castle Lv 분포
    
    2. 전날 0 ~ 24시 동안의 랭크전 평균 매칭 대기 시간 & 평균 점수 차이
    
    3. 전날 0 ~ 24시 동안의 매칭 비율
    
    4. 전날 0 ~ 24시 동안의 랭크전 주요 카드 사용 현황((1) Crown Point 600점 이상 / (2) Castle Level 3 이상 / (3) 랭크전)
    
    5. 전날 0 ~ 24시 동안의 마법사 픽률 & 승률
    
    * 자세한 내용은 '원노트 기획 -> 박희준 -> 데이터 요청사항' 확인 요망
    https://onedrive.live.com/edit.aspx/%eb%ac%b8%ec%84%9c/%ec%ba%90%ec%8a%ac%eb%b2%88?cid=3d7703c304c2aa03&id=documents
    '''
    part = MIMEText(text, 'plain')
    outer.attach(part)

    for filename in filepath_list:

        path = os.path.join(filename)
        if not os.path.isfile(path):
            continue
        # Guess the content type based on the file's extension.  Encoding
        # will be ignored, although we should check for simple things like
        # gzip'd or compressed files.
        ctype, encoding = mimetypes.guess_type(path)
        if ctype is None or encoding is not None:
            # No guess could be made, or the file is encoded (compressed), so
            # use a generic bag-of-bits type.
            ctype = 'application/octet-stream'
        maintype, subtype = ctype.split('/', 1)
        if maintype == 'text':
            with open(path) as fp:
                # Note: we should handle calculating the charset
                msg = MIMEText(fp.read(), _subtype=subtype)
        elif maintype == 'image':
            with open(path, 'rb') as fp:
                msg = MIMEImage(fp.read(), _subtype=subtype)
        elif maintype == 'audio':
            with open(path, 'rb') as fp:
                msg = MIMEAudio(fp.read(), _subtype=subtype)
        else:
            with open(path, 'rb') as fp:
                msg = MIMEBase(maintype, subtype)
                msg.set_payload(fp.read())
            # Encode the payload using Base64
            encoders.encode_base64(msg)
        # Set the filename parameter
        date_reg_exp = re.compile(r'\d{4}[-/]\d{2}[-/]\d{2}')
        file_num = re.search(date_reg_exp,filename).span()
        msg.add_header('Content-Disposition', 'attachment', filename=filename[file_num[0]:])
        outer.attach(msg)

    with smtplib.SMTP_SSL('smtp.naver.com', 465) as server:
        server.ehlo()
        server.login('vmurmurv', 'quake541!')  # FIXME: 여러분의 네이버 계정명으로 변경해주세요.
        server.send_message(outer)

    print(yester + ' 이메일을 발송했습니다.')


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
        trigger = CronTrigger(day_of_week='mon-sun', hour='8', minute='25')
        self.sched.add_job(job, trigger)
        self.sched.start()


if __name__ == '__main__':
    scheduler = Scheduler()
    scheduler.scheduler()