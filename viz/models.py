from django.db import models

class Raw(models.Model):
    app = models.CharField(db_column='app', max_length=32, blank=True, null=True)
    version = models.CharField(db_column='version', max_length=32, blank=True, null=True)
    id = models.CharField(db_column='id', max_length=255, blank=True, null=True)
    title = models.CharField(db_column='title', max_length=255, blank=True, null=True)
    content = models.TextField(db_column='content', blank=True, null=True)
    date = models.DateField(db_column='date', null=False, primary_key=True)
    rating = models.IntegerField(db_column='rating', blank=True, null=True)
    lang = models.CharField(db_column='lang', max_length=32, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'app'

class Word_count(models.Model):
    date_word = models.CharField(db_column='date_word', max_length=32, primary_key=True)
    date = models.DateField(db_column='date',null=False, primary_key=True)
    word = models.CharField(db_column='word', max_length=32, blank=True, null=True)
    total_count = models.IntegerField(db_column='total_count', blank=True, null=True)
    r1_count = models.IntegerField(db_column='r1_count', blank=True, null=True)
    r2_count = models.IntegerField(db_column='r2_count', blank=True, null=True)
    r3_count = models.IntegerField(db_column='r3_count', blank=True, null=True)
    r4_count = models.IntegerField(db_column='r4_count', blank=True, null=True)
    r5_count = models.IntegerField(db_column='r5_count', blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'app_word'

class Fantasy(models.Model):
    app = models.CharField(db_column='app', max_length=32, blank=True, null=True)
    id = models.CharField(db_column='id', max_length=255, blank=True, primary_key=True)
    date = models.DateField(db_column='date', null=False)
    title = models.CharField(db_column='title', max_length=255, blank=True, null=True)
    content = models.TextField(db_column='content', blank=True, null=True)
    rating = models.IntegerField(db_column='rating', blank=True, null=True)
    lang = models.CharField(db_column='lang', max_length=32, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'fantasy'

class Fantasy_count(models.Model):
    date = models.DateField(db_column='date', null=False, primary_key=True)
    word = models.CharField(db_column='word', max_length=32, blank=True, null=True)
    total_count = models.IntegerField(db_column='total_count', blank=True, null=True)
    r1_count = models.IntegerField(db_column='r1_count', blank=True, null=True)
    r2_count = models.IntegerField(db_column='r2_count', blank=True, null=True)
    r3_count = models.IntegerField(db_column='r3_count', blank=True, null=True)
    r4_count = models.IntegerField(db_column='r4_count', blank=True, null=True)
    r5_count = models.IntegerField(db_column='r5_count', blank=True, null=True)


    class Meta:
        managed = False
        db_table = 'fantasy_count'


class CastleBurn(models.Model):
    app = models.CharField(db_column='app', max_length=32, blank=True, null=True)
    id = models.CharField(db_column='id', max_length=255, blank=True, primary_key=True)
    date = models.DateField(db_column='date', null=False)
    title = models.CharField(db_column='title', max_length=255, blank=True, null=True)
    content = models.TextField(db_column='content', blank=True, null=True)
    rating = models.IntegerField(db_column='rating', blank=True, null=True)
    lang = models.CharField(db_column='lang', max_length=32, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'castleburn'

class CastleBurn_count(models.Model):
    date = models.DateField(db_column='date', null=False, primary_key=True)
    word = models.CharField(db_column='word', max_length=32, blank=True, null=True)
    total_count = models.IntegerField(db_column='total_count', blank=True, null=True)
    r1_count = models.IntegerField(db_column='r1_count', blank=True, null=True)
    r2_count = models.IntegerField(db_column='r2_count', blank=True, null=True)
    r3_count = models.IntegerField(db_column='r3_count', blank=True, null=True)
    r4_count = models.IntegerField(db_column='r4_count', blank=True, null=True)
    r5_count = models.IntegerField(db_column='r5_count', blank=True, null=True)


    class Meta:
        managed = False
        db_table = 'castleburn_count'