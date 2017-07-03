from django.db import models
import django_tables2 as tables

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

class Eng(models.Model):
    app = models.CharField(db_column='app', max_length=32, blank=True, null=True)
    version = models.CharField(db_column='version', max_length=32, blank=True, null=True)
    id = models.CharField(db_column='id', max_length=255, blank=True, null=True)
    title = models.CharField(db_column='title', max_length=255, blank=True, null=True)
    content = models.TextField(db_column='content', blank=True, null=True)
    date = models.DateField(db_column='date',null=False, primary_key=True)
    rating = models.IntegerField(db_column='rating', blank=True, null=True)
    lang = models.CharField(db_column='lang', max_length=32, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'app_en'
