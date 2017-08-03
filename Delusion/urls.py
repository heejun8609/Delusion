"""Delusion URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.10/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url, include
from django.contrib import admin
from viz import views

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^$', views.index, name='index'),  # main index view 연결
    url(r'^simple_list/?', views.simple_list, name='simple_list'),
    url(r'^review_star_trend/?', views.review_star_trend, name='review_star_trend'),
    url(r'^card_trend/?', views.card_trend, name='card_trend'),
    url(r'^issue_trend/?', views.issue_trend, name='issue_trend'),
    url(r'^word_2_vec/?', views.word_2_vec, name='word_2_vec'),
    url(r'^search/?', views.simple_list, name='search'),

]