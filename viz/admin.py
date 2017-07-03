from django.contrib import admin

# Register your models here.
from django.utils import timezone
import datetime
import pandas as pd
pd.to_datetime(timezone.now())
datetime.datetime.today()- datetime.timedelta(days=99999)
print(timezone.now() - datetime.timedelta(days=99999))