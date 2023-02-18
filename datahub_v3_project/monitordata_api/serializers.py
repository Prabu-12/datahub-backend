from rest_framework import serializers
from datahub_v3_app.models import *


class Schedule_Log_Serializer(serializers.ModelSerializer):

    class Meta:
        model= schedule_log
        fields = '__all__'