from dataclasses import fields
from rest_framework import serializers
from datahub_v3_app.models import *
class Member_Serializer(serializers.ModelSerializer):
    class Meta:
        model= member
        fields = '__all__'


class Team_Serializer(serializers.ModelSerializer):
    class Meta:
        model= teams_api_model
        fields = '__all__'