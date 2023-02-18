from django.http.response import Http404
from datahub_v3_app.models import *
from rest_framework.views import APIView
from rest_framework.response import Response
from monitordata_api.serializers import Schedule_Log_Serializer

class Monitor_View(APIView):
    def get_object(self, pk):
            try:
                return schedule_log.objects.get(pk=pk)
            except schedule_log.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = Schedule_Log_Serializer(data)
                return Response([var_serializer.data])

            else:
                data = schedule_log.objects.all()
                var_serializer = Schedule_Log_Serializer(data, many=True)

                return Response(var_serializer.data)