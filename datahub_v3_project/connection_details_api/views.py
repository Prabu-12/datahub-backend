from django.http.response import Http404
from datahub_v3_app.models import connection_detail, pipeline_schedule
from rest_framework.views import APIView
from connection_details_api.serializers import Connection_Details_Serializer
from rest_framework.response import Response



class Connectiton_Detail_View(APIView):
    def get_object(self, pk):
            try:
                return connection_detail.objects.get(pk=pk)
            except connection_detail.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
        var_pipeline_det_id= connection_detail.objects.filter(id=3).values('con_str')
        print(var_pipeline_det_id)
        # temp_pd_id=pipeline_det_id[0]
        if pk:
                data = self.get_object(pk)
                var_serializer = Connection_Details_Serializer(data)
                return Response([var_serializer.data])

        else:
                data = connection_detail.objects.all()
                var_serializer = Connection_Details_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        var_serializer = Connection_Details_Serializer(data=data)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': 'connect_detail Created Successfully',
            'data': var_serializer.data
        }
        return response

    def put(self, request, pk=None, format=None):
        var_update_conn_details = connection_detail.objects.get(pk=pk)
        var_serializer = Connection_Details_Serializer(instance=var_update_conn_details,data=request.data, partial=True)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'conect_detail Updated Successfully',
            'data': var_serializer.data
        }
        return response

    def delete(self, request, pk, format=None):
        var_delete_conn_details =  connection_detail.objects.get(pk=pk)
        var_delete_conn_details.delete()
        return Response({
            'message': 'connect_detail Deleted Successfully'
        })