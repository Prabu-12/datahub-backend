from django.http.response import Http404
from datahub_v3_app.models import schema_migration
from rest_framework.views import APIView
from schema_api.serializers import Schema_Serializer
from rest_framework.response import Response
from schema_framework.views import *

class Schema_View(APIView):
    def get_object(self, pk):
            try:
                return schema_migration.objects.get(pk=pk)
            except schema_migration.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = Schema_Serializer(data)
                return Response(var_serializer.data)
            else:
                data = schema_migration.objects.all()
                var_serializer = Schema_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        serializer = Schema_Serializer(data=data)

        serializer.is_valid(raise_exception=True)

        pass_set=serializer.save()
        set = {"id":pass_set.id}
        response = Response()

        response.data = {
            'message': 'connect_detail Created Successfully',
            'data': set
        }
        return response


    def put(self, request, pk=None, format=None):
        var_update_scheme = schema_migration.objects.get(pk=pk)
        var_serializer = Schema_Serializer(instance=var_update_scheme,data=request.data, partial=True)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': 'Schema Updated Successfully',
            'data': var_serializer.data
        }

        return response
    def delete(self, request, pk, format=None):
        var_delete_schema =  schema_migration.objects.get(pk=pk)

        var_delete_schema.delete()

        return Response({
            'message': 'Schema Deleted Successfully'
        })
    
class soft(APIView):

    def get(self,request,pk):
        # import pdb 
        # pdb.set_trace()
        print(pk)
        schema_fram(schema_id=pk)
        return Response("Done")