from urllib import response
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response
from rest_framework.exceptions import AuthenticationFailed
# from login_api.serializers import UserSerializer, LoginSerializer
from datahub_v3_app.models import User
import jwt, datetime
from login_api.views import Login_View
from rest_framework import status


class Security_View(APIView):
    def post(self, request):
        jwt_new = request.data["authentication"]
        jwt_new1 = jwt.decode(jwt_new, 'secret', algorithms="HS256")
        use_id = jwt_new1.get('id')
        user = User.objects.get(id=use_id)
        if use_id == user.id:
          new = True
          payload = {
          'id': use_id,
          'status':new
          }
          token_ = jwt.encode(payload, 'secret', algorithm='HS256') #.decode('utf-8')
          return Response({
          'verify token': token_,
          'status':new
          })
        elif use_id != user.id:
          return Response(False)
        
        # var_jwt_new = request.data["authentication"]
        # var_jwt_new1 = jwt.decode(var_jwt_new, 'secret', algorithms="HS256")
        # var_use_id = var_jwt_new1.get('id')
        # var_user = User.objects.get(id=var_use_id)

        # if var_use_id == var_user.id:
        #   var_new = True
        #   payload = {
        #     'id': var_use_id,
        #     'status':var_new
        #   }
        #   var_token_ = jwt.encode(payload, 'secret', algorithm='HS256').decode('utf-8')
        #   return Response({
        #     'verifyToken': var_token_,
        #     'status':var_new
        #      })
        # elif var_use_id != var_user.id:
        #     return Response(False)      

        