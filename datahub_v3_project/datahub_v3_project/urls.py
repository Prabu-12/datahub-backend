"""datahub_v3_project URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
import imp
from login_api.views import Login_View
from register_api.views import RegisterView
from django.urls import path
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi
from connection_api.views import List_Connections_View, Create_Connections_View, Update_Connections_View, Delete_Connections_View
from schedule_dependency.views import Schedule_Dependency_View
from db_sql_extract.views import Sql_Extract_View
from pipeline_schedule_api.views import Pipeline_Schedule_View
from pipeline_api.views import Pipeline_View
from pipeline_details_api.views import Pipeline_Detail_View
from connection_details_api.views import Connectiton_Detail_View
from db_config_api.views import Db_Config_View
from role_api.views import Role_View
from role_detail_api.views import Role_detail_View
from profile_api.views import Profile_View
from user_role_api.views import User_Role_View
from user_api.views import User_Profile_View
from page_api.views import Page_View
from count_api.views import Count_View
from new_count_api.views import NewCount_View, NewCount_Detail_View, TotalCount_View
# from pipeline_framework.views import main_framwork
from monitordata_api.views import Monitor_View
from security_imple_api.views import Security_View
from datatype_api.views import Datatype_View
from schema_api.views import Schema_View
from utility_api.views import *
from schedule_api.views import Schdule_View
from team_member_api.views import Member_View,Team_View
from tenant_register.views import Tenant_Register_View
from sql_generator.views import SQL_Generator_View
from pre_audit.views import preauditdb
from schema_api.views import *
schema_view = get_schema_view(
   openapi.Info(
      title="Datahub",
      default_version='v1',
      description="Make migrations like brezes",
      terms_of_service="https://www.google.com/policies/terms/",
      contact=openapi.Contact(email="contact@snippets.local"),
      license=openapi.License(name="BSD License"),
   ),
   public=True,
   permission_classes=[permissions.AllowAny],
)

urlpatterns = [
   
   path('register/', RegisterView.as_view(), name='register'),
   path('login/', Login_View.as_view(), name='login'),
   path('', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
   path('redoc/', schema_view.with_ui('redoc', cache_timeout=0), name='schema-redoc'),
   path('getconnection/',List_Connections_View.as_view(),name='ListConnectionsAPIView'),
   path('getconnection/<int:pk>/', List_Connections_View.as_view(), name='ListConnectionsAPIView'),
   path('postconnection/',Create_Connections_View.as_view(),name='CreateConnectionsAPIView'),
   path('putconnection/',Update_Connections_View.as_view(),name='UpdateConnectionsAPIView'),
   path('putconnection/<int:pk>/', Update_Connections_View.as_view(), name='UpdateConnectionsAPIView'),
   path('deleteconnection/<int:pk>/',Delete_Connections_View.as_view(),name='DeleteConnectionsAPIView'),
   path('schedule_dep/', Schedule_Dependency_View.as_view()),
   path('schedule_dep/<int:pk>', Schedule_Dependency_View.as_view()),
   path('sql_extract_api/', Sql_Extract_View.as_view()),
   path('sql_extract_api/<str:pk>', Sql_Extract_View.as_view()),
   path('pipe_sc/',Pipeline_Schedule_View.as_view()),
   path('pipe_sc/<int:pk>',Pipeline_Schedule_View.as_view()),
   path('pipeline/', Pipeline_View.as_view()),
   path('pipeline/<str:pk>', Pipeline_View.as_view()),
   path('pipeline_det/', Pipeline_Detail_View.as_view()),
   path('pipeline_det/<str:pk>',Pipeline_Detail_View.as_view()),
   path('connection_det/',Connectiton_Detail_View.as_view()),
   path('connection_det/<int:pk>',Connectiton_Detail_View.as_view()),
   path('db_config/',Db_Config_View.as_view()),
   path('db_config/<int:pk>',Db_Config_View.as_view()),
   path('role/', Role_View.as_view()), #--> slash need to be addded
   path('role/<int:pk>', Role_View.as_view()),
   path('role_detail/', Role_detail_View.as_view()), #--> slash need to be addded
   path('role_detail/<int:pk>', Role_detail_View.as_view()),
   path('profile',Profile_View.as_view()), # slash should be removed
   path('profile/<int:pk>', Profile_View.as_view()),
   path('user_role/',User_Role_View.as_view()),
   path('user_role/<int:pk>',User_Role_View.as_view()),
   path('user_api/',User_Profile_View.as_view()),
   path('user_api/<str:pk>',User_Profile_View.as_view()), # slash should be removed
   path('pages/',Page_View.as_view()),
   path('pages/<int:pk>',Page_View.as_view()),
   path('count/',Count_View.as_view()),
   path('new_count/',NewCount_View.as_view()),
   path('new_detail/',NewCount_Detail_View.as_view()),
   path('total_count/',TotalCount_View.as_view()),
   path('monitor/', Monitor_View.as_view()),
   path('monitor/<int:pk>', Monitor_View.as_view()),
   path('security/',Security_View.as_view()),
   path('datatype/',Datatype_View.as_view()),
   path('datatype/<int:pk>', Datatype_View.as_view()),
   path('schema/',Schema_View.as_view()),
   path('schema/<str:pk>',Schema_View.as_view()),
   path('audit/', Audit.as_view()),
   path('auditsf/', AuditSnow.as_view()),
   path('auditsql/', AuditSql.as_view()),
   path('schedule/', Schdule_View.as_view()),
   path('schedule/<int:pk>',Schdule_View.as_view()),
   path('team/',Team_View.as_view()),
   path('team/<int:pk>',Team_View.as_view()),
   path('member/',Member_View.as_view()),
   path('member/<int:pk>',Member_View.as_view()),
   path('tenantregister/',Tenant_Register_View.as_view(), name='tenantregister'),
   path('sqlgenerator/', SQL_Generator_View.as_view()),
   path('sqlgenerator/<str:pk>',SQL_Generator_View.as_view()),
   path('preauditdatabase/',preauditdb.as_view()),
   path('preauditdatabase/<int:pk>',preauditdb.as_view()),
   path('schema_trigger/<int:pk>',soft.as_view()),
   ]
