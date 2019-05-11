from django.urls import path

from .views import *

urlpatterns = [
    path('analyzer', AnalyzerAPIViewSet.as_view(), name='analyzer'),
    path('point', PointAPIViewSet.as_view(), name='point'),
]
