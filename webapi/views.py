import tempfile

from django.http import HttpResponseBadRequest
from rest_framework import views
from rest_framework.exceptions import ValidationError
from rest_framework.parsers import JSONParser
from rest_framework.response import Response

from core import csv2athena
from webapi.business import DDLBuilder
from webapi.models import AnalyzerObject, PointObject
from webapi.serializers import AnalyzeObjectSerializer, AnalyzerResultObjectSerializer, PointSerializer


class AnalyzerAPIViewSet(views.APIView):
    parser_classes = (JSONParser,)

    def get(self, request, format=None):
        obj = AnalyzerObject("default_schema", data_location="s3://path/to/object/")
        serializer = AnalyzeObjectSerializer(obj)
        return Response(serializer.data)

    def post(self, request, format=None):
        serializer = AnalyzeObjectSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=HttpResponseBadRequest.status_code)

        guess_data = self._guess_csv_datatype(serializer.validated_data['csv_file'])
        builder = DDLBuilder(guess_data, serializer.create(serializer.validated_data))
        ddl = builder.build()

        res_serializer = AnalyzerResultObjectSerializer(data={"ddl": ddl})
        try:
            if res_serializer.is_valid():
                return Response(res_serializer.validated_data)
        except ValidationError as e:
            return Response(res_serializer.errors, status=HttpResponseBadRequest.status_code)

    @staticmethod
    def _guess_csv_datatype(f: str):
        with tempfile.TemporaryFile(mode='ab+') as fp:
            fp.write(f.encode())
            fp.seek(0)
            return csv2athena._guess_csv_datatype(fp)


class PointAPIViewSet(views.APIView):
    parser_classes = (JSONParser,)

    def get(self, request, format=None):
        point = int(request.query_params.get('point', 0))
        plusing = int(request.query_params.get('plusing', 0))
        obj = PointObject(point=point + plusing, plusing=plusing)

        serializer = PointSerializer(obj)
        return Response(serializer.data, status=200)

    def post(self, request, format=None):
        serializer = PointSerializer(data=request.data)
        if serializer.is_valid():
            return Response(serializer.validated_data)
        else:
            return Response(serializer.errors, status=HttpResponseBadRequest.status_code)
