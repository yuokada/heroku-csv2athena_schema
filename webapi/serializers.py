from rest_framework import serializers

from webapi.models import AnalyzerObject, AnalyzerResultObject, SERDE_CHOICES, STORED_CHOICES, PointObject


class AnalyzeObjectSerializer(serializers.Serializer):
    serde_choices = SERDE_CHOICES
    stored_choices = STORED_CHOICES

    # Fields
    schema = serializers.CharField(required=True, max_length=100)
    data_location = serializers.CharField(required=True, max_length=128)

    serde = serializers.ChoiceField(serde_choices)
    serde_properties = serializers.ListSerializer(child=serializers.CharField(), required=False)
    # serde_properties = serializers.DictField(child=serializers.CharField())

    stored = serializers.ChoiceField(stored_choices)

    table_properties = serializers.ListSerializer(child=serializers.CharField(), required=False)
    # table_properties = serializers.DictField(child=serializers.CharField())
    csv_file = serializers.CharField(allow_blank=True, required=True)

    def update(self, instance, validated_data):
        instance.schema = validated_data.get('schema', instance.schema)
        instance.table = validated_data.get('table', instance.table)
        instance.data_location = validated_data.get('data_location', instance.data_location)
        instance.serde = validated_data.get('serde', instance.serde)
        instance.serde_properties = validated_data.get('serde_properties', instance.serde_properties)
        instance.stored = validated_data.get('stored', instance.stored)
        instance.table_properties = validated_data.get('table_properties', instance.table_properties)
        instance.csv_file = validated_data.get('csv_file', instance.csv_file)
        instance.create_table = validated_data.get('create_table', instance.create_table)
        return instance

    def create(self, validated_data):
        return AnalyzerObject(**validated_data)


class AnalyzerResultObjectSerializer(serializers.Serializer):
    ddl = serializers.CharField(required=True)

    def create(self, validated_data):
        return AnalyzerResultObject(**validated_data)

    def update(self, instance, validated_data):
        return AnalyzerResultObject(
            ddl=validated_data.get('ddl', instance.ddl),
        )


class PointSerializer(serializers.Serializer):
    METHOD_CHOICES = ('GET', 'POST')
    point = serializers.IntegerField()
    method = serializers.ChoiceField(default=METHOD_CHOICES[0], choices=METHOD_CHOICES)

    def update(self, instance, validated_data):
        # NOTE: This is frozen object
        return PointObject(
            point=validated_data.get('point', instance.point),
            method=validated_data.get('method', instance.method)
        )

    def create(self, validated_data):
        return PointObject(**validated_data)
