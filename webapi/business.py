from django.template import loader

from core.csv2athena import convert_fields
from webapi.models import AnalyzerObject


class DDLBuilder(object):
    def __init__(self, guess_data, parameters: AnalyzerObject) -> None:
        self.guess_data = guess_data
        self.params = parameters

    def build(self) -> str:
        # 1. csv -> Athena field type
        fields = convert_fields(self.guess_data, self.params.serde)

        # 2. table_name
        if self.params.schema is not None:
            table_name = self.params.schema + "." + self.params.table
        else:
            table_name = self.params.table

        arguments = dict(
            schema=table_name,
            table_fields=fields,
            serde=self.params.serde,
            # serde_properties=post_form['serde_properties'].data,
            serde_properties=self.params.serde_properties,
            location=self.params.data_location,
            stored_as=self.params.stored,
            # table_properties=post_form['table_properties'].data,
            table_properties=self.params.table_properties,
        )
        return loader.render_to_string('webapi/ct.sql', arguments)
