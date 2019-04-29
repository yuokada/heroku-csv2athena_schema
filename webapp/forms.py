from typing import Dict

from attr import dataclass
from django import forms


@dataclass
class FormArgument:
    csvfile: str
    schema: str
    location: str
    serde: str
    serde_properties: Dict[str, str]
    stored_as: str
    table_properties: Dict[str, str]


class UploadFileForm(forms.Form):
    csvfile = forms.FileField(required=True, label='your input csvfile')
    schema = forms.CharField(max_length=32, required=True)
    location = forms.CharField(max_length=50)
    serde = forms.CharField(max_length=128)
    stored_as = forms.CharField(empty_value=True)

    # serde_properties= forms.MultiValueField()
    # serde_properties = forms.MultipleHiddenInput()

    # table_properties = forms.MultiValueField('table_properties', require_all_fields=False)
    # table_properties = forms.CharField(required=False)

    # NOTE: Adhoc code
    table_properties = []
    serde_properties = []


def form2obj(frm: UploadFileForm) -> FormArgument:
    return FormArgument(
        schema=frm['schema'].data,
        serde=frm['serde'].data,
        location=frm['location'].data,
        csvfile=frm['csvfile'].data.name,
        serde_properties=dict(),
        stored_as=frm['stored_as'].data,
        table_properties=dict(),
    )
