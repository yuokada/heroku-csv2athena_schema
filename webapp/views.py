import tempfile

from django.shortcuts import render
from django.views.generic import FormView

import lib.csv2athena as csv2athena
from webapp.forms import UploadFileForm, form2obj


# see: https://docs.djangoproject.com/en/2.1/topics/auth/default/#the-loginrequired-mixin
# class IndexView(LoginRequiredMixin, TemplateView):
# login_url = '/login/'


class IndexView(FormView):
    template_name = 'webapp/index.html'

    def get_initial(self):
        return super().get_initial()

    def form_valid(self, form):
        return super().form_valid(form)

    # def get(self, request, *args, **kwargs):
    #     return super().get(request, *args, **kwargs)


def index_view(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        # see: http://y0m0r.hateblo.jp/entry/20120625/1340631834
        # table_properties = request.POST.getlist('table_properties')
        if form.is_valid():
            form.serde_properties = request.POST.getlist('serde_properties')
            form.table_properties = request.POST.getlist('table_properties')
            guess_result = handle_uploaded_file(request.FILES['csvfile'])
            ct = csv2athena.build_ct(guess_result, form2obj(form)).strip()
            return render(request, 'webapp/index.html',
                          {'form': form, 'ct': ct, 'table_properties': form.table_properties,
                           'serde_properties': form.serde_properties})
    else:
        form = UploadFileForm()
    return render(request, 'webapp/index.html', {'form': form})


def handle_uploaded_file(f):
    with tempfile.TemporaryFile(mode='ab+') as fp:
        for chunks in f.chunks():
            fp.write(chunks)
        fp.seek(0)
        return csv2athena._guess_csv_datatype(fp)
