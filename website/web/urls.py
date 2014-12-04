from django.conf.urls import patterns, include, url
from . import views
urlpatterns = patterns('',url(r'^$', views.get_name),
        url(r'^reco/(?P<userId>\S+)$', views.show_reco)
        )
