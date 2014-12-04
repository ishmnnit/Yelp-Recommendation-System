from django.db import models
from django import forms


# Create your models here.
class NameForm(forms.Form):
    your_name = forms.CharField(label='Your name', max_length=100)
