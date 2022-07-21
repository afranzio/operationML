from tkinter.ttk import Separator
from click import option
from django.shortcuts import render, redirect, HttpResponseRedirect
from django.urls import reverse
import pandas as pd

# Create your views here.
def home(request):
    df = pd.read_csv ('D:\\CodeCode\\Afranzio-Sunday\\vivekML\\stocks\\BRTI.csv', index_col=False)
    option_list = []
    for column in df.columns:
        if column != 'Date' and 'Unnamed:' not in column:
            separator_space = column.replace('_', ' ')
            option_title = separator_space.capitalize()
            option_list.append(option_title)
    return render(request, 'basic/home.html', {'optionList': option_list})