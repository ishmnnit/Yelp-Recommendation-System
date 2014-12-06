from django.shortcuts import render
from models import NameForm
from pymongo import Connection
import collections

def get_name(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
    # create a form instance and populate it with data from the request:
        form = NameForm(request.POST)
    # check whether it's valid:
        if form.is_valid():
    # process the data in form.cleaned_data as required
            return HttpResponseRedirect('/thanks/')
    else:
        form = NameForm()

    return render(request, 'name.html', {'form': form})

def show_reco(request,userId, algorithm):
    server="localhost"
    port = 27017
    conn = Connection(server,port)
    recos = {}
    if algorithm == "lda":
        recos = conn.Topics.Reco.find_one({"userId":userId})

        topReco=[]
        i=0
        for r in recos["recoList"]:
            i = i+1
            if i > 10:
                break;
            print r
            s=""
            for p in r:
                s=p;
                break;
            print s
            businessInfo=conn.Dataset_Challenge_Reviews.Business.find_one({"business_id":s})
            catg=[]
            for c in businessInfo["categories"]:
                 catg.append(c.encode('UTF-8'))

            r.append(businessInfo["name"])
            r.append(catg)
            r.append(businessInfo["stars"])
            topReco.append(r)
        #print topReco
        return render(request,'reco.html',{'reco':topReco})
    else:
        recos = conn.Topics.UserGraph.find({"user_id":userId})
        topReco=[]
        i = 0
        for r in recos:
            i = i + 1
            if i > 10:
                break;
            businessInfo = conn.Dataset_Challenge_Reviews.Business.find_one({"business_id": r["business_id"]})
            catg=[]
            for c in businessInfo["categories"]:
                catg.append(c.encode('UTF-8'))
            r.update({"business_name": businessInfo["name"]})
            r.update({"category": catg})
            r.pop("_id", None)
            dict1 = collections.OrderedDict()
            dict1.update({"user_id":r["user_id"]})
            dict1.update({"score": r["score"]})
            dict1.update({"business_id": r["business_id"]})
            dict1.update({"business_name": r["business_name"]})
            dict1.update({"category": r["category"]})
            topReco.append(dict1)
        return render(request,'reco1.html',{'reco':topReco})
