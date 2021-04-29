#_*_coding:utf-8_*_
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from smartlibrary import settings
import uuid
import os
from django.shortcuts import render,HttpResponse
from home.models import book,hits
def importBookData(request):
    if request.method=='POST':
        file=request.FILES.get('file',None)
        if not file:
            return HttpResponse('None File uploads !')
        else:
            name=file.name
            handle_upload_file(name,file)
            return HttpResponse('success')
    return render(request,'utils/upload.html')
def handle_upload_file(name,file):
    path=os.path.join(settings.BASE_DIR,'uploads')
    fileName=path+'/'+name
    print(fileName)
    with open(fileName,'wb') as destination:
        for chunk in file.chunks():
            destination.write(chunk)
    insertToSQL(fileName)
def insertToSQL(fileName):
    txtfile=open(fileName,'r')
    id=""
    for line in txtfile.readlines():
        try:
            bookinfo = line.split(',')
            id = bookinfo[0].decode().encode('utf-8')
            name = bookinfo[1].decode().encode('utf-8')
            rating = bookinfo[2].decode().encode('utf-8')
            price = bookinfo[3].decode().encode('utf-8')
            publish = bookinfo[4].decode().encode('utf-8')
            url = bookinfo[5].decode().encode('utf-8')
            try:
                bk_entry=book(no=id,name=name,price=price,url=url,publish=publish,rating=rating)
                bk_entry.save()
            except Exception,e:
                print('save error'+ id + e.message)
        except Exception,e:
            print('read error '+ id + e.message)


