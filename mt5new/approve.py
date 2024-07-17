import requests

id_list=[]
a=requests.get('http://localhost:8000/mt5/mt5-approve')
data=a.json()
for i in data['results']:
    id_list.append(i['fx_id'])
    


for i in id_list:
    new_data={
        'id':i,
        'selet':1
    }
    print(new_data)
    redata=requests.post('http://localhost:8000/mt5/mt5-select',json=new_data)
    print(redata)