from clickhouse_driver import connect,Client
import pandas as pd

first=1

# client = Client('10.8.0.42')
connect=connect('clickhouse://10.8.0.42')

cursor=connect.cursor()

with open('webNameForTesla.csv') as f:
    items=f.read().split('\n')
for item in items:
    print(item)

    query=f"SELECT `Domain` FROM neometa.whois where `Domain` like '{item.lower()}%com' or `Domain` like '{item.lower()}%net' or `Domain` like '{item.lower()}%io' or `Domain` like '{item.lower()}%org'"

    cursor.execute(query)
    data=cursor.fetchall()
    if data:
        for line in data:
            url=line[0]
            result={}
            result['name']=item
            result['url']=url
            df=pd.DataFrame([result])
            df.to_csv('whoischeckResult3_tesla.csv',mode='a',index=0,header=(first==0))
            first+=1


print('--------done-------')
