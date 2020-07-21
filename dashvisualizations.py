#from pysparkanalytics import *
import psycopg2 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import dash
import datetime
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from numpy import random
import pandas as pd
import random
import time
app = dash.Dash()

# initiate dataframe
#df = pd.DataFrame(columns=['time', 'cats'])

username='postgres' 
password='123'

con = psycopg2.connect(database='ecommerce',user=username,password=password);
cursor = con.cursor()
queryD="SELECT count(*) from ecommerce where extract(year from time)='2019' GROUP BY site_version;"
cursor.execute(queryD)
DM = cursor.fetchall();
con.commit()

trace1= go.Bar(x=['accessories','clothes','electronics','sneakers','sports_nutrition'], y=[0,0,0,0,0], name='Order')
trace2= go.Bar(x=['accessories','clothes','electronics','sneakers','sports_nutrition'], y=[0,0,0,0,0], name='Banner_Click')
trace3= go.Bar(x=['accessories','clothes','electronics','sneakers','sports_nutrition'], y=[0,0,0,0,0], name='Banner_Show')
df = pd.DataFrame(columns=['time', 'cats'])
app.layout = html.Div(html.Div([
    html.H1(children='E-commerce Site Dashboard'),
    html.Div(children='''Real Time Updates Every Minute'''),
    dcc.Graph(
        id='graphid',
        #figure={
            #'data': [go.Bar(x=['clothes'], y=ccount, name='Clothes'), go.Bar(x=['accessories'], y=acount, name='Accessories'),go.Bar(x=['electronics'], y=ecount, name='Electronics'),
            #go.Bar(x=['sneakers'], y=scount, name='Sneakers'),go.Bar(x=['sports_nutrition'], y=sncount, name='Sports Nutrition')],
            #'layout': go.Layout(title='Live Order Status by Customers wrt Time in seconds', barmode='stack')
        #})
        figure={
            'data': [trace1,trace2,trace3],
            'layout':
            go.Layout(title="Today's Live Order Status by Customers", barmode='group')
        }),
    dcc.Graph(
        id='graphtwo', figure={
            'data': [
                go.Scatter(x=['Jan,2019','Feb,2019','March,2019','April,2019','May,2019','July,2020'], y=[], mode = 'lines+markers')
            ],
            'layout': {
                'title': 'Total Orders from E-commerce Site for over time (Real-Time Updates)'
            }
        }
    ),
    dcc.Interval(
        id='1-second-interval',
        interval=100000, # 2000 milliseconds = 2 seconds
        n_intervals=0
    ),
    dcc.Graph(
        id='pie', figure={
            'data': [
                go.Pie(labels=['Desktop','Mobile'], values=[*DM[0],*DM[1]])
            ],
            'layout': {
                'title': 'Usage Desktop vs Mobile 2019'
            }
        }
    )
])
)

def compute():
    now = datetime.datetime.now()
    date=now.strftime("%m/%d/%Y")
    selectbs = "select product,count(*) from ecommerce where title='banner_show' and date='"+date+"' GROUP BY product;"
    cursor.execute(selectbs)
    rowsbs = cursor.fetchall();
    con.commit()

    selectbc = "select product,count(*) from ecommerce where title='banner_click' and date='"+date+"' GROUP BY product;"
    cursor.execute(selectbc)
    rowsbc = cursor.fetchall();
    con.commit()

    selecto = "select product,count(*) from ecommerce where title='order' and date='"+date+"' GROUP BY product;"
    cursor.execute(selecto)
    rowso = cursor.fetchall();
    con.commit()

    print(rowsbs,rowsbc,rowso)
    bs=[0,0,0,0,0]
    bc=[0,0,0,0,0]
    o=[0,0,0,0,0]
    for i in rowsbs:
        if i[0]=='accessories':
            bs[0]=i[1]
        elif i[0]=='clothes':
            bs[1]=i[1]
        elif i[0]=='electronics':
            bs[2]=i[1]
        elif i[0]=='sneakers':
            bs[3]=i[1]
        elif i[0]=='sports_nutrition':
            bs[4]=i[1]
    for j in rowsbc:
        if j[0]=='accessories':
            bc[0]=j[1]
        elif j[0]=='clothes':
            bc[1]=j[1]
        elif j[0]=='electronics':
            bc[2]=j[1]
        elif j[0]=='sneakers':
            bc[3]=j[1]
        elif j[0]=='sports_nutrition':
            bc[4]=j[1]
    for k in rowso:
        if k[0]=='accessories':
            o[0]=k[1]
        elif k[0]=='clothes':
            o[1]=k[1]
        elif k[0]=='electronics':
            o[2]=k[1]
        elif k[0]=='sneakers':
            o[3]=k[1]
        elif k[0]=='sports_nutrition':
            o[4]=k[1]
        
    return bs,bc,o



@app.callback(Output('graphid', 'figure'),
              [Input('1-second-interval', 'n_intervals')])
def update_layout(n):
    start=time.time()
    bs,bc,o=compute()
    end=time.time()
    print("f",end-start,bs,bc,o)
    
    trace1= go.Bar(x=['accessories','clothes','electronics','sneakers','sports_nutrition'], y=bs, name='Banner_Show') #[clothesbs,abs,sbs,snbs,ebs]
    trace2= go.Bar(x=['accessories','clothes','electronics','sneakers','sports_nutrition'], y=bc, name='Banner_Click')
    trace3= go.Bar(x=['accessories','clothes','electronics','sneakers','sports_nutrition'], y=o, name='Order')
    figure={
            'data': [trace1,trace2,trace3],
            'layout':
            go.Layout(title="Today's Live Order Status by Customers", barmode='group')
        }

    return figure
   
@app.callback(Output('graphtwo', 'figure'),
            [Input('1-second-interval', 'n_intervals')])
def update_graphtwo(n):
    start=time.time()
    time.sleep(5)
    monthlynineteen="SELECT count(*) from ecommerce where title='order' and extract(year from time)='2019' GROUP BY  extract(month from time);"
    cursor.execute(monthlynineteen)
    rowsn = cursor.fetchall();
    time.sleep(5)
    con.commit()
    
    monthlytweenty="SELECT count(*) from ecommerce where title='order' and extract(year from time)='2020' GROUP BY  extract(month from time);"
    cursor.execute(monthlytweenty)
    rowst = cursor.fetchall();
    con.commit()
    nineteen=[]
    tweety=[]
    for i in rowsn:
        nineteen.append(*i)
    for j in rowst:
        tweety.append(*j)
    time.sleep(5)
    

    total=nineteen+tweety
    end=time.time()
    print("s",end-start,rowsn,rowst,total)
    #clothes
    monthlynineteenc="SELECT count(*) from ecommerce where title='order' and extract(year from time)='2019' and product='clothes' GROUP BY  extract(month from time);"
    cursor.execute(monthlynineteenc)
    rowsnc = cursor.fetchall();
    time.sleep(5)
    con.commit()
    
    monthlytweentyc="SELECT count(*) from ecommerce where title='order' and extract(year from time)='2020' and product='clothes' GROUP BY  extract(month from time);"
    cursor.execute(monthlytweentyc)
    rowstc = cursor.fetchall();
    con.commit()
    nineteenc=[]
    tweetyc=[]
    for i in rowsnc:
        nineteenc.append(*i)
    for j in rowstc:
        tweetyc.append(*j)
    time.sleep(5)
    
   
    totalc=nineteenc+tweetyc
    #accessories
    monthlynineteena="SELECT count(*) from ecommerce where title='order' and extract(year from time)='2019' and product='accessories' GROUP BY  extract(month from time);"
    cursor.execute(monthlynineteena)
    rowsna = cursor.fetchall();
    time.sleep(5)
    con.commit()
    
    monthlytweentya="SELECT count(*) from ecommerce where title='order' and extract(year from time)='2020' and product='accessories' GROUP BY  extract(month from time);"
    cursor.execute(monthlytweentya)
    rowsta = cursor.fetchall();
    con.commit()
    nineteena=[]
    tweetya=[]
    for i in rowsna:
        nineteena.append(*i)
    for j in rowsta:
        tweetya.append(*j)
    time.sleep(5)
    
   
    totala=nineteena+tweetya

    #sneakers
    monthlynineteens="SELECT count(*) from ecommerce where title='order' and extract(year from time)='2019' and product='sneakers' GROUP BY  extract(month from time);"
    cursor.execute(monthlynineteens)
    rowsns = cursor.fetchall();
    time.sleep(5)
    con.commit()
    
    monthlytweentys="SELECT count(*) from ecommerce where title='order' and extract(year from time)='2020' and product='sneakers' GROUP BY  extract(month from time);"
    cursor.execute(monthlytweentys)
    rowsts = cursor.fetchall();
    con.commit()
    nineteens=[]
    tweetys=[]
    for i in rowsns:
        nineteens.append(*i)
    for j in rowsts:
        tweetys.append(*j)
    time.sleep(5)
    
   
    totals=nineteens+tweetys
    #sports_nutition
    monthlynineteensn="SELECT count(*) from ecommerce where title='order' and extract(year from time)='2019' and product='sports_nutrition' GROUP BY  extract(month from time);"
    cursor.execute(monthlynineteensn)
    rowssn = cursor.fetchall();
    time.sleep(5)
    con.commit()
    
    monthlytweentysn="SELECT count(*) from ecommerce where title='order' and extract(year from time)='2020' and product='sports_nutrition' GROUP BY  extract(month from time);"
    cursor.execute(monthlytweentysn)
    rowstsn = cursor.fetchall();
    con.commit()
    nineteensn=[]
    tweetysn=[]
    for i in rowssn:
        nineteensn.append(*i)
    for j in rowstsn:
        tweetysn.append(*j)
    time.sleep(5)
    
   
    totalsn=nineteenc+tweetysn
    #electronics has no orders in 2019
    monthlytweentye="SELECT count(*) from ecommerce where title='order' and extract(year from time)='2020' and product='electronics' GROUP BY  extract(month from time);"
    cursor.execute(monthlytweentye)
    rowste = cursor.fetchall();

    trace1=go.Scatter(x=['Jan,2019','Feb,2019','March,2019','April,2019','May,2019','July,2020'], y=total, mode = 'lines+markers',name='Total Orders')
    trace2=go.Scatter(x=['Jan,2019','Feb,2019','March,2019','April,2019','May,2019','July,2020'], y=totalc, mode = 'lines+markers',name='Total Orders-Clothes')
    trace3=go.Scatter(x=['Jan,2019','Feb,2019','March,2019','April,2019','May,2019','July,2020'], y=totala, mode = 'lines+markers',name='Total Orders-Accessories')
    trace4=go.Scatter(x=['Jan,2019','Feb,2019','March,2019','April,2019','May,2019','July,2020'], y=totals, mode = 'lines+markers',name='Total Orders-sneakers')
    trace5=go.Scatter(x=['Jan,2019','Feb,2019','March,2019','April,2019','May,2019','July,2020'], y=totalsn, mode = 'lines+markers',name='Total Orders-sports Nutrition')
    trace6=go.Scatter(x=['Jan,2019','Feb,2019','March,2019','April,2019','May,2019','July,2020'], y=[0,0,0,0,0,*rowste[0]], mode = 'lines+markers',name='Total Orders-Electronics')
    figure={
            'data': [trace1,trace2,trace3,trace4,trace5,trace6],
            'layout': {
                'title': 'Total Orders from E-commerce Site for over time (Real-Time Updates)'
            }
        }
    return figure






if __name__ == '__main__':
    
    app.run_server(debug=True)
