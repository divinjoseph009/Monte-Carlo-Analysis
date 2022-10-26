#!/usr/bin/env python3
import time
import http.client
from concurrent.futures import ThreadPoolExecutor
import os
import logging
import json
import boto3
from flask import Flask, request, render_template
import math
import statistics
import random
import ast
import yfinance as yf
import pandas as pd
from datetime import date, timedelta
from pandas_datareader import data as pdr
os.environ['AWS_SHARED_CREDENTIALS_FILE']='./cred' 

app = Flask(__name__)


#def warmup():

def yfinance_data(): #returns the entire yfinance data with Buy and Sell Signals
    #print("\nInside yfinance_data() function")
    
    # override yfinance with pandas – seems to be a common step
    yf.pdr_override()

    # Get stock data from Yahoo Finance – here, asking for about 10 years of Gamestop
    # which had an interesting time in 2021: https://en.wikipedia.org/wiki/GameStop_short_squeeze 
    today = date.today()
    decadeAgo = today - timedelta(days=3652)

    data = pdr.get_data_yahoo('BP.L', start=decadeAgo, end=today)
    # Other symbols: TSLA – Tesla, AMZN – Amazon, NFLX – Netflix, BP.L – BP 


    # Add two columns to this to allow for Buy and Sell signals
    # fill with zero
    data['Buy']=0
    data['Sell']=0
    data= data.rename_axis('Date').reset_index()
    # Find the 4 different types of signals – uncomment print statements
    # if you want to look at the data these pick out in some another way
    for i in range(len(data)): 
        # Hammer
        realbody=math.fabs(data.Open[i]-data.Close[i])
        bodyprojection=0.3*math.fabs(data.Close[i]-data.Open[i])

        if data.High[i] >= data.Close[i] and data.High[i]-bodyprojection <= data.Close[i] and data.Close[i] > data.Open[i] and data.Open[i] > data.Low[i] and data.Open[i]-data.Low[i] > realbody:
            data.at[data.index[i], 'Buy'] = 1
            #print("H", data.Open[i], data.High[i], data.Low[i], data.Close[i])   

    # Inverted Hammer
        if data.High[i] > data.Close[i] and data.High[i]-data.Close[i] > realbody and data.Close[i] > data.Open[i] and data.Open[i] >= data.Low[i] and data.Open[i] <= data.Low[i]+bodyprojection:
            data.at[data.index[i], 'Buy'] = 1
            #print("I", data.Open[i], data.High[i], data.Low[i], data.Close[i])

    # Hanging Man
        if data.High[i] >= data.Open[i] and data.High[i]-bodyprojection <= data.Open[i] and data.Open[i] > data.Close[i] and data.Close[i] > data.Low[i] and data.Close[i]-data.Low[i] > realbody:
            data.at[data.index[i], 'Sell'] = 1
            #print("M", data.Open[i], data.High[i], data.Low[i], data.Close[i])

    # Shooting Star
        if data.High[i] > data.Open[i] and data.High[i]-data.Open[i] > realbody and data.Open[i] > data.Close[i] and data.Close[i] >= data.Low[i] and data.Close[i] <= data.Low[i]+bodyprojection:
            data.at[data.index[i], 'Sell'] = 1
        #print("S", data.Open[i], data.High[i], data.Low[i], data.Close[i])
    
    #print(data)    
    return(data)


#When resource is Lambda
def resource_lambda(signal, resources, minhistory, shots, data):
    var95 = []
    var99 = []
    date_list = []
    risk95 = []
    risk99 = []
    #print("\n Inside resource_lambda()")
    
    def getpage(id):
        if signal == 'Buy': # 0 - Only Buy signals
            #print('Buy')
            for i in range(minhistory, len(data)):
                #print("Inside")
                if data.Buy[i]==1: # if we were only interested in Buy signals
                    date_list.append(data.Date[i])
                    mean=data.Close[i-minhistory:i].pct_change(1).mean()
                    #print(mean)
                    std=data.Close[i-minhistory:i].pct_change(1).std()
                    try:
                        c = http.client.HTTPSConnection("lnf5htk87j.execute-api.us-east-1.amazonaws.com")
                        lambda_input = '{"shots": "'+shots+'", "mean": "'+str(mean)+'", "std": "'+str(std)+'"}'
                        c.request("POST", "/default/COMM034_CW", lambda_input)
                        response = c.getresponse()
                        lambda_out = response.read().decode('utf-8')
                        lambda_outDict = json.loads(lambda_out)
                        var95.append(lambda_outDict['var95'])
                        var99.append(lambda_outDict['var99'])
                    except IOError:
                        print( 'Failed to open', host)
                        
            
        elif signal == 'Sell': # 0 - Only Sell signals
            for i in range(minhistory, len(data)): 
                if data.Sell[i]==1: # if we were only interested in Buy signals
                    date_list.append(data.Date[i])
                    mean=data.Close[i-minhistory:i].pct_change(1).mean()
                    std=data.Close[i-minhistory:i].pct_change(1).std()
                    try:
                        c = http.client.HTTPSConnection("lnf5htk87j.execute-api.us-east-1.amazonaws.com")
                        lambda_input = '{"shots": '+str(shots)+', "mean": '+str(mean)+', "std": '+str(std)+'}'
                        c.request("POST", "/default/COMM034_CW", lambda_input)
                        response = c.getresponse()
                        lambda_out = response.read().decode('utf-8')
                        lambda_outDict = json.loads(lambda_out)
                        var95.append(lambda_outDict['var95'])
                        var99.append(lambda_outDict['var99'])
                        
                    except IOError:
                        print( 'Failed to open', host)
    
        else:
            print("yfinance() Not working")
                      
        TPE_output = {'date':date_list, 'var95': var95, 'var99': var99}    
        #print(TPE_output)
        #print(len(date_list))
        #print(len(var95))
        #print(len(var99))
        return TPE_output
        
    runs=[value for value in range(resources)]
    def getpages():
        with ThreadPoolExecutor() as executor:
            results = executor.map(getpage, runs)
        return results
   

    start_time = time.time()
    result = getpages()
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    result = list(result)
    date_signal = result[0]['date']
    number_TPE = len(result) #no. of parallel executions
    #print(number_TPE)
    len_signal = len(result[0]['var95']) #no. of buy or sell signals
    #print(len_signal)
    
    #Calculates average of var95 and var99 of each buy or sell signal across each parallel execution
    for i in range(len_signal): #iterates over each buy or sell signal 
        temp1 = 0 
        temp2 = 0
        for j in range(number_TPE): #iterates over each parallel execution
            temp1 += result[j]['var95'][i]
            temp2 += result[j]['var99'][i]
        avg_var95 = abs(temp1/len_signal)*100 #average of var95 of each buy or sell signal over parallel executions
        #print(avg_var95)
        avg_var99 = abs(temp2/len_signal)*100 #average of var99 of each buy or sell signal over parallel executions
        #print(avg_var99)
        risk95.append(avg_var95)
        risk99.append(avg_var99)
  
        
    #Dataset with date of each buy or sell signal, average 95% and 99% risk values of each signal over each parallel execution
    df_avg_var = pd.DataFrame({'Date': date_signal, '95% Risk Value': risk95, '99% Risk value': risk99})
    #print(df_avg_var)
    
    #Calculate max and min risk values at 95% and 99%
    risk_max = max(max(risk95), max(risk99))
    risk_min = min(min(risk95), min(risk99))
    #print(risk_max) 
    #print(risk_min)
    
    #Final two values of 95% and 99% risk values
    risk95_final = statistics.mean(risk95)
    risk99_final = statistics.mean(risk99)    
    #print(risk95_final)
    #print(risk99_final)
     
    
    df_audit = pd.DataFrame({'Signal': signal, 'Resources': resources, 'Min. History': minhistory, 'Data Points': shots, 'Time taken': elapsed_time, 'Average of 95% Risk': risk95_final, 'Average of 99% Risk': risk99_final}, index=[0])
    print(df_audit)
     
    return date_signal, df_avg_var, df_audit, risk_max, risk_min, risk95_final, risk99_final, elapsed_time
    
#When resource is EC2
def ec2_resource(resources, signal, minhistory, shots):
    ec2_dns_list = []
    
    # Above line needs to be here before boto3 to ensure cred file is read from the right place
    ec2 = boto3.resource('ec2', region_name='us-east-1')

    instances = ec2.create_instances(
        ImageId = 'ami-07da39629b5f7b6a5', # Ubuntu 22.04 AMI
        MinCount = 1, 
        MaxCount = resources, 
        InstanceType = 't2.micro', 
        KeyName = 'us-east-1kp', # Make sure you have the named us-east-1kp
        SecurityGroups=['SSH'], # Make sure you have the named SSH
        )
        
# Wait for AWS to report instance(s) ready. 
    for i in instances:
        i.wait_until_running()
        # Reload the instance attributes
        i.load()
        print(i.public_dns_name) # ec2 com address
        ec2_dns_list.append(i.public_dns_name)
        #print(ec2_address_list)

#Included a 20 second wait as occassionally the instances were not loading   
    time.sleep(20)
            
    for i in ec2_dns_list:
        try:
            #print("start\n")
            host = i
            c = http.client.HTTPConnection(host)
            c.request("GET", "/cgitest.py?"+signal+"&"+str(minhistory)+"&"+shots)
            response = c.getresponse()
            ec2_out = response.read().decode('utf-8')
            #print(ec2_out)
            output = ec2_out.split('#')
            #print(type(output[1]))
            date_signal = output[1]
            var95 = ast.literal_eval(output[2])
            #print(var95)
            var99 = ast.literal_eval(output[3])
            #print(var99)
            output = {'date': date_signal, 'var95': var95, 'var99': var99}
            #print(output)
        except IOError:
            print( 'Failed to open', host)
        
    df_avg_var = pd.DataFrame({'95% Risk Value': var95, '99% Risk value': var99})
    risk95_final = statistics.mean(var95)
    risk99_final = statistics.mean(var99)
    risk_max = max(max(var95), max(var99))
    risk_min = min(min(var95), min(var99))
    df_audit = pd.DataFrame({'Signal': signal, 'Resources': resources, 'Min. History': minhistory, 'Data Points': shots, 'Average of 95% Risk': risk95_final, 'Average of 99% Risk': risk99_final}, index=[0])
     
    print(df_audit )
    return df_avg_var, df_audit, risk_max, risk_min, risk95_final, risk99_final

@app.route('/home', methods=['POST'])
def InputHandler():
    if request.method == 'POST':        
        service = request.form.get('service')
        signal = request.form.get('signal')
        minhistory = int(request.form.get('minhistory'))
        shots = request.form.get('shots')
        resources = int(request.form.get('resources'))
        print(service, signal, minhistory, shots, resources)
        if(service == 'Lambda'):
            data = yfinance_data()
            date_signal, df_avg_var, df_audit, risk_max, risk_min, risk95_final, risk99_final, time_elapsed = resource_lambda(signal, resources, minhistory, shots, data)

            return doRender('output.htm', {'table': df_avg_var.to_html(classes='data'), 'table1': df_audit.to_html(classes='data'), 'time_elapsed': time_elapsed, 'risk95_final': risk95_final, 'risk99_final': risk99_final, 'risk_max': risk_max, 'risk_min': risk_min})
            
         
        if(service == 'EC2'):
            start = time.time()
            df_avg_var, df_audit, risk_max, risk_min, risk95_final, risk99_final = ec2_resource(resources, signal, minhistory, shots)
            end = time.time()
            elapsed_time = end - start
            print(elapsed_time)
            return doRender('output.htm', {'table': df_avg_var.to_html(classes='data'), 'table1': df_audit.to_html(classes='data'), 'time_elapsed': elapsed_time, 'risk95_final': risk95_final, 'risk99_final': risk99_final, 'risk_max': risk_max, 'risk_min': risk_min})
 
#Code recycled from https://stackoverflow.com/questions/38122563/filter-instances-by-state-with-boto3                       
@app.route('/terminate', methods=['POST'])
def ec2_terminate():
    print("Terminate")
    ec2 = boto3.resource('ec2', region_name='us-east-1')
    ec2_id = [instance.id for instance in ec2.instances.all()]
    #print(ec2_id)
    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    for i in instances:
        ec2.instances.filter(InstanceIds=ec2_id).terminate()
    
    return doRender('index.htm', {'note': 'Terminated all EC2 Instances'})

def doRender(tname, values={}):
	if not os.path.isfile( os.path.join(os.getcwd(), 'templates/'+tname) ): #No such file
		return render_template('index.htm')
	return render_template(tname, **values)

# catch all other page requests - doRender checks if a page is available (shows it) or not (index)
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def mainPage(path):
	return doRender(path)

@app.errorhandler(500)
# A small bit of error handling
def server_error(e):
    logging.exception('ERROR!')
    return """
    An  error occurred: <pre>{}</pre>
    """.format(e), 500 


if __name__ == '__main__':
    # Entry point for running on the local machine
    # On GAE, endpoints (e.g. /) would be called.
    # Called as: gunicorn -b :$PORT index:app,
    # host is localhost; port is 8080; this file is index (.py)
    app.run(host='127.0.0.1', port=8080, debug=True)
    
    
        
    
    
