#!/usr/bin/python3
from index import *
import os
import math
import random
import yfinance as yf
import pandas as pd
from datetime import date, timedelta
from pandas_datareader import data as pdr
from concurrent.futures import ThreadPoolExecutor
import time
import json
from flask import Flask, request, render_template

app = Flask(__name__)


def resource_lambda(resources, shots, mean_list, std_list):
    result = []
    data_lambda_out_list = []
    print("\n Inside resource_lambda()")
    runs=[value for value in range(resources)]
    
    def getpage(id):
        try:
            date_list, mean_list, std_list = yfinance_data(signal, minhistory)
            #print(mean_list)
            c = http.client.HTTPSConnection("lnf5htk87j.execute-api.us-east-1.amazonaws.com")
        
            for i in range(len(mean_list)):
                lambda_input = '{"shots": '+str(shots)+', "mean": '+str(mean_list[i])+', "std": '+str(std_list[i])+'}'
                #print(lambda_input)
                c.request("POST", "/default/COMM034_CW", lambda_input)
                response = c.getresponse()
                data_lambda_out = response.read().decode('utf-8')
                data_lambda_out_list.append(data_lambda_out)
                
        except IOError:
                print( 'Failed to open ', host ) # Is the Lambda address correct?        
                       
        print(data_lambda_out+" from "+str(id)) # May expose threads as completing in a different order
        return "page "+str(id)

    def getpages():
        with ThreadPoolExecutor() as executor:
            results=executor.map(getpage, runs)
        return results

    start_time = time.time()
    result = getpages()
    stop_time = time.time()
    elapsed_time = start_time - stop_time
    print( "Elapsed Time: ", elapsed_time)
    print(data_lambda_out_list)
    return data_lambda_out_list, elapsed_time
    
@app.route('/userinput', methods=['POST'])
def ec2_ProcessHandler():
    ProcessHandler()                     
    return doRender('index.htm',{'note':'Done'})

def doRender(tname, values={}):
	if not os.path.isfile( os.path.join(os.getcwd(), 'templates/'+tname) ): #No such file
		return render_template('index.htm')
	return render_template(tname, **values)

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
    #ec2_resource()
    app.run(host='127.0.0.1', port=8080, debug=True)
