#!/usr/bin/env python3
import os
import time
import http.client
import boto3
import logging
import json
import ast
import pandas as pd
import datetime as dt
from datetime import date, timedelta

from pandas_datareader import data as pdr
os.environ['AWS_SHARED_CREDENTIALS_FILE']='./cred' 
signal = 'Buy'
minhistory = 101
shots = 1000
resources = 2
def ec2_resource(signal, minhistory, shots):
    #var95 = []
    #var99 = []
    #ec2_out_list = []
    split1 = []
    dt_obj = []
    date = []
    risk95 = []
    risk99 = []
    ec2_dns_list = []
    ec2_out_list = []
    
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

#Included a 30 second wait as occassionally the instances were not loading   
    time.sleep(30)
            
    for i in ec2_dns_list:
        try:
            #print("start\n")
            host = i
            c = http.client.HTTPConnection(host)
            c.request("GET", "/cgitest.py?"+signal+"&"+str(minhistory)+"&"+str(shots))
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
            print(output)
        except IOError:
            print( 'Failed to open', host)
        
    

def ec2_terminate():
    print("Terminate")
    ec2 = boto3.resource('ec2', region_name='us-east-1')
    ec2_id = [instance.id for instance in ec2.instances.all()]
    #print(ec2_id)
    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    for i in instances:
        ec2.instances.filter(InstanceIds=ec2_id).terminate()
     
        

if __name__ == '__main__':
    #ec2_resource(signal, minhistory, shots)
    ec2_terminate()


