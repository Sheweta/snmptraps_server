# This file creates a always learning process to receive bytes data
# It will receive data from host port using udp and pass it to a module
# It assumes that the server and port is open and the process has 
# permission to receive data.No handshake happens 
# The process expects host port and data as positional arguements
# Sample command : python3 udpreceiveserver.py 0.0.0.0 0 module_name parameter_list
# For more information run python3 udpreceiveserver.py -h
#

import socket
import sys
import json
import argparse
import _thread
import logging
import datetime
import redis
import process_message
from process_message import processtrapmsg, convertmsgforkafka

import support
from support import *

import kafka
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Initialize the socket object 
def initialize(Network,Protocol,host,port):
    err_flag=0

    if (Network is None):
       print("Network is not defined")
       err_flag=1
    
    if (Protocol is None):
       print("Protocol is not defined")
       err_flag=1     
   

    if (host is None):
       print("host is not defined")
       err_flag=1     
   

    if (port is None):
       print("port is not defined")
       err_flag=1     
   
    if (err_flag == 1):
       sys.exit(1)
   
    try:
        sock = socket.socket(Network, Protocol)
        
    except socket.error as e:
        print("Error while creating socket.",e)
        err_flag=1
    except Exception as e:
        print("Exception raised: ",e)
        err_flag=1

    #Bind to socket
    try:
        sock.bind((host,port))
    except socket.gaierror as e:
        print("Address-related error connecting to server: ", e)
        err_flag=1
    except socket.error as e:
        print("Connection error: ",  e)
        err_flag=1

    if err_flag == 1:
        sys.exit(1) 
    return sock   


def validate_category_in_redis(category,topiclist,redisconnect,default_topic):
    try:
        print(topiclist)
        print("category =",category,"next")
        topics=redisconnect.lrange(category,0,-1)
        print("topics is ",topics)
        if topics is None or topics == []:
            return (False,default_topic)
        if set(topics).issubset(set(topiclist)):
            return (True,topics)
        else:
            return (False,default_topic)
    except Exception as e:
        print("Exception in getting topic",e)
        return (False,default_topic)


def process_msg(message,loghandle,recvedfrom,recvedon,redisconnect,topics,bootstrap_server,default_topic):
    """
    Save Received date and fron and process message into readableform
    """
    
    try:
        print("thread started")
        recvDict={}
        recvDict["recvedfrom"] =recvedfrom[0] +" "+str(recvedfrom[1])
        recvDict["recvedon"]  = recvedon.strftime("%Y-%m-%d %H:%M:%S")
        msgDict=processtrapmsg(message)
        recvDict.update(msgDict)
        jsonmsg=json.dumps(recvDict)
        updatedmsg,category=convertmsgforkafka(jsonmsg)
        print(category,len(category))
        loghandle.info(updatedmsg)
        status,outtopic=validate_category_in_redis(category,topics,redisconnect,default_topic)
        print(status)
        try:
            #print(bootstrap_server)
            kafkaproducer = KafkaProducer(bootstrap_servers=[bootstrap_server])
            connected = True
            
            try:
                if status == False:
                    topiclist=outtopic
        
                    kafkaproducer.send(topiclist,str(updatedmsg).encode("utf-8"))
                    kafkaproducer.flush()
                    print ("message sent")
                else:
                    topiclist=[topic.decode("utf-8") for topic in outtopic ]
                    print("message",topiclist) 
                    for t in topiclist:
                        kafkaproducer.send(t,str(updatedmsg).encode("utf-8"))
                        kafkaproducer.flush()
            except Exception as e:
                print("Error while sending the message",updatedmsg," to topic ",e)
        except Exception as e:
            print ("Kafka Broker not running. Check for Kafka service ",e)
            _thread.exit_thread()  
    except Exception as e:
        print (e , "in thread")
    print("exit thread")
    _thread.exit_thread()


# Parse the arguements received 
# Initialize the socket
# Recieve data to server port
# Pass it to module_name

def main():
    
    parser = argparse.ArgumentParser(description="Recieves data through udp port and pass it to kafka server with topics logic in redis server")
    parser.add_argument("socket_host",  help="Host of the socket server")
    parser.add_argument("socket_port", type=int, help="Port of the socket server")
    parser.add_argument("broker_host", help="Host of broker (Zookeeper)")
    parser.add_argument("broker_port", help="Port of broker (Zookeeper)")
    parser.add_argument("default_topic",help="Default topic where all messages are to be sent.")
    parser.add_argument("redis_host", help="Host of redis server")
    parser.add_argument("redis_port", help ="Port of redis server.")
    parser.add_argument("redis_dbno", help ="Db number (0-15) of redis server.")
     
    args = parser.parse_args()
      
    Network=socket.AF_INET
    Protocol=socket.SOCK_DGRAM
    if args.socket_host and args.socket_port  and args.broker_host and args.broker_port and args.redis_host and args.redis_port and args.redis_dbno:

        if validate_host(args.socket_host):
            host=args.socket_host
        
        if validate_port(args.socket_port):
            port=args.socket_port
        
        if validate_host(args.broker_host):
            broker_host=args.broker_host

        if validate_host(args.redis_host):
            redis_host=args.redis_host
           
        if validate_port(args.broker_port):
            broker_port=args.broker_port

        if validate_port(args.redis_port):
            redis_port=args.redis_port

        if  validate_number(args.redis_dbno,0,15):
            redis_dbno =args.redis_dbno

        if str(args.default_topic):
            default_topic=str(args.default_topic)
        bootstrap_server=broker_host+":"+broker_port
        print(bootstrap_server)
    setup_logger('msglog', '/var/log/snmptraps/'+datetime.datetime.now().strftime("%Y%m%d%H%M%S") +'incomingtraps.log')
    loghandle = logging.getLogger('msglog')
    
    socket_obj=initialize(Network,Protocol,host,port)
    

    try:
        redisconnect = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_dbno)
        try:
            topics=redisconnect.lrange('snmptraps',0,-1)
            
        except Exception as e:
            print("Entry for topics of snmptraps is missing ",e)

    except redis.ConnectionError:
        print ("Could not connect to Redis server")

    
    ctr=0
    print(ctr,datetime.datetime.now())
    while True:
              
            ctr=ctr+1
            if ctr%1000 == 0:
                print(ctr,datetime.datetime.now())
            # Receive from  socket
            try:
                record,addr =socket_obj.recvfrom(9000)
                now=datetime.datetime.now()
                 
                try:
                    print("message received")
                    _thread.start_new_thread( process_msg, (record,loghandle,addr,now,redisconnect,topics,bootstrap_server,default_topic) )
                except:
                    print ("Error: unable to start thread")
                    exit(3)
                
            except socket.error as e:
                print("Error receiving data:", e)
                sys.exit(1)
    
        
       
        
if __name__ == "__main__":
    main()
 




"""
    try:
            redisconnect = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_dbno)
            try:
                 topics=redisconnect.lrange('snmptraps',0,-1)
            
            except Exception as e:
                print("Entry for topics of snmptraps is missing ",e)

        except redis.ConnectionError:
            print ("Could not connect to Redis server")

    sendtokafka(bootstrap_server,jsonmsg,redisconnect)

"""