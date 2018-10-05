import redis
import argparse
import csv
import support
from support import *

parser = argparse.ArgumentParser(description="Add/Update/Remove category topic pair to  redis server and synchronize the redis server from a file.")
parser.add_argument("redis_host", help="Host of redis server")
parser.add_argument("redis_port", type=int, help ="Port of redis server.")
parser.add_argument("redis_dbno", type=int,help ="Db number (0-15) of redis server.")
parser.add_argument("action",help ="Action to be performed on redis server(add, update or delete or synchronize .")
parser.add_argument("filename",help="File from where data is to be synchronized with full path.")
parser.add_argument("--key",help ="Alarm category on which action is to be performed .")
parser.add_argument("--value",help ="Topic on which action is to be performed .") 

args = parser.parse_args()

redis_host=None
redis_port=None
redis_dbno=None
dbdetails={}
filedetails={}
redisconnect =None

if  args.redis_host is not None and args.redis_port is not None  and args.redis_dbno is not None and args.action is not None :
    if validate_host(args.redis_host):
        redis_host=args.redis_host

    if validate_port(args.redis_port):
        redis_port=args.redis_port

    if  validate_number(args.redis_dbno,0,15):
        redis_dbno =args.redis_dbno

    try:
        if args.filename:
            fd=open(args.filename,"r+")
            reader=csv.reader(fd,delimiter="~",quotechar='"')
            for line in reader:
                filedetails[line[0]]=line[1].split(",")
                inputkeys=filedetails.keys()

            print(filedetails)
    except Exception as e:
        print("Filename from which data to be synch with redis  is required",e)
        exit(1) 

    if args.action == "synchronize":
        pass
    elif args.action in ["add","update","delete"]:
        try:
            if args.category:
                category=args.category
            
            if args.topic:
                topic=args.topic
            
            if args.filename:
                fdw=filename(args.filename,"a")
        except Exception as e:
            print("category ,topic and master file are mandatory for action add, upddate, delete")


try:
    redisconnect = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_dbno,decode_responses=True)
    try:
        keys =redisconnect.keys('*')
        for key in keys:
            keytype = redisconnect.type(key)
            dbdetails[key]=redisconnect.lrange(key,0,-1)
        print(dbdetails)
        dbkeys=dbdetails.keys()
    except Exception as e:
        print("Issue while recovering data from database ",e)

except redis.ConnectionError:
    print ("Could not connect to Redis server")

if args.action == 'synchronize':
    for key,valuelist in filedetails.items():
        update =False
        existing=[]
        print(key,valuelist)
        if key in dbdetails.keys():
            existing=dbdetails[key]
            print (existing,valuelist)
            if existing != valuelist:
                update= True
                redisconnect.delete(key)
        else:
            update=True
        if update == True:
            redisconnect.delete(key)
            for value in valuelist:
                print(key,value)
                if value not in existing:
                    redisconnect.rpush(key, value)
        
    diffkeys=list(set(list(dbkeys))-set(list(inputkeys)))
    for key in diffkeys:
        redisconnect.delete(key)
fdw=open(args.filename,"r+")

if args.action == 'add':
    if args.category in dbkeys:
        existing = dbdetails[args.category]
        if args.topic in existing:
            print(args.category, " already exists with topic ",args.topic)
    else:
        redisconnect.rpush(args.category,args.topic)
        if args.category in inputkeys:
            filedetails[args.category].append(topic)
        else:
            filedetails[args.category]=topic
        print(args.category," is added to redis with topic ",args.topic)

if args.action == 'update':
    if args.category in dbkeys:
        existing = dbdetails[args.category]
        if args.topic in existing:
            print(args.category, " already exists with topic ",args.topic)
        else:
            redisconnect.rpush(args.category,args.topic)
            if args.category in inputkeys:
                filedetails[args.category].append(topic)
            else:
                filedetails[args.category]=topic

            print(args.category, " updated with topic ",args.topic)
         
    else:
        print(args.category," doesnot exist  in redis")

if args.action == 'delete':
    if args.category in dbkeys:
        existing = dbdetails[args.category]
        if args.topic in existing:
            redisconnect.delete(args.category,args.topic)
            print(args.category," is deleted for topic ", args.topic)
            if args.category in inputkeys:
                filedetails[args.category].remove(topic)
            else:
                filedetails[args.category]=topic

        else:
            
            print(args.category, " doesnot exist with topic ",args.topic)
         
    else:
        print(args.category," doesnot exist  in redis")


print(redisconnect.keys('*'))
for key in redisconnect.keys('*'):
    print (key,redisconnect.lrange(key,0,-1))
fd.close()
fd.open(args.filename,"w")
