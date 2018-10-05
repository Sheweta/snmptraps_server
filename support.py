import logging
import ipaddress
import json

# Initialize the logger
def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)


def validate_host(host):
    try:
        if host == 'localhost':
            return True
        version=ipaddress.ip_address(host).version
        if version not in [4,6]:
            print(host, "does not appear to be an IPv4 or IPv6 address")
            return False
    except Exception as e:
        print(e)
        return False
    return True
 
def validate_port(port):
    try:
        if (str(port).isdigit() == False) :
            print(port, " is not a valid number.")
            return False
        else:
            port=int(port)
            return True
    except Exception as e:
        print ("Error in validate port",e)
        return False
    return True

def validate_number(value,start=None,end=None):
    try:
        if (str(value).isdigit() == False) :
            print(value, " is not a valid number.")
            return False
        else:
            val=int(value)
            if start is not None:
            	if val < start:
            		print(value, "is less than the start value: ",start)
            		return False
            if end is not None:
            	if val > end :
            		print(value, "is greater than the end value: ",end)
            		return False

            return True
    except Exception as e:
        print ("Error in validate number",e)
        return False
    return True
