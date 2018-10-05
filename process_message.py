"""
To convert snmptraps into readable pdus 

"""
from pysnmp.carrier.asynsock.dispatch import AsynsockDispatcher
from pysnmp.carrier.asynsock.dgram import udp, udp6
from pyasn1.codec.ber import decoder
from pysnmp.proto import api
from pysnmp.hlapi import *
import datetime
import json



start="-value="
end="\n\n\n"
start_len=len(start)

oidNamesict= {'1.3.6.1.2.1.1.3.0': "sysUpTimeInstance_epoch",
    '1.3.6.1.6.3.1.1.4.1.0': "snmpTrapOID",
    '1.3.6.1.4.1.9.9.311.1.1.2.1.1.0':'cenAlarmIndex',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.2.0':'cenAlarmVersion',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.3.0':'cenAlarmTimestamp',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.4.0':'cenAlarmUpdatedTimestamp',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.5.0':'cenAlarmInstanceID',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.6.0':'cenAlarmStatus',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.7.0':'cenAlarmStatusDefinition',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.8.0':'cenAlarmType',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.9.0':'cenAlarmCategory',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.10.0':'cenAlarmCategoryDefinition',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.11.0':'cenAlarmServerAddressType',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.12.0':'cenAlarmServerAddress',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.13.0':'cenAlarmManagedObjectClass',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.14.0':'cenAlarmManagedObjectAddressType',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.15.0':'cenAlarmManagedObjectAddress',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.16.0':'cenAlarmDescription',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.16.0':'cenAlarmDescription',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.17.0':'cenAlarmSeverity',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.18.0':'cenAlarmSeverityDefinition',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.19.0':'cenAlarmTriageValue',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.20.0':'cenEventIDList',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.21.0':'cenUserMessage1',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.22.0':'cenUserMessage2',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.23.0':'cenUserMessage3',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.24.0':'cenAlarmMode',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.25.0':'cenPartitionNumber',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.26.0':'cenPartitionName',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.27.0':'cenCustomerIdentification',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.28.0':'cenCustomerRevision',
    '1.3.6.1.4.1.9.9.311.1.1.2.1.29.0':'cenAlertID',
    "recvedon":"transaction_time",
    "recvedfrom":"traplogserver",
    "request-id":"transaction_id",
    "error-status": "error-status",
    "error-index":"error-index"
    }



def processtrapmsg(wholeMsg):
    """
    check the snmp version of the message 
    if supported version(version in [1,2,3]) process the message
    get version,community and data pdus
    get all the pdus (snmpV2-trap,requestid, errorstatus ,error index and varBindings)
    break varBindings into oid value pair
    """
    msgDict={} 

    try:
        
        msgVer = int(api.decodeMessageVersion(wholeMsg))
        
        if msgVer in api.protoModules:
            pMod = api.protoModules[msgVer]
            
        else:
            print('Unsupported SNMP version %s' % msgVer)
            return    
        reqMsg, wholeMsg = decoder.decode(
            wholeMsg, asn1Spec=pMod.Message(),
            )
        reqPDU = pMod.apiMessage.getPDU(reqMsg)
        
        for key,value in reqPDU.items():
            msgDict[str(key)] =str(value)
        msgDict.pop("variable-bindings")
        
        varBinds = pMod.apiTrapPDU.getVarBindList(reqPDU)
        bindDict={} 
        for x in varBinds:
            y=str(x[1])
            start_value=y.find(start)
            bindDict[str(x[0])]=y[start_value+start_len:y.find(end)]
        msgDict.update(bindDict)   
    except Exception as e:
        print(e)    
    return (msgDict)



def convertmsgforkafka(msg):    
    trapdict=json.loads(msg)
    msgDict={}
    for key,value in trapdict.items():
        msgDict[oidNamesict[key]]=value
        
    categorydefwhole=msgDict['cenAlarmCategoryDefinition']
    categorydef=categorydefwhole[categorydefwhole.rfind(",")+2:]   
                
    return (msgDict,categorydef)
