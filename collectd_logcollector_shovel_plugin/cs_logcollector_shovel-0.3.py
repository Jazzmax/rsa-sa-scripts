#!/usr/bin/env python
#------------------------------------------------------------------------------
# Name:        cs_logcollector_shovel.py
# Purpose:     Collectd module for SA to generate LC Shovel Status stats out of a rabbimq rest api
#
# Author:      Maxim Siyazov
# Version:     0.3
# Created:     22/02/2016
# Copyright:   (c) msiyazov 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import re
import nwutils
#import cs_common
import copy
import restclient
#import imp
import types
#import urllib
import operator
import json


#restclient = imp.load_source('restclient', '/usr/lib/collectd/python/restclient.py')

shovels = {}
dest_group = ""
shovel_state = ""
#logger = StdOutLogger()

operations = {
    '+': operator.add,
    '-': operator.sub,
    '*': operator.mul,
    '/': operator.div
}

class StdOutLogger :

    def __init__(self) :
        "ok"

    def logError(self, message) :
        print "Error: {}".format(message)

    def logWarning(self, message) :
        print "Warning: {}".format(message)

    def logNotice(self, message) :
        print "Notice: {}".format(message)

    def logInfo(self, message) :
        print "Info: {}".format(message)

    def logDebug(self, message) :
        print "Debug: {}".format(message)

def getRequiredStatKeys() :
    return [
        "^logcollector_logcollection.vmware.workmanager/gauge-workgroup_num_enabled$",
        "^logcollector_logcollection.sdee.workmanager/gauge-workgroup_num_enabled$",
        "^logcollector_logcollection.cmdscript.workmanager/gauge-workgroup_num_enabled$",
        "^logcollector_logcollection.windows.workmanager/gauge-workgroup_num_enabled$"
    ]


def genCompositeStats(logger, stats) :
    global shovels
    ret = []
    shovel_prim_dest = []

    # Generate Shovel primary destination (single group)
    # TODO Process multiple destination groups
    shovel_prim_dest = get_shovel_conf()

    for dest_group in shovel_prim_dest:
        data = {
            'plugin': 'messagebus_localhost_logcollector',
            #'plugin': 'messagebus_localhost_shovel',
            'type': 'string',
            'type_instance': 'shovel_prim_dest' + '.' + dest_group['name'],
            'values': [0]
        }
        data['meta'] = {}
        data['meta']['name'] = "Shovel Primary Destination"
        data['meta']['description'] = "IP Address of Primary Event Broker Shovel destination"
        data['meta']['category'] = 'MessageBus'
        data['meta']['string_value'] = get_primary_ip(dest_group['address'])
        data['meta']['subitem'] = dest_group['name']
        ret.append(data)

    # Generate Shovel Active Destinations
    get_active_dest()

    for key, value in shovels.iteritems() :
        # Active Destination
        data = {
            'plugin': 'messagebus_localhost_logcollector',
            #'plugin': 'messagebus_localhost_shovel',
            'type': 'string',
            'type_instance': 'shovel_active_dest'+'.'+key,
            'values': [0]
        }
        data['meta'] = {}
        data['meta']['name'] = "Shovel Active Destination"
        data['meta']['description'] = "IP Address of Active Event Broker Shovel destination"
        data['meta']['category'] = 'MessageBus'
        data['meta']['string_value'] = value['active_dest']
        data['meta']['subitem'] = key
        ret.append(data)

         # Shovel Failed over to secondary destination
        data = {
            'plugin': 'messagebus_localhost_logcollector',
            #'plugin': 'messagebus_localhost_shovel',
            'type': 'string',
            'type_instance': 'shovel_failedover'+'.'+key,
            'values': [0]
        }
        data['meta'] = {}
        data['meta']['name'] = "Shovel Failover Status"
        data['meta']['description'] = "True if a shovel failed over to a secondary destination"
        data['meta']['category'] = 'MessageBus'

        for existing_meta in ret:
            if 'shovel_prim_dest.'+key in existing_meta.values():
                prim_dest = existing_meta['meta']['string_value']
        if value['active_dest'] == prim_dest:
            data['meta']['string_value'] = 'False'
        else:
            data['meta']['string_value'] = 'True'
        data['meta']['subitem'] = key
        ret.append(data)

        # Group state
        data = {
            'plugin': 'messagebus_localhost_logcollector',
            #'plugin': 'messagebus_localhost_shovel',
            'type': 'string',
            'type_instance': 'shovel_group_state'+'.'+key,
            'values': [0]
        }
        data['meta'] = {}
        data['meta']['name'] = "Shovel Destination Group State"
        data['meta']['description'] = "State of the Shovel Destination Group"
        data['meta']['category'] = 'MessageBus'
        data['meta']['string_value'] = value['dest_state']
        data['meta']['subitem'] = key
        ret.append(data)

        # Group active since
        data = {
            'plugin': 'messagebus_localhost_logcollector',
            #'plugin': 'messagebus_localhost_shovel',
            'type': 'string',
            'type_instance': 'shovel_active_since'+'.'+key,
            'values': [0]
        }
        data['meta'] = {}
        data['meta']['name'] = "Shovel Active Destination Since"
        data['meta']['description'] = "Time when the Shovel Active Destination host changed"
        data['meta']['category'] = 'MessageBus'
        data['meta']['string_value'] = value['active_since']
        data['meta']['subitem'] = key
        ret.append(data)




    return ret

'''
def get_primary_shovel_dest(address):
    # Get the primary destination out of a shovel config file
    #shovel_config = nwutils.readFile("/etc/rabbitmq/shovel_config")
    addrs_found = ""
    ret = []

    address = address.rstrip().lstrip(' ')
    x = re.search('^\{addresses,\[(.+?)\]\},', line)
    if x:
        addrs_found = x.group(1).replace('"', '')
        print ("Found addresses: %s" % addrs_found)
    else:
        print ("Destination address not found.")
    ret = addrs_found.split(",")[0]
    return ret
'''

def getRequest(node) :
    global logger
    if node['client'] == None :
        endpoint = copy.copy(get(node, 'endpoint'))
        endpoint['password'] = "********"
        #logger.logDebug("Creating REST client with config: %s ..." % endpoint)
        print ("Creating REST client with config: %s ..." % endpoint)
        node['client'] = restclient.RestClient(get(node, 'endpoint'))
        #logger.logDebug("Initialized REST client: %s" % node['client'])
        print("Initialized REST client: %s" % node['client'])
    return node['client'].getRequest()

def get_active_dest():
    global logger
    ret = []
    node = {}
    node['endpoint'] = {
        'scheme' :  "https",
        'port' : 15671,
        'verify' : False,
        'path' : 'api/nw/shovel',
        'username' : "guest",
        'password' : "guest"
    }
    node['client'] = None

    response = {}
    try :
        response = getRequest(node)
        #logger.logDebug("Got response: %s" % response)
        print("Got response: %s" % response)
    except Exception as e :
        client = None
        endpoint = copy.copy(get(node, 'endpoint'))
        endpoint['password'] = "********"
        #logger.logError("Unable to Connect to Endpoint.  Endpoint config: %s; error: %s" % (endpoint, e))
        print("Unable to Connect to Endpoint.  Endpoint config: %s; error: %s" % (endpoint, e))

    if response.status_code == 200 :
        json_string = response.json()
        #print json_string
        #the_dict = json.loads(json_string)
        #print json_string['state']['running']

        walk_dict(json_string)


    else :
        endpoint = copy.copy(get(node, 'endpoint'))
        endpoint['password'] = "********"
        #logger.logError("Bad response from Endpoint.  Response code: %d.  Endpoint config: %s" % (response.status_code, endpoint))
        print("Bad response from Endpoint.  Response code: %d.  Endpoint config: %s" % (response.status_code, endpoint))

    return ret

def get_shovel_conf():

    ret = []
    node = {}
    node['endpoint'] = {
        'scheme' :  "http",
        'port' : 50101,
        'verify' : False,
        'path' : 'event-broker/destinations?msg=get&force-content-type=application/json',
        'username' : "admin",
        'password' : "netwitness"
    }
    node['client'] = None

    response = {}
    try :
        response = getRequest(node)
        #logger.logDebug("Got response: %s" % response)
        print("Got response: %s" % response)
    except Exception as e :
        client = None
        endpoint = copy.copy(get(node, 'endpoint'))
        endpoint['password'] = "********"
        #logger.logError("Unable to Connect to Endpoint.  Endpoint config: %s; error: %s" % (endpoint, e))
        print("Unable to Connect to Endpoint.  Endpoint config: %s; error: %s" % (endpoint, e))

    if response.status_code == 200 :
        json_string = response.json()
        print json_string

        ret = json_string['params']

    else :
        endpoint = copy.copy(get(node, 'endpoint'))
        endpoint['password'] = "********"
        #logger.logError("Bad response from Endpoint.  Response code: %d.  Endpoint config: %s" % (response.status_code, endpoint))
        print("Bad response from Endpoint.  Response code: %d.  Endpoint config: %s" % (response.status_code, endpoint))

    return ret


def get(dictionary, key, default = None) :
    if not key in dictionary :
        if default == None :
            raise RuntimeError("No entry found for key %s in dictionary." % key)
        else :
            return default
    return dictionary[key]

# walk trhough a badly built dict and create a global shovels dict
# and returns the shovel state
def walk_dict(d,depth=0):
    global shovels
    global dest_group
    global shovel_state
    for k,v in sorted(d.items(),key=lambda x: x[0]):
        if isinstance(v, dict):
            #print ("  ")*depth + ("%s" % k)
            if depth == 1:
                #print ("shovel state=%s" % k)
                shovel_state=k
            if depth == 2:
                #print ("dest group=%s" % k)
                shovels[k]={}
                dest_group=k
            if depth == 4:
                #print ("dest state=%s" % k)
                shovels[dest_group]['dest_state']=k
            walk_dict(v,depth+1)
        else:
            #print ("  ")*depth + "%s %s" % (k, v)
            #print ("active destination=%s" % k)
            #print ("active since=%s" % v)
            shovels[dest_group]['active_dest']=k
            shovels[dest_group]['active_since']=v

    return shovel_state


def get_primary_ip(address):
    # Get the primary destination out of a shovel config file
    #shovel_config = nwutils.readFile("/etc/rabbitmq/shovel_config")
    prime_destination = ""
    ret = []

    if address:
        prime_destination = address.split()
        print ("Found addresses: %s" % prime_destination)
        ret = prime_destination[0]
    else:
        print ("Destination address not found.")

    return ret