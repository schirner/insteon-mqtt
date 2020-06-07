#===========================================================================
#
# MQTT utilities
#
#===========================================================================
from .. import on_off
import json


def parse_on_off(data, have_mode=True):
    """Parse on/off JSON data from an input message payload.

    The on/off flag is controlled by the data['cmd'] attribute which must be
    'on' or 'off'.

    The on/off mode is NORMAL by default.  It can be set by the optional
    field data['mode'] which can be 'normal', 'fast', or 'instant'.  Or it
    can be set by the boolean fields data['fast'] or data['instant'].

    Args:
      data (dict):  The message payload converted to a JSON dictionary.
      have_mode (bool):  If True, mode parsing is supported.  If False,
                the returned mode will always be None.

    Returns:
      (bool is_on, on_off.Mode): Returns a boolean to indicate on/off and the
         requested on/off mode enumeration to use.  If have_mode is False,
         then only the is_on flag is returned.
    """
    # Parse the on/off command input.
    cmd = data.get('cmd').lower()
    if cmd == 'on':
        is_on = True
    elif cmd == 'off':
        is_on = False
    else:
        raise Exception("Invalid on/off command input '%s'" % cmd)

    if not have_mode:
        return is_on

    # If mode is present, use that to specify normal/fast/instant.
    # Otherwise look for individual keywords.
    if 'mode' in data:
        mode = on_off.Mode(data.get('mode', 'normal').lower())
    else:
        mode = on_off.Mode.NORMAL
        if data.get('fast', False):
            mode = on_off.Mode.FAST
        elif data.get('instant', False):
            mode = on_off.Mode.INSTANT

    return is_on, mode

def announce_entity_device(link, discover_topic, ha_class, mqttObj, payload, suffix):
    """Creates the common portion of the discovery payload"""
    # NOTE this should go into the MQTT baseclass for devices. However, 
    # since we don't have one, this is the next best place to implement
    # common functionality

    # generate common part of HA MQTT discovery 
    # see: https://www.home-assistant.io/docs/mqtt/discovery/
    
    # use name (as defined by user with capitals) in HA if defined,
    # address otherwise
    name = mqttObj.device.name_caps if mqttObj.device.name_caps else mqttObj.device.addr.hex

    # construct the topic name 
    # each entity needs to have an own unique object_id hence use name + suffix
    topic = "{b}/{c}/{n}/config".format(c=ha_class, b=discover_topic,n=name+suffix)

    # device part of the payload is common and for the overall device (not the individual entity)
    payload['name'] = name + suffix 
    payload['device'] =  {
            'name' : name, # this is the basename without any suffix
            'manufacturer' : 'Insteon', 
            'identifiers'  : mqttObj.device.addr.hex,
        }
    # use the address to create unique id as it is unique
    payload['unique_id'] = 'inst_' + mqttObj.device.addr.hex + suffix

    # conditionally add info from the DB if present 
    if mqttObj.device.db.firmware:
        payload['device']['sw_version'] = mqttObj.device.db.firmware
    if mqttObj.device.db.desc.model and mqttObj.device.db.desc.description:
        payload['device']['model'] = mqttObj.device.db.desc.model + ": " + mqttObj.device.db.desc.description

    # send it off via mqtt 
    link.publish(topic,json.dumps(payload))

    return 
#===========================================================================
