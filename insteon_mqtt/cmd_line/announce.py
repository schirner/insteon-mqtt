#===========================================================================
#
# Announce commands
#
#===========================================================================
from . import util

#===========================================================================
def sendRequest(args, config):
    """common function for announce request since they only differ in command
       string"""

    payload = {
        "cmd" : args.mode,
        }

    reply = util.send(config, args.announce_topic, payload, args.quiet)
    return reply["status"]


#===========================================================================
