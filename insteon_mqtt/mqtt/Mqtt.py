#===========================================================================
#
# MQTT main interface
#
#===========================================================================
import functools
import json
import logging
from .. import log
from . import config
from .MsgTemplate import MsgTemplate
from .Reply import Reply

LOG = log.get_logger()


class Mqtt:
    """Main MQTT interface class.

    This class translates MQTT messages to Insteon commands and Insteon
    commands into MQTT messages.  Low level MQTT is handled by the
    network.Mqtt class which communicates data to this class via Signals.
    The main Insteon interface is the Modem which is used to find devices to
    send them commands.  The Modem is also used to notify us of new Insteon
    devices and this class connects their state change signals to ourselves
    so we can send messages out when the device state changes.

    This class subscribes to the Insteon command and set topics defined in
    the configuration input.  It will publish state changes on the Insteon
    state topic when the devices change state.

    The exact format of the output topics and payloads is controlled by the
    configuration file and the individual devices.  The input command topics
    and payloads are passed through a jinja template to convert them to a
    format that a device understands.  For message details, see the
    individual devices.

    This class also handles "system commands".  These are not Insteon
    specific states or updates but are commands that the Insteon-Mqtt system
    implements for various things.  The payload for these messages is always
    a json data object that will get passed to the Insteon device for
    handling
    """
    def __init__(self, mqtt_link, modem):
        """Constructor

        Args:
          mqtt_link (network.Mqtt):  The network MQTT link to use for
                    communicating with the MQTT broker.
          modem (mqtt.Modem):  The MQTT PLM modem object.
        """
        # Connect a callback for handling when a new device is created in the
        # modem.  We'll use it to create a corresponding MQTT device.
        self.modem = modem
        self.modem.signal_new_device.connect(self.handle_new_device)

        # Callback for when we're connected to the broker so we can subscribe
        # to the various topics we need to monitor.
        self.link = mqtt_link
        self.link.signal_connected.connect(self.handle_connected)

        # Map of Address ID to MQTT device.
        self.devices = {}

        # The command topic template (MstTemplate) to use.
        self._cmd_topic = None

        # MQTT message parameters.  These get loaded via the config.
        self.qos = 1
        self.retain = True

        # Loaded config object.
        self._config = None

    #-----------------------------------------------------------------------
    def load_config(self, data):
        """Load a configuration dictionary.

        This should be the mqtt key in the configuration data.  Key inputs
        are:

        The input configuration dictionary can contain:
        - broker:    (str) The broker host to connect to.
        - port:      (int) Thr broker port to connect to.
        - username:  (str) Optional user name to log in with.
        - passord:   (str) Optional password to log in with.

        - qos:         (int) QOS level to use for sent messages (Default 1).
        - retain:      (bool) Retain sent messages (Default True)
        - cmd_topic:   (str) The MQTT topic prefix to subscribe to for
                       system commands.
        - announce_topic:   (str) The MQTT topic prefix to subscribe to for
                       announce commands.

        Args:
          data (dict):  Configuration data to load.
        """
        # Pass connection data to the MQTT link.  This will configure the
        # connection to the broker.
        self.link.load_config(data)

        # Create a template for prcessing messages on the command topic.
        self._cmd_topic = MsgTemplate.clean_topic(data['cmd_topic'])

        # Create the template for discovery topic
        # default value is None (do not announce presence)
        self._discover_topic = data.get('discover_topic', None)
        if self._discover_topic: 
            self._discover_topic = MsgTemplate.clean_topic(self._discover_topic)

        # Create the template for discovery topic
        # default value is None (do not listen to announce requests)
        self._announce_topic = data.get('announce_topic', None)
        if self._announce_topic: 
            self._announce_topic = MsgTemplate.clean_topic(self._announce_topic)

        # template for device info 
        self._discovery_device_info = data.get('discovery_device_info', None)

        # MQTT message parameters.
        self.qos = data.get('qos', self.qos)
        self.retain = data.get('retain', self.retain)

        # Save the config for later passing to devices when they are created.
        self._config = data

        # Subscribe to the new topics.
        if self.link.connected:
            self._subscribe()


    def load_discovery_templates(self,data,discList):
        """Extracts the discovery templates from config and returns 
        them in a hash. Called by the devices.
        Args:
            data (dict):  sub tree of config data from config.yaml for this device
            discList (array): list of qualifiers to extract
        Returns: 
            dict <qualifier> = (<topic_template>,<payload_template>)
        """
        # start with emtpy template hash
        discHash = {}

        # for each discovery qualifier (string) 
        for qual in discList:
            # the main entry has an emtpy qualifier, for all 
            # others add trailing _
            qual_ext = qual
            if qual != "":
                qual_ext = qual + "_"
            topic_key   = "discovery_{q}topic".format(q=qual_ext)
            payload_key = "discovery_{q}payload".format(q=qual_ext)

            # only add if both topic and payload definition exists in config
            if topic_key in data and payload_key in data:
                # add tuple to has with (<topic template>, <payload template>)
                discHash[qual] = (data[topic_key],data[payload_key])

        return discHash


    #-----------------------------------------------------------------------
    def publish(self, topic, payload, qos=None, retain=None):
        """Publish a message out.

        Args:
          topic (str):  The MQTT topic to publish with.
          payload (str):  The MQTT payload to send.
          qos (int):  None to use the class QOS. Otherwise the QOS level
              to use.
          retain (bool):  None to use the class retain flag.  Otherwise
                 the retain flag to use.
        """
        qos = self.qos if qos is None else qos
        retain = self.retain if retain is None else retain

        # Pass the message to the network link.
        self.link.publish(topic, payload, qos, retain)

    #-----------------------------------------------------------------------
    def close(self):
        """Close the MQTT link.
        """
        self.link.close()

    #-----------------------------------------------------------------------
    def handle_connected(self, link, connected):
        """MQTT (dis)connection callback.

        This is called when the low level MQTT client connects to the broker.
        After the connection, we'll subscribe to our topics.

        Args:
          link (network.Mqtt):  The MQTT network link.
          connected (bool):  True if connected, False if disconnected.
        """
        if connected:
            self._subscribe()

            # make devices announce themselves for auto discovery
            self.announce()

    def announce(self):
        """Announce discovery message to Home Assistant"""
        
        if self._discover_topic:
            LOG.ui("Sending discovery announcements.")
            nOK = 0
            nNOK = 0

            # let each device announce itself (if its class has an announce method)
            for device in self.devices.values():
                # check if the device implements announce
                announce_op = getattr(device, "announce", None)
                if announce_op and callable(announce_op):
                    # call the device's announce
                    device.announce(self.link, self._discover_topic)
                    nOK = nOK + 1
                else:
                    # no need to do anything if announce is not implemented
                    # This is normal for e.g. the modem as it will not be announced to HA.
                    # just skip
                    nNOK = nNOK + 1

            LOG.ui("Send announcements for %d device (%d skipped since not implemented)", nOK, nNOK)
        else:
            # Send info since user might invoke on command line and expect output.
            LOG.ui("Discovery announcements disabled. discover_topic not defined in config.yaml.")

    def unannounce(self):
        """Announce empty discovery messages to Home Assistant"""
        
        if self._discover_topic:
            LOG.ui("Sending empty discovery announcements to delete.")
            nOK = 0
            nNOK = 0

            # let each device announce itself (if its class has an announce method)
            for device in self.devices.values():
                try:
                    device.unannounce(self.link, self._discover_topic)
                    nOK = nOK + 1
                except AttributeError:
                    # no need to do anything if announce is not implemented
                    # just skip
                    nNOK = nNOK + 1

            LOG.ui("Send empty announcements for %d devices (%d skipped since not implemented)", nOK, nNOK)
        else:
            # Send info since user might invoke on command line and expect output.
            LOG.ui("Discovery announcements disabled. discover_topic not defined in config.yaml.")

    def announce_entity_device_template(self, device, discList, discovery_templates, data):
        """Announce each entity in discList based on discovery_templates and data"""
        # This is called from each device with config to announce

        # Home Assiaten MQTT discovery 
        # see: https://www.home-assistant.io/docs/mqtt/discovery/
        
        # augment the template data with common definitions 
        #   (unless overwritten by inputs even though I would not know why we'd need it)

        # set base for announcement topic 
        if not 'discovery_topic_base' in data:
            data['discovery_topic_base'] = self._discover_topic

        # augment  template data with device specific info

        # Name with captialization for auto discovery
        data["Name"] = device.name_caps if device.name_caps else device.addr.hex

        # conditionally add info from the DB if present 
        if device.db.firmware:
            data['sw_version'] = device.db.firmware
        if device.db.desc and device.db.desc.model and device.db.desc.description:
            data['model'] = device.db.desc.model + ": " + device.db.desc.description

        # manufacturer is hard coded (we probably exclude X10 devices here)
        data['manufacturer'] = 'Insteon'


        # templates are not recursively resolved. Since we know that 
        # 'discovery_device_info' by itself is a template, render it first
        if not 'discovery_device_info' in data:
            # lets use the MsgTemplate for rendering
            devInfo = MsgTemplate(topic=None,payload=self._discovery_device_info)
            data['discovery_device_info'] = devInfo.render_payload(data)

        # for each discovery template defined
        for qual in discList:
            # lookup topic and payload templates
            (discTopic, discPayload) = discovery_templates[qual]
            msg = MsgTemplate(topic=discTopic,payload=discPayload)
            # render template and publish 
            msg.publish(self,data,retain=True)

        return 

    #-----------------------------------------------------------------------
    def handle_new_device(self, modem, device):
        """New Insteon device callback.

        This is called when the Insteon modem creates a new device (from the
        config file).  We'll connect the device signals to our callbacks so
        we can send out MQTT messages when the device changes.

        Args:
          modem (Modem):  The Insteon modem device.
          device (device.Base):  The Insteon device that was added.
        """
        # Find the MQTT class type that matches the new insteon device.
        mqtt_cls = config.find(device)
        if not mqtt_cls:
            LOG.error("Coding error - can't find MQTT device class for "
                      "Insteon device %s: %s", device.__class__, device)
            return

        # Create the MQTT device class.  This will also link signals from the
        # Insteon device to the MQTT device.
        obj = mqtt_cls(self, device)

        # Set the configuration input data for this device type.
        if self._config:
            obj.load_config(self._config, self.qos)

        # NOTE announce code not added to here, sequence seems
        # to first load insteon devices, then connect to broker
        # hence sending out discovery messages is handled in the 
        # handle_connected 

        # Save the MQTT device so we can find it again.
        self.devices[device.addr.id] = obj

    #-----------------------------------------------------------------------
    def handle_cmd(self, client, userdata, message):
        """MQTT command message callback.

        This is called when an MQTT message is received.  Check it's topic
        and pass it off to the correct handler.  The command is a json
        dictionary that contains these keys:

        - session: Optional string to identify this command session.  Server
          will publish user interface messages to the session topic for
          communication back to the remote client.

        - cmd: The command dictionary.  This gets passed to the MQTT device
          that corresponds to the Instoen device for decoding.

        Args:
          client (paho.Client):  The paho mqtt client (self.link).
          data:  Optional user data (unused).
          message:  MQTT message - has attrs: topic, payload, qos, retain.
        """
        LOG.info("MQTT message %s %s", message.topic, message.payload)

        # Decode the JSON payload.
        try:
            data = json.loads(message.payload.decode("utf-8"))
        except:
            LOG.exception("Error decoding command payload: %s",
                          message.payload)
            return

        # For commands, we want the ability to send messages back to show
        # what's happening with the command.  We can't just print them
        # because this is a server.  So if the sender puts a 'session' key in
        # the data, we'll publish these user interface messages to that topic
        # so the remote client can get status upates.  Obviously the remote
        # client and this code have to match what they expect the session
        # topic to be.
        end_reply = lambda *x: None
        if "session" in data:
            # Turn the session into a topic.
            reply_topic = "%s/session/%s" % (message.topic,
                                             data.pop("session"))

            # Push the handle_reply callback to the logging object.  This way
            # any call to LOG.UI() will send out a message.  This allows the
            # server code to use the regular logging API to send out UI
            # messages to the remote client with out changing any of the
            # code.
            reply_cb = functools.partial(self.handle_reply, topic=reply_topic)
            LOG.set_ui_callback(reply_cb)

            # end_reply is called when the command is done and passes
            # record=None to indicate the command is finished.
            end_reply = functools.partial(self.handle_reply, None,
                                          topic=reply_topic)

        # Extract the device name/address from the topic and use it to find
        # the device object to handle the command.
        device_id = message.topic.split("/")[-1]
        device = self.modem.find(device_id)
        if not device:
            LOG.error("Unknown Insteon device '%s'", device_id)
            end_reply()
            return

        # Find the command string and map it to the method to use on the
        # device.
        cmd = data.pop("cmd", None)
        if not cmd:
            LOG.error("Input command has no 'cmd' key: %s", cmd)
            end_reply()
            return

        LOG.ui("Commanding %s device %s cmd=%s", device.type(), device.label,
               cmd)

        # Get the command function from the device.
        cmd_func = device.cmd_map.get(cmd, None)
        if not cmd_func:
            LOG.error("Unknown command '%s' for device type %s.  Valid "
                      "commands: %s", cmd, device.type(),
                      device.cmd_map.keys())
            end_reply()
            return

        # Set up a callback to handle when finished.  This will send out the
        # finaly reply to the session topic to insure the remote client knows
        # what happened.
        def on_done(success, msg, data):
            if success:
                LOG.ui(msg)
            else:
                LOG.error(msg)
            end_reply()

        try:
            # Pass the rest of the command arguments as keywords to the
            # method.
            cmd_func(on_done=on_done, **data)
        except:
            LOG.exception("Error running command %s on device %s", cmd,
                          device.label)
            end_reply()

    #-----------------------------------------------------------------------
    def handle_announce(self, client, userdata, message):
        """MQTT announce message callback.

        This is called when an MQTT message is received for controlling 
        discovery behavior. This is for all devices together.

        Args:
          client (paho.Client):  The paho mqtt client (self.link).
          data:  Optional user data (unused).
          message:  MQTT message - has attrs: topic, payload, qos, retain.
        """
        LOG.info("MQTT message %s %s", message.topic, message.payload)

        # Decode the JSON payload.
        try:
            data = json.loads(message.payload.decode("utf-8"))
        except:
            LOG.exception("Error decoding command payload: %s",
                          message.payload)
            return

        # NOTE the reply handing with GUI is just copied from handle_cmd 
        #  (except the callback as MQTT stuff is done synchronous in this
        #   thread -- I presume )

        # For commands, we want the ability to send messages back to show
        # what's happening with the command.  We can't just print them
        # because this is a server.  So if the sender puts a 'session' key in
        # the data, we'll publish these user interface messages to that topic
        # so the remote client can get status upates.  Obviously the remote
        # client and this code have to match what they expect the session
        # topic to be.
        end_reply = lambda *x: None
        if "session" in data:
            # Turn the session into a topic.
            reply_topic = "%s/session/%s" % (message.topic,
                                             data.pop("session"))

            # Push the handle_reply callback to the logging object.  This way
            # any call to LOG.UI() will send out a message.  This allows the
            # server code to use the regular logging API to send out UI
            # messages to the remote client with out changing any of the
            # code.
            reply_cb = functools.partial(self.handle_reply, topic=reply_topic)
            LOG.set_ui_callback(reply_cb)

            # end_reply is called when the command is done and passes
            # record=None to indicate the command is finished.
            end_reply = functools.partial(self.handle_reply, None,
                                          topic=reply_topic)

        if (data['cmd'] == 'register'):
            # announce own presence and presence of each device
            self.announce()
        elif (data['cmd'] == 'unregister'):
            # delete any previously announced entities
            self.unannounce()
        elif (data['cmd'] == 'flush'):
            self.unannounce()
            self.announce()
        else:
            # unkown command
            LOG.error("Unknown announce command '%s'", data[cmd])
            end_reply()
            return

        end_reply()
        return


    #-----------------------------------------------------------------------
    def handle_reply(self, record, topic):
        """: Session logging reply.

        This is called by the LOG.ui() function to handling sending status
        messages to the remote client.  The API is defined by the logging
        system.

        If record is None, that indicates the command is done.  We'll remove
        ourselves as a callback on the logging system in that case.

        Args:
          record:  Logging record.  None if the command is finished.
          topic (str):  The session topic to publish the log message to.
        """
        # Command is finished.  Cleanup and send an END reply.
        if record is None:
            LOG.del_ui_callback()
            reply = Reply(Reply.Type.END)

        # Normal reply.  Convert the logging object to a Reply object to send.
        else:
            type = Reply.Type.MESSAGE
            if record.levelno >= logging.ERROR:
                type = Reply.Type.ERROR

            reply = Reply(type, record.msg % record.args)

        # Publish the message to the remote client.
        payload = reply.to_json()
        self.link.publish(topic, payload)

    #-----------------------------------------------------------------------
    def _subscribe(self):
        """Subscribe to the command and set topics.

        This will subscribe to the command topic and tell all the MQTT
        devices to subscribe to their command topics.
        """
        if self._cmd_topic:
            self.link.subscribe(self._cmd_topic + "/+", self.qos,
                                self.handle_cmd)

        if self._announce_topic:
            self.link.subscribe(self._announce_topic, self.qos,
                                self.handle_announce)

        for device in self.devices.values():
            device.subscribe(self.link, self.qos)

    #-----------------------------------------------------------------------
    def _unsubscribe(self):
        """Unsubscribe to the command and set topics.

        This will unsubscribe from all the topics.
        """
        if self._cmd_topic:
            self.link.unsubscribe(self._cmd_topic + "/+")

        if self._announce_topic:
            self.link.unsubscribe(self._announce_topic)

        for device in self.devices.values():
            device.unsubscribe(self.link)

    #-----------------------------------------------------------------------

#===========================================================================
