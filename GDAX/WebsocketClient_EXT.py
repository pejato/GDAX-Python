#
# GDAX/WebsocketClient.py
# Daniel Paquin
#
# Template object to receive messages from the GDAX Websocket Feed

#
# GDAX/WebsocketClient_EXT.py
# Peter Tolsma
#
# Extended version of the template class

from __future__ import print_function
import json
import pytz
import time
from threading import Thread
from websocket import create_connection

# Takes time in str format
def get_time(time_str):
    if time_str[-1] == "Z":
        return datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
    return datetime.datetime.utc(int(time_str))    

class WSMessage(object):
    def __init___(self, json_dict):
        self.json_dict = json_dict

class WSMessage_Trade(WSMessage):
    def __init__(self, json_dict):
        WSMessage.__init__(self, json_dict)
        self.time = get_time(json_dict['time'])
        self.product = json_dict['product_id']
        self.sequence = int(json_dict['sequence'])
        # Indicates, make order side, sell indicates uptick, buy indicates downtick
        self.side = json_dict['side']

class WSMessage_Received(WSMessage_Trade):
    kind = "received"
    def __init__(self, json_dict):
        WSMessage_Trade.__init__(self, json_dict)
        self.order_id = json_dict['order_id']
        self.order_type = json_dict['order_type']
        if self.order_type == "market":
            self.size = float(json_dict['size'])
            self.price = float(json_dict['price'])
        elif self.order_type == "limit":
            self.funds = float(json_dict['funds'])

class WSMessage_Open(WSMessage_Trade):
    kind = "open"
    def __init__(self,json_dict):
        WSMessage_Trade.__init__(self, json_dict)
        self.price = float(json_dict['price'])
        self.order_id = json_dict['order_id']
        self.remaining_size = float(json_dict['remaining_size'])

class WSMessage_Done(WSMessage_Trade):
    kind = "done"
    def __init__(self, json_dict):
        WSMessage_Trade.__init__(self, json_dict)
        self.price = float(json_dict['price'])
        self.order_id = json_dict['order_id']
        self.remaining_size = float(json_dict['remaining_size'])
        self.reason = json_dict['reason']

class WSMessage_Match(WSMessage_Trade):
    kind = "match"
    def __init__(self,json_dict):
        WSMessage_Trade.__init__(self, json_dict)
        self.maker_order_id = json_dict['maker_order_id']
        self.taker_order_id = json_dict['taker_order_id']
        self.price = float(json_dict['price'])
        self.size = float(json_dict['size'])
        self.trade_id = int(json_dict['trade_id'])

class WSMessage_Change(WSMessage_Trade):
    kind = "change"
    def __init__(self, json_dict):
        WSMessage_Trade.__init__(self, json_dict)
        self.order_id = json_dict['order_id']
        self.price = json_dict['price']
        if "new_size" in json_dict:
            self.new_size = json_dict['new_size']
            self.old_size = json_dict['old_size']
        elif "new_funds" in json_dict:
            self.new_funds = json_dict['new_funds']
            self.old_funds = json_dict['old_funds']


class WSMessage_Margin(WSMessage):
    kind = "margin_profile_update"
    def __init__(self, json_dict):
        WSMessage.__init__(self, json_dict)
        self.product = json_dict['product_id']
        self.time = get_time(json_dict['timestamp'])
        self.user_id = json_dict['user_id']
        self.profile_id = json_dict['profile_id']
        self.nonce = int(json_dict['nonce'])
        self.position = float(json_dict['position'])
        self.position_size = float(json_dict['position_size'])
        self.position_complement = float(json_dict['position_complement'])
        self.position_max_size = float(json_dict['position_max_size'])
        self.call_side = json_dict['call_side']
        self.call_price = float(json_dict['call_price'])
        self.call_size = float(json_dict['call_size'])
        self.call_funds = float(json_dict['call_funds'])
        self.covered = json_dict['covered']
        self.next_expire_time = json_dict['next_expire_time']
        self.base_balance = float(json_dict['base_balance'])
        self.base_funding = float(json_dict['base_funding'])
        self.quote_balance = float(json_dict['quote_balance'])
        self.quote_funding = float(json_dict['quote_funding'])
        self.private = json_dict['private']

class WSMessage_Heartbeat(WSMessage):
    kind = "heartbeat"
    def __init__(self, json_dict):
        WSMessage.__init__(self, json_dict)
        self.sequence = json_dict['sequence']
        self.last_trade_id = json_dict['last_trade_id']
        self.product = json_dict['product_id']
        self.time = get_time(json_dict['time'])

class WSMessage_Error(WSMessage):
    kind = "error"
    def __init__(self, json_dict):
        WSMessage.__init__(self, json_dict)
        self.message = "error message"

message_types = [WSMessage_Received, WSMessage_Open, WSMessage_Done, WSMessage_Margin,wsClient,WSMessage_Heartbeat, 
                 WSMessage_Error]

# TODO Revamp onError? Remove it? Don't like its current method of error handling...
class WebsocketClient(object):
    """
    Websocket Client for GDAX cryptocurrency exchange. Makes use of the Python threading and websocket libraries.

    Args:
        url: The URL the client attempts to connect to; changing this will break the client
        products: A list of currency pairs to subscribe to with item format CURR1-CURR2. Supported currencies are :
                  BTC, ETH, LTC, USD
        type: Lets GDAX know that we wish to subscribe the web socket feed; changing this will break the client
        save_location: An optional dump file for received messages. If not included, no log will be kept.
        save_name: The name that the file will be saved under. Must supply save_location if save_name is supplied.
                   Each save_name will be prepended by a number (1, 2, etc)
        save_size: Max size of each save file in megabytes. When a file reaches this size, the program will write to a 
                   new file with an incremented file number.

    Returns:
        None

    Raises:
        IOError: Raised if provided save information is not valid.
    """
    def __init__(self, url=None, products=None, type=None, save_location=None, save_name=None, save_size = 1):
        if url is None:
            url = "wss://ws-feed.gdax.com"
        if save_location != None:
            if save_name == None: save_name = "default.txt"
        else: save_name = '1_' + save_name
            if save_location[-1] == ("/" or "\\"): save_location = save_location[:-1]
            try:
                self.write_file = open(save_location + "/" + save_name, "r")
            except IOError as e:
                if e.errno == 2:
                    raise IOError('Incorrect save location')
                else:
                    raise IOError(e.message)
                self.close()
            else:

        self.url = url
        self.save_size = long(save_size / (1024.0 ** 2)) 
        self.products = products
        self.type = "subscribe" #type or "subscribe"
        self.stop = False
        self.ws = None
        self.thread = None

    def start(self):
        """
        Sets up the connection and listening thread by calling _connect() and _listen()

        Args:
            None

        Returns:
            None

        Raises:
            None
        """
        
        def _go():
            self._connect()
            self._listen()

        self.onOpen()
        self.ws = create_connection(self.url)
        self.thread = Thread(target=_go)
        self.thread.start()

    def _connect(self):
        """
        Initializes connection with GDAX.

        Args:
            None

        Returns:
            None

        Raises:
            None
        """
        if self.products is None:
            self.products = ["BTC-USD"]
        elif not isinstance(self.products, list):
            self.products = [self.products]

        if self.url[-1] == "/":
            self.url = self.url[:-1]

        self.stop = False
        # required subscription params, changing these will break the client
        sub_params = {'type': 'subscribe', 'product_ids': self.products}
        self.ws.send(json.dumps(sub_params))
        if self.type == "heartbeat":
            sub_params = {"type": "heartbeat", "on": True}
            self.ws.send(json.dumps(sub_params))

    def _listen(self):
        """
        Waits to receive message, calling onMessage if the message is received successfully.

        Args:
            None

        Returns:
            None

        Raises:
            json related exceptions (?)
            TODO figure out what specifically this can raise
        """
        while not self.stop:
            try:
                # this will "block" ie freeze the program at this line until the message is received or an Exception is
                # raised
                msg = json.loads(self.ws.recv())
            except Exception as e:
                self.onError(e)
                self.close()
            else:
                self.onMessage(msg)

    def close(self):
        """
        Closes and cleans up the connection.

        Args:
            None

        Returns:
            None

        Raises:
            None
        """
        if not self.stop:
            if self.type == "heartbeat":
                self.ws.send(json.dumps({"type": "heartbeat", "on": False}))
            self.onClose()
            self.stop = True
            #self.thread = None
            self.thread.join()
            self.ws.close()
            self.write_file.close()

    def onOpen(self):
        print("-- Subscribed! --\n")

    def onClose(self):
        print("\n-- Socket Closed --")

    def onMessage(self, msg):
        if save_location != None:
            self.saveMessage(msg)
        # print(msg)

    def saveMessage(self, msg):
        """
        Saves message to a file as function receives them. Does not do any sorting.
        """
        for pair in msg:
            self.write_file.write("{string} : {value}\n".format(string=pair, value=msg[pair]))
        self.write_file.write("\n")
        self.wf_size = os.path.getsize(write_file.name)
        if wf_size > self.save_size:
            write_file.close()
            self.save_name = str(int(self.save_name[0]) + 1) + self.save_name[1:]
            write_file = open(self.save_location + '/' + self.save_name, 'w')


    def convMessage(self, msg):
                """
        Returns client message as WSMessage object

        Args:
            msg: The JSON object

        Returns:
            WSMessage object

        Raises:
            ValueError: if the message type cannot be deduced.
        """
        for message_type in message_types:
            if msg['type'] == message_type.kind:
                return message_type(msg)
        raise ValueError('Invalid or malformed message')

    def onError(self, e):
        SystemError(e)


if __name__ == "__main__":
    # import GDAX, time
    # class myWebsocketClient(GDAX.WebsocketClient):
    #     def onOpen(self):
    #         self.url = "wss://ws-feed.gdax.com/"
    #         self.products = ["BTC-USD", "ETH-USD"]
    #         self.MessageCount = 0
    #         print ("Lets count the messages!")

    #     def onMessage(self, msg):
    #         if 'price' in msg and 'type' in msg:
    #             print ("Message type:", msg["type"], "\t@ %.3f" % float(msg["price"]))
    #         self.MessageCount += 1

    #     def onClose(self):
    #         print ("-- Goodbye! --")

    # wsClient = myWebsocketClient()
    # wsClient.start()
    # print(wsClient.url, wsClient.products)
    # # Do some logic with the data
    # while (wsClient.MessageCount < 500):
    #     print ("\nMessageCount =", "%i \n" % wsClient.MessageCount)
    #     time.sleep(1)
    # wsClient.close()
