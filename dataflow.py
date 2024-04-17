'''
This script is an example of YahooFinance Connector that retrieves 
data from Yahoo Finance in real-time. This is based on websockets and bytewax 
(open-source framework to build highly scalable pipelines).
'''
import base64
import json
from datetime import timedelta

from bytewax import operators as op
from bytewax.testing import run_main

from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async

import websockets
from google.protobuf.json_format import MessageToJson
from ticker_pb2 import Ticker

# input
ticker_list = ['AMZN', 'MSFT']
# we can use BTC-USD outside of stock exchange opening hours
#ticker_list = ['BTC-USD']

# Asynchronous function yielding data input from Yahoo Finance...
# ...converted to JSON format
async def _ws_agen(worker_tickers):
    url = "wss://streamer.finance.yahoo.com/"
    async with websockets.connect(url) as websocket:
        msg = json.dumps({"subscribe": worker_tickers})
        await websocket.send(msg)
        await websocket.recv()

        while True:
            msg = await websocket.recv()
            # YahooFinance uses Protobufs
            # We need to deserialize the messages
            ticker_ = Ticker()
            msg_bytes = base64.b64decode(msg)
            ticker_.ParseFromString(msg_bytes)
            yield ticker_.id, ticker_

# Yahoo partition class inherited from Bytewax input StatefulSourcePartition class
class YahooPartition(StatefulSourcePartition):
    '''
    Input partition that maintains state of its position.
    '''
    def __init__(self, worker_tickers):
        '''
        Get async messages from Yahoo input and batch them
        up to 0,5 seconds or 100 messages.
        '''
        agen = _ws_agen(worker_tickers)
        self._batcher = batch_async(agen, timedelta(seconds=0.5), 100)

    def next_batch(self):
        '''
        Attempt to get the next batch of items.
        '''
        return next(self._batcher)

    def snapshot(self):
        '''
        Snapshot the position of the next read of this partition.
        Returned via the resume_state parameter of the input builder.
        '''
        return None

# Yahoo source class inherited from Bytewax input FixedPartitionedSource class
class YahooSource(FixedPartitionedSource):
    '''
    Input source with a fixed number of independent partitions.
    '''
    def __init__(self, worker_tickers):
        self.worker_tickers = worker_tickers

    def list_parts(self):
        '''
        List all partitions the worker has access to.
        '''
        return ["single-part"]

    def build_part(self, step_id, for_key, _resume_state):
        '''
        Build anew or resume an input partition.
        Returns the built partition
        '''
        return YahooPartition(self.worker_tickers)

# Creating dataflow and input
flow = Dataflow("yahoofinance")
inp = op.input(
    "input", flow, YahooSource(ticker_list)
)
# Printing dataflow
op.inspect("input_check", inp)

# Type in your terminal the following command to run the dataflow:
# python -m bytewax.run dataflow
# Please note that will launch an infinite loop, use Ctrl+C to interrupt...
# ...(you will get an error message you can ignore)
