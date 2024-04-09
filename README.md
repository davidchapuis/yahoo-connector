# Yahoo Finance Connector

This script is an example of YahooFinance Connector that retrieves 
data from Yahoo Finance in real-time. This is based on websockets and bytewax 
(open-source framework to build highly scalable pipelines).

## ****Prerequisites****

Make sure you are in the folder of the project and have a virtual environment activated.
Install the Python modules (Websockets, Protobuf, Bytewax) by runing the following command in your terminal:

``` bash
> pip install -r requirements.txt
```

## **Running the Dataflow**

``` bash
> python -m bytewax.run dataflow
```