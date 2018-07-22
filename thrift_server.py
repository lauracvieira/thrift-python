#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

from dsid import ThriftService
from dsid import ttypes
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

import random

smallstringResponse = "x5cHKQWH0Vsou5Ej"
bigstringResponse = "xo6TV5sSIHSSVeMvna5YPhuIjgBpIj9RG5u6j5QVPMgnUKDSfoL2YIjDoXmcQjeAknfRg03qc83Xo2npjEbyvDcQIlMgjJpuO91SC3wArcM7nnfvghSJROecz49hmCXNNGlTZ4gTEtT2kt1RN7GyjLBivIvXI2fFsr6HkHRiaskSzdgVPBbwTpSJ0Bbq8fiKWbZfqMmBqGqDrqHoAYIFGg3FCMnRzeBp8t4RM0J1pASiys5WCXdc9p2QEP8aqXjjCbgCMqRfufdWFewowKtlIDdSZmPARA0F4UwFDkD11J4T7eSrCbp3Ml6CG8Hoy1k7x9bcXrxBhpLE5yW9xxQdclrQYu1IVSTks8m5XSWbapMIFoq0lvgjuNWiXLz5vTyiHeOTheiBMstjK7SXNlW9zXEALeaX4m2XuMnIoDawEt8Xl5R7wNAhDiFW8nHdFUxBlyizSaOUHHtmz5ayaomemxoiiurQkOdalHiLj7sdZCevMtHwuAiehGcvmOWRnGm89sg8vlwI7Tsiizrgio84LGkYh9zAkzKwqdo5INixJzsjdMRmfU3Q1ZzxYItlKPcBv3r52wnJv5bq6dHVMUFciuc7B8p4jLp6wUectpi9XcuPFE21y1U8aSb99nAA7sy7uOgu14KUJALwNTiXBLT61QcxzsjQvmCXyHaFShSzDELOrV6LT2kjRszC5zy5lG9t2sad1Cg5BAxLfwQCBHCBBLwnjuqCaTxBABVRjJYGQGqMcoHQdU9pgjmzgWsZQZA8gDq2GTDDG8LEnDVQvXURjc7PI5MmmlkAlv44N65gIUKB87uSiTGHrLYG4CGKlgMW1LTEmGTJQousjed8bcmHnes02lClyOJtB4613annnKs2OHbAXnpeRatka81j4aGejhkazQolSNtHbSRkL1voLjyQXzcC1tTfczAVn0jZOM4c2dBanCY4XMtbTuJjwfgawySH6nqEmX7j8zcq4PMevnzRP1Hww50OQK4xwKdb8QyXurXS8blHt2lM7ZzH7zBN"
longResponse = 12345678988881011

objectResponse = ttypes.Object()
objectResponse.longAttr = longResponse
objectResponse.stringAttr = smallstringResponse
objectResponse.booleanAttr = False

class ThriftHandler:

    def __init__(self):
        self.log = {}

    def VoidRequestVoid(self):
        print("VoidRequestVoid")

    def VoidRequestBigString(self):
        print("VoidRequestBigString")
        return bigstringResponse + str(random.randint(0, 9))

    def StringRequestSameString(self, parameter1):
        print("StringRequestSameString")
        return parameter1

    def StringRequestBigString(self, parameter1):
        print("StringRequestBigString")
        return bigstringResponse + str(random.randint(0, 9))

    def LongRequestLong(self, parameter1):
        print("LongRequestLong")
        return longResponse + random.randint(0, 9)

    def EightLongRequestLong(self, parameter1, parameter2, parameter3, parameter4, parameter5, parameter6, parameter7, parameter8):
        print("EightLongRequestLong")
        return longResponse + random.randint(0, 9)

    def LongListRequestLong(self, parameter1):
        print("LongListRequestLong")
        return longResponse + random.randint(0, 9)

    def LongRequestObject(self, parameter1):
        print("LongRequestObject")
        return objectResponse

    def ObjectRequestLong(self, parameter1):
        print("ObjectRequestLong")
        return longResponse + random.randint(0, 9)

    def ObjectRequestObject(self, parameter1):
        print("ObjectRequestObject")
        return objectResponse

    def BigIntegerList(self, parameter1):
        print("IntegerListRequestMethod")

if __name__ == '__main__':
    handler = ThriftHandler()
    processor = ThriftService.Processor(handler)
    transport = TSocket.TServerSocket(host='0.0.0.0', port=8001)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    # You could do one of these for a multithreaded server
    # server = TServer.TThreadedServer(
    #     processor, transport, tfactory, pfactory)
    # server = TServer.TThreadPoolServer(
    #     processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')
