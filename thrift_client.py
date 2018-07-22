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
from dsid.ttypes import Object

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import numpy as np
import log

from sys import getsizeof
import time

logger = log.setup_custom_logger('thrift_client')

_STRINGS_TO_TEST = {
    1: "a",
    2: "ab",
    4: "abcd",
    8: "abcdefgh",
    16: "x5cHKQWH0Vsou5Ej",
    32: "xv1lWaeDWxn48TTVEjMFeam74Dj7xaeB",
    64: "VPh1NLqEa80CUurgztYQawE0D6uJWuwqZK8hxrpxflfbkRb3MnoPqAhWFkgLd41p",
    128: "so5aM6kdJb7a9EYKgDtDbWgmW59pU3sQ6TnNjVtRFUVQiHLUNlPfGRbu2tEUnmMuASpmNOr0WnhzYwePiYe594n2eFqAA22jidu85wAz8wwR74PLHOg1412fojP98KcA",
    256: "uMfPDBeBAtELNFnYcAi4JFwST5OyaoJergMCzuueIbWGLlFXYwUPTW3EA7h9EghKSoWVfL2497Rqg3Hx8TCmKU9aAvWwS9FFfwTiMA0B33IqyvTLfj3Gj65jZolhTIv0dJS1YWxWQRNa6kwHyTLvhfThzOpgp5p4aYR1gR30UfdrKCc29EWF9qseIoJirHE9ju7N140Zw8wx4jRIPhC5Vx5JhxPCbarBSTvc1DSTXKzpxA2dEtpWEIIeWMjWGFnF",
    512: "w1E0dYaHTXJrR0rEn5pEQJgi0OqWPY4G72P8P7C2b6Pey0MnDGlretB7jZxxvjVKIB0H6C3DYQvzK10NhOxYONT0QfktCGIo40wXR8tyY2GUpVEH72VZEzLAmYnvgTbVo6ggJ2faKH0eNs73jtGBY2BD8ErhWwelevxsZfkjRqaA7CzyARK6MROfJEbkSf0TZUyGfl5RZhDQFRhOO9spsTLFHOq3wVUdOku9KGGDeTituLEzUjruNdpQ1oc7g29lD5QW0CDK7o3wD8NkcxVll1LgQMzd5105S6p3fpuiJLBWtd8GeFPm7chv1U1BZPhEQF6jdvymSYt7wDYltpt7HhxoHKpuc68eHljAsKH7noAtNYKJZuRCE5LbBUcMz4EX5N311a16zATLOZayiF1F042hpR0UJdcMkBA1f0kQMTRG7nnoTIEk5PLQrubmuwIdLixNxk6sXQMFmGPFolNQDWBPISd76RkiysoK8Ak1By0tuAu79kSEqfM6OWFAkEyZ",
    1024: "xo6TV5sSIHSSVeMvna5YPhuIjgBpIj9RG5u6j5QVPMgnUKDSfoL2YIjDoXmcQjeAknfRg03qc83Xo2npjEbyvDcQIlMgjJpuO91SC3wArcM7nnfvghSJROecz49hmCXNNGlTZ4gTEtT2kt1RN7GyjLBivIvXI2fFsr6HkHRiaskSzdgVPBbwTpSJ0Bbq8fiKWbZfqMmBqGqDrqHoAYIFGg3FCMnRzeBp8t4RM0J1pASiys5WCXdc9p2QEP8aqXjjCbgCMqRfufdWFewowKtlIDdSZmPARA0F4UwFDkD11J4T7eSrCbp3Ml6CG8Hoy1k7x9bcXrxBhpLE5yW9xxQdclrQYu1IVSTks8m5XSWbapMIFoq0lvgjuNWiXLz5vTyiHeOTheiBMstjK7SXNlW9zXEALeaX4m2XuMnIoDawEt8Xl5R7wNAhDiFW8nHdFUxBlyizSaOUHHtmz5ayaomemxoiiurQkOdalHiLj7sdZCevMtHwuAiehGcvmOWRnGm89sg8vlwI7Tsiizrgio84LGkYh9zAkzKwqdo5INixJzsjdMRmfU3Q1ZzxYItlKPcBv3r52wnJv5bq6dHVMUFciuc7B8p4jLp6wUectpi9XcuPFE21y1U8aSb99nAA7sy7uOgu14KUJALwNTiXBLT61QcxzsjQvmCXyHaFShSzDELOrV6LT2kjRszC5zy5lG9t2sad1Cg5BAxLfwQCBHCBBLwnjuqCaTxBABVRjJYGQGqMcoHQdU9pgjmzgWsZQZA8gDq2GTDDG8LEnDVQvXURjc7PI5MmmlkAlv44N65gIUKB87uSiTGHrLYG4CGKlgMW1LTEmGTJQousjed8bcmHnes02lClyOJtB4613annnKs2OHbAXnpeRatka81j4aGejhkazQolSNtHbSRkL1voLjyQXzcC1tTfczAVn0jZOM4c2dBanCY4XMtbTuJjwfgawySH6nqEmX7j8zcq4PMevnzRP1Hww50OQK4xwKdb8QyXurXS8blHt2lM7ZzH7zBN"
}

rangesForBigRequest = [10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000]

longParameter = 12345678988881011

numIterations = 50
objectParameter = Object()
objectParameter.longAttr = longParameter
objectParameter.stringAttr = _STRINGS_TO_TEST[16]
objectParameter.booleanAttr = False

time_list = []
long_list = [longParameter, longParameter, longParameter, longParameter, longParameter, longParameter, longParameter,
             longParameter]

transport = None
client = None

def main():
    stablishConnection()

    logger.info(
        "Type of parameter | Size of parameter in bytes | Type of response | Size of response in bytes | average time in milliseconds | standard deviation | min time | max time | number of iterations: " + str(
            numIterations))

    # VoidRequestVoid
    for i in range(numIterations):
        start = time.time() * 1000
        client.VoidRequestVoid()
        end = time.time() * 1000
        time_list.append(end - start)
    log("Void", 0, "Void", 0)

    # VoidRequestBigString
    for i in range(numIterations):
        start = time.time() * 1000
        response = client.VoidRequestBigString()
        end = time.time() * 1000
        time_list.append(end - start)
    log("Void", 0, "String 1024 characters", getsizeof(response))

    # StringRequestSameString
    for key in _STRINGS_TO_TEST:
        for i in range(numIterations):
            start = time.time() * 1000
            response = client.StringRequestSameString(_STRINGS_TO_TEST[key])
            end = time.time() * 1000
            time_list.append(end - start)
        log("String " + str(key) + " characters", getsizeof(_STRINGS_TO_TEST[key]), "Same String " + str(key) + " characters",
            getsizeof(response))

    # StringRequestBigString
    for key in _STRINGS_TO_TEST:
        for i in range(numIterations):
            start = time.time() * 1000
            response = client.StringRequestBigString(_STRINGS_TO_TEST[key])
            end = time.time() * 1000
            time_list.append(end - start)
        log("String " + str(key) + " characters", getsizeof(response), "Big String 1024 characters",
            getsizeof(response))

    # LongRequestLong
    for i in range(numIterations):
        start = time.time() * 1000
        response = client.LongRequestLong(longParameter)
        end = time.time() * 1000
        time_list.append(end - start)
    log("Long", getsizeof(longParameter) * 8, "Long", getsizeof(response))

    # EightLongRequestLong
    for i in range(numIterations):
        start = time.time() * 1000
        response = client.EightLongRequestLong(longParameter, longParameter, longParameter, longParameter,
                                               longParameter,
                                               longParameter, longParameter, longParameter)
        end = time.time() * 1000
        time_list.append(end - start)
    log("8 Long Parameters", getsizeof(longParameter) * 8, "Long", getsizeof(response))

    # LongListRequestLong
    for i in range(numIterations):
        start = time.time() * 1000
        response = client.LongListRequestLong(long_list)
        end = time.time() * 1000
        time_list.append(end - start)
    log("8 Long in List", getsizeof(long_list), "Long", getsizeof(response))

    # LongRequestObject
    for i in range(numIterations):
        start = time.time() * 1000
        response = client.LongRequestObject(longParameter)
        end = time.time() * 1000
        time_list.append(end - start)
    log("Long", getsizeof(longParameter), "Object", getsizeof(response))

    # ObjectRequestLong
    for i in range(numIterations):
        start = time.time() * 1000
        response = client.ObjectRequestLong(objectParameter)
        end = time.time() * 1000
        time_list.append(end - start)
    log("Object", getsizeof(objectParameter), "Long", getsizeof(response))

    # ObjectRequestObject
    for i in range(numIterations):
        start = time.time() * 1000
        response = client.ObjectRequestObject(objectParameter)
        end = time.time() * 1000
        time_list.append(end - start)
    log("Object", getsizeof(objectParameter), "Object", getsizeof(response))

    # BigIntegerListRequest
    for key in rangesForBigRequest:
        bigRequestParameter = list(range(key))
        for i in range(numIterations):
            start = time.time() * 1000
            client.BigIntegerList(bigRequestParameter)
            end = time.time() * 1000
            time_list.append(end - start)
        log("BigIntegerList", getsizeof(objectParameter), "Void", 0)

    finishConection()

def stablishConnection():
    global transport, client
    transport = TSocket.TSocket('localhost', 8001)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = ThriftService.Client(protocol)
    transport.open()
    return client

def finishConection():
    transport.close()


def log(parameter_type, parameter_size, response_type, response_size):
    global time_list

    logger.info(parameter_type + " | " + str(parameter_size) + " | " + response_type + " | " + str(
        response_size) + " | " + repr(np.mean(time_list)) + " | " + repr(np.std(time_list, ddof=1)) + " | " + repr(
        min(time_list)) + " | " + repr(max(time_list)))
    time_list = []


if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
