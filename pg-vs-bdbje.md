# PostgreSQL vs Berkeley DB Java Edition

##### Environment Details
OS VM Specs - Ubuntu 18.04, 4vcpu, 8GB RAM, 30GB SSD


JVM - Java8 with options "-server -Xms1024m -Xmx1024m"


##### Test Channel Details
The channel consists of a HTTP listener and a JS writer which doesn't do anything.

There are no transformers or processors in the channel. The channel code is [here](https://github.com/kayyagari/connect/blob/je/mc-integ-tests/http-listener.xml).

##### Message Payload and Client
The test message is a single HL7 message of size 2.8KB sent using a [client program](https://github.com/kayyagari/connect/blob/je/mc-integ-tests/mc-http-client.go)
from the localhost.


##### Results
| Database      | Number of messages | Total Time Taken |
| :------------ | :----------------: | :-------------   |
|Postgresql v10 | 100K               | 29m 27sec        |
|BDB JE v7.5.11 | 100K               | 4m 70sec         |
