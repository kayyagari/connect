# PostgreSQL vs Berkeley DB Java Edition

##### Environment Details
OS VM Specs - Ubuntu 18.04, 4vcpu, 8GB RAM, (SSD details are mentioned at individual results sections)


JVM - Java8 with options "-server -Xms1024m -Xmx1024m"


##### Test Channel Details
The channel consists of a HTTP listener and a JS writer which doesn't do anything.

There are no transformers or processors in the channel. The channel code is [here](https://github.com/kayyagari/connect/blob/je/mc-integ-tests/http-listener.xml).

##### Message Payload and Client
The test message is a single HL7 message of size 2.8KB sent using a [client program](https://github.com/kayyagari/connect/blob/je/mc-integ-tests/mc-http-client.go)
from the localhost.

##### Serialization Format
Internally data objects are stored in Berkeley DB JE in [Cap'n Proto](https://capnproto.org/) format.

##### Results
###### Hard Disk - Azure Premium SSD, 30GB with 120 Max IOPs
| Number of messages      | Time Taken for Postgresql v10 | Time Taken for BDB JE v7.5.11 |
| :------------ | :---------------- | :-------------   |
| 100K | 29m 27s | 4m 70s |


###### Hard Disk - Azure Premium SSD, 250GB with 1100 Max IOPs
| Number of messages | Time Taken for Postgresql v10 | Time Taken for BDB JE v7.5.11 |
| :------------ | :---------------- | :-------------   |
| 100K | 23m 31s | 3m 10s |
| 1M | 3h 47m 55s | 31m 21s |
| 10M | 37h 46m 20s | 5h 8m 38s |

Note: [Berkeley DB Java Edition](https://docs.oracle.com/cd/E17277_02/html/index.html) is licensed under Apache License v2.0