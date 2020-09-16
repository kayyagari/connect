# PostgreSQL vs Berkeley DB Java Edition

##### Environment Details
OS VM Specs - Ubuntu 18.04, 4vcpu, 8GB RAM, 30GB SSD
JVM - Java8 with options "-server -Xms1024m -Xmx1024m"

##### Results
| Database      | Number of messages | Total Time Taken |
| :------------ | :----------------: | :-------------   |
|Postgresql v10 | 100K               | 29m 27sec        |
|BDB JE v7.5.11 | 100K               | 4m 70sec         |
