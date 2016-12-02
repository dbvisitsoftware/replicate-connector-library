# Replicate Connector Library

Copyright (C) 2016 Dbvisit Software Limited -- Updated 1 December 2016

# 1 Introduction

Use the replicate connector library to connect to Parsed Logs (PLOGs) produced by [Dbvisit Replicate software](http://www.dbvisit.com), which mines change records from Oracle REDO Logs.

# 2 Licensing

This software is released under the Apache 2.0 license, a copy of which is located in the LICENSE file.

# 3 Development

You can build this library with Maven using the standard lifecycle phases.

To install it to local Maven repository:

```
mvn clean install
```

To use it in another project include it to your POM file:

```
<dependency>
    <groupId>com.dbvisit.replicate</groupId>
    <artifactId>replicate-connector-lib</artifactId>
    <version>2.8.02-SNAPSHOT</version>
</dependency>
```

To build the document the Replicate Connector Library:

```
mvn javadoc:javadoc
```

# 4 Contributions

Active communities contribute code and we're happy to consider yours. To be involved, please email <a href="mailto:mike.donovan@dbvisit.com">Mike Donovan</a>.

