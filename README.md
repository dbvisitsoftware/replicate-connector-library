# Replicate Connector Library

Copyright (C) 2016 Dbvisit Software Limited -- Updated 2 December 2016.

# 1 Introduction

Use the Replicate Connector Library to process Parsed Logs (PLOGs) produced by the [Dbvisit Replicate application](http://www.dbvisit.com/products/dbvisit_replicate_real_time_oracle_database_replication/), which mines change records from Oracle REDO Logs.

# 2 Licensing

This software is released under the Apache 2.0 license, a copy of which is located in the LICENSE file.

# 3 Development

You can build this library with Maven using the standard lifecycle phases.

To install it to local Maven repository:

```
mvn clean install
```

To use it in another project include it in your POM file:

```
<dependency>
    <groupId>com.dbvisit.replicate</groupId>
    <artifactId>replicate-connector-lib</artifactId>
    <version>2.8.04</version>
</dependency>
```

To build the documentation for the Replicate Connector Library:

```
mvn javadoc:javadoc
```

# 4 Contributions

Active communities contribute code and we're happy to consider yours. To be involved, please email <a href="mailto:github@dbvisit.com">Mike Donovan</a>.

