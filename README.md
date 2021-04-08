# multimaster

multimaster is a Postgres extension with a set of core patches that turn the
DBMS into a synchronous shared-nothing symmetric cluster providing high
availability with strong consistency and read scalability.

It offers the following benefits, some of which are not available in traditional streaming replication based solutions:
* Fault tolerance and automatic node recovery
* Fast failover
* Both read and write transactions can be executed on any node.
* Read scalability
* Working with temporary tables on each cluster node
* Online minor upgrades

## Documentation

[current documentation](https://postgrespro.github.io/mmts/)

Documentation for versions released with PostgresPro Enterprise can be found
[here](https://postgrespro.ru/docs/enterprise/current/multimaster?lang=en).

## Building from source

Since multimaster depends on core patches, both Postgres and extension must be compiled. The patched version (based on Postgres 13) is available [here](https://github.com/postgrespro/postgres_cluster/tree/rel13_mm_2). Follow the [documentation](https://www.postgresql.org/docs/current/installation.html) to build it.

Then enter the build directory and install the extension with
```shell
cd contrib
git clone https://github.com/postgrespro/mmts/
cd mmts
make install
```
