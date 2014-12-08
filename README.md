1. 这个项目要做什么
---------------------------------------------

redis当前的最新版本是3.0.0 RC-1，支持cluster。但是redis的cluster还不够完善：

+ 节点不能自动发现，并自动构建cluster；
+ 不能动态地增删cluster中的节点；
+ 增删节点时，需要手动做数据迁移（resharding）；

redis提供了redis-trib.rb工具对cluster进行管理，包括创建集群、增删节点，以及数据的迁移。但是redis的java客户端Jedis并没有提供对应的实现。

本项目在Jedis的基础上，主要提供两个功能：

+ 对redis-trib.rb集群管理功能的java实现，包括创建集群、增删节点，以及数据迁移；
+ 集群模式下，mset/mget等可能跨slot的操作是被禁用的，但是我们可以根据CRC16算法，根据slot对所有的key进行分类，对一个slot中的key使用pipeline，这样在cluster模式下以pipeline的方式实现mset/mget等批量操作，可以显著提高性能。


2. 实现的要点
----------------------------------------------------

### 2.1 创建cluster的主要命令

+ CLUSER MEET
+ CLUSTER REPLICATE
+ CLUSTER SETSLOTS

### 2.2 数据迁移的主要命令

+ CLUSTER SETSLOT <slot> IMPORTING <source_node_id>
+ CLUSTER SETSLOT <slot> MIGRATING <target_node_id>
+ CLUSTER GETKEYSINSLOT <slot> <count>
+ CLUSTER MIGRATE <target_ip> <target_port> <key_name> 0 <timeout>
+ CLUSTER SETSLOT <slot> NODE <target_node_id>

### 2.3 增删节点

增删节点主要就是数据的迁移，比如增加节点的时候，如果是主节点，则需要从其它主节点迁移一些slot到新的主节点，如果删除的是主节点，则删除之前，需要将该主节点上的数据迁移到cluster中其它有效的主节点上。

### 2.4 cluster模式下批量操作的优化

cluster模式下不能直接使用mset/mget等可能跨slot的操作，但是我们知道key是如何map到slot的：使用CRC16算法计算key的值，然后对16384求余。因此，我们可以先对批量key进行预处理，将key都映射到对应的slot中，然后对每一个slot中的所有key使用pipeline操作，可以间接地实现mset/mget等操作，提高cluster模式下批量处理的性能。

> 具体的实现及使用请可以博文[redis-toolkit：Java实现的redis工具(一)](http://nkcoder.github.io/blog/20141024/redis-tookit-implement-in-java-1/)