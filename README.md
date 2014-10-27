redis-toolkit provides a utility interface for redis. It is implemented in Java, based on Jedis.

How To Use
--------------------------------------------------

### 1. Create a Cluster

We can create a cluster with redis-trib.rb like this:

	./redis-trib.rb create --replicas 1 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005
	
With redis-toolkit, the method `Create#create()` is used to create a new cluster, each master can have zero, one and many slaves:

	/* create a cluster with 3 masters, each has one slave */
    ArrayListMultimap<HostAndPort, HostAndPort> clusterNodes = ArrayListMultimap.create();
    clusterNodes.put(HostAndPort.fromString("127.0.0.1:7000"), HostAndPort.fromString("127.0.0.1:7001"));
    clusterNodes.put(HostAndPort.fromString("127.0.0.1:7002"), HostAndPort.fromString("127.0.0.1:7003"));
    clusterNodes.put(HostAndPort.fromString("127.0.0.1:7004"), HostAndPort.fromString("127.0.0.1:7005"));
    Create.create(clusterNodes);
    
### 2. Resharding

After beta8, redis supports resharding without interaction. For example, you can migrate 100 slots from 38807bd0262d99f205ebd0eb3e483cc09e927731 to 
47ef6c293bb3f9763d421f56c63f00cf06ef5b3f:

	redis-trib.rb reshard --from 38807bd0262d99f205ebd0eb3e483cc09e927731 --to 47ef6c293bb3f9763d421f56c63f00cf06ef5b3f --slots 100 --yes 127.0.0.1:7000
	
Redis-toolkit supports migrating, too. The method `Reshard#migrateSlots()` is to migrate the specified slots from one node to another, and the method
 `Reshard#migrate()` is to migrate specified number slots: 

	/* migrate slot 9189 from node 7002 to node 7006 */
    HostAndPort srcNodeInfo = HostAndPort.fromString("10.7.40.49:7002");
    HostAndPort destNodeInfo = HostAndPort.fromString("10.7.40.49:7006");
    Reshard.migrateSlots(srcNodeInfo, destNodeInfo, 9189);
    
    /* migrate 100 slots from node 7000 to node 7004 */
    HostAndPort srcNodeInfo = HostAndPort.fromString("10.7.40.49:7000");
    HostAndPort destNodeInfo = HostAndPort.fromString("10.7.40.49:7004");
    Reshard.migrate(srcNodeInfo, destNodeInfo, 100);
    
### 3. Add/Remove nodes

If the node to add is master, we should migrate some slots to it; if it is a slave, replicate it to it's master.

With redis-trib.rb, you can add a master node:

	./redis-trib.rb add-node 127.0.0.1:7006 127.0.0.1:7000
	
And you have two ways to add a slave node: 
	
	./redis-trib.rb add-node --slave 127.0.0.1:7006 127.0.0.1:7000
    
    ./redis-trib.rb add-node --slave --master-id 3c3a0c74aae0b56170ccb03a76b60cfe7dc1912e 127.0.0.1:7006

With redis-toolkit, we call `Manage#addNewNode()` to add a node to the cluster, the second param is the node to add, if it is a master, 
the third param
is null; if it is a slave, the third param is it's master node. For example:

	/* add master node 7006 to the cluster */ 
    HostAndPort clusterNodeInfo = HostAndPort.fromString("10.7.40.49:7000");
    HostAndPort newMaster = HostAndPort.fromString("10.7.40.49:7006");
    Manage.addNewNode(clusterNodeInfo, newMaster, null);
    
    /* add slave node 8001 to the cluster, it's master node is 7002 */
    HostAndPort clusterNodeInfo = HostAndPort.fromString("10.7.40.49:7000");
    HostAndPort master = HostAndPort.fromString("10.7.40.49:7002");
    HostAndPort newSlave = HostAndPort.fromString("10.7.40.49:8001");
    Manage.addNewNode(clusterNodeInfo, newSlave, master);

With redis-trib.rb, we can delete a node using the following command: 

	./redis-trib del-node 127.0.0.1:7000 3c3a0c74aae0b56170ccb03a76b60cfe7dc1912e
	
With redis-toolkit, we call `Manage#deleteNode()` to delete a node, the method itself will check whether the node is master or slave. If the node is 
master, first migrate all nodes away: 

	/* delete node 7006 from the cluster */
    HostAndPort oneNode = HostAndPort.fromString("10.7.40.49:7000");
    HostAndPort nodeToDelete = HostAndPort.fromString("10.7.40.49:7006");
    Manage.removeNode(oneNode, nodeToDelete);
