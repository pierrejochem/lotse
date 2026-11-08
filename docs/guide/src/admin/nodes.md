# Nodes

The list of nodes lives in the metadata group. The nodes in the cluster file are the first ones. Later changes are made at runtime and need no restart of the other nodes ([ADR 0048](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0048-adding-and-removing-nodes.md)).

Every node hosts the metadata group (Raft group 1) and the coordinator group (group 2). Both have as many voters as there are active nodes, at most five, and an odd number where possible. Every other node follows the metadata group as a learner, so each has the metadata state.

Adding and removing nodes needs a cluster admin. Listing them needs a cluster viewer. `<admin>` below is `--cert <file> --key <file>` or `--user <name>` with `LOTSE_PASSWORD` set.

## Add a node

1. Give the node a `[[node]]` entry in the cluster file and copy the file to every node. Node ids are never used twice.
2. Make its certificate: `lotse certs --config cluster.toml`. Existing files are kept.
3. Announce it to the cluster:

   ```sh
   lotse node add --config cluster.toml --node 4 <admin>
   ```

   The addresses come from the node's entry. If the node is not in the file, give `--peer <host:port>` and `--client-addr <host:port>`.
4. Start it on its machine with `--join`:

   ```sh
   lotse run --config cluster.toml --node 4 --join
   ```

   In a container or under systemd, set `LOTSE_JOIN=1` instead. With systemd, a drop-in made with `systemctl edit lotse@4` can set it.

A joining node starts outside the configured groups. Their leaders make it a member. The voters the configured groups started with are kept on the node's disk (`first-voters`) from its first start, so a later change of the file never alters them.

## Rebalancing

A new node fills up by itself ([ADR 0050](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0050-rebalancing-groups-onto-new-nodes.md)). While no node drains, the metadata leader moves one replica at a time from the node with the most replicas to the one with the fewest. It stops when the two differ by at most one replica or a tenth of the fullest node's count. At most four groups move at a time across the cluster. When a move is due, the group that evens out the two nodes' bytes most goes first ([ADR 0052](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0052-rebalancing-weighs-moves-by-size.md)).

Leaders spread too. Every node computes the same preferred leader for each group, one of its voters. A leader hands over to it with a Raft leader transfer, once that voter has every entry: at most one handover per node every 100 ticks and per group every 1000.

Each move is a full copy of the group through Raft. Rebalancing counts replicas; it uses sizes only to choose which group moves, so nodes with equal counts and very different sizes stay as they are. Traffic is not weighed.

## Drain and remove a node

```sh
lotse node remove --config cluster.toml --node 2 <admin>
lotse node list   --config cluster.toml <admin>
```

`remove` marks the node draining. Its replicas move to other nodes: for each group, the least loaded active node takes its place. `lotse node list` shows, per node, its addresses, whether it is `active` or `draining`, its replica count (`replicas`) and the groups moving (`moving`).

When no group counts the node as a voter and no move off it is under way, the metadata leader forgets it. The node itself stops, closing its listeners and groups, and `lotse run` exits with status 0, so a supervisor such as systemd (`Restart=on-failure`) does not start it again ([ADR 0051](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0051-a-drained-node-stops.md)).

A draining node that was cut off while the cluster forgot it asks the metadata leader for the node list every 100 ticks and stops when the list leaves it out. A node that cannot reach any node it lists keeps running until it can.

A drained node cannot come back under its old id. To return the machine to the cluster, join it under a new id.

## Pin a leader

A cluster admin can pin a partition's leader to one of its voters with `MoveLeader` (`move_leader` in `lotse-client`) ([ADR 0065](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0065-pinned-leaders.md)). It works for partitions of streams and queues created at runtime. A node id of 0 unpins.

- The pin is kept in the metadata state, so it holds across failovers and restarts. When the pinned node comes back, leadership returns to it.
- The handover waits until the target has every entry and is paced like the automatic one.
- A pin to a node that is no longer a voter counts for nothing, so a drain or rebalance is never blocked by it. A pin is dropped when its resource is deleted or its node leaves the cluster.

The console can move leaders as the signed-in user ([ADR 0066](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0066-console-actions.md)). It does not show whether a partition is pinned.

## Limits

- Four groups move at a time.
- A node that restarts with an old view of the metadata group asks for the node list and takes only the metadata group's frames from the nodes it is missing until its own state lists them ([ADR 0050](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0050-rebalancing-groups-onto-new-nodes.md)).
