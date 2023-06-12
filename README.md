# Cloud Computing

* * *

@ University of Illinois Urbana-Champaign   
@ [Course Website](https://www.coursera.org/specializations/cloud-computing#courses)

* * *

## Overview

<p>
Clouds, Distributed Systems, Networking. Learn about and build distributed and networked systems for clouds and big data.</p>

* * *

### Cloud Computing Programming Assignments

| Assignment Index                                            | Detailed Requirements                                                                                                                     | Quick Link to My Solution                                                               |
|-------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| [Assignment 1](#assignment1-membership-protocol)            | [Membership Protocol](https://github.com/AlexYoungZ/Cloud-Computing/blob/master/mp1/mp1_specifications.pdf)                               | [Gossip](https://github.com/AlexYoungZ/Cloud-Computing/tree/master/mp1)                 |
| [Assignment 2](#assignment2-fault-tolerant-key-value-store) | [Fault-Tolerant Key-Value Store](https://github.com/AlexYoungZ/Cloud-Computing/blob/master/mp2_assignment/MP2-specification-document.pdf) | [FT KV Store](https://github.com/AlexYoungZ/Cloud-Computing/tree/master/mp2_assignment) |

* * *

## Assignment1 Membership Protocol

<p>

This MP (Machine Programming assignment) is about implementing a membership protocol similar to one
we discussed in class. Since it is infeasible to run a thousand cluster nodes (peers) over a real network, we
are providing you with an implementation of an emulated network layer (EmulNet). Your membership
protocol implementation will sit above EmulNet in a peer- to-peer (P2P) layer, but below an App layer.
Think of this like a three-layer protocol stack with Application, P2P, and EmulNet as the three layers (from
top to bottom). More details are below.

Your protocol must satisfy: i) Completeness all the time: every non-faulty process must detect every node
join, failure, and leave, and ii) Accuracy of failure detection when there are no message losses and message
delays are small. When there are message losses, completeness must be satisfied and accuracy must be
high. It must achieve all of these even under simultaneous multiple failures.

Here are the functionalities your implementation must have:

* Introduction: Each new peer contacts a well-known peer (the introducer) to join the group. This is
  implemented through JOINREQ and JOINREP messages. Currently, JOINREQ messages reach the
  introducer, but JOINREP messages are not implemented. JOINREP messages should specify the
  cluster member list. The introducer does not need to maintain a list of all peers currently in the
  system; a partial list of fixed size can be maintained.
* Membership: You need to implement a membership protocol that satisfies completeness all the
  time (for joins and failures), and accuracy when there are no message delays or losses (high
  accuracy when there are losses or delays). We recommend implementing either gossip-style
  heartbeating or SWIM-style membership, although all to all heartbeating would be fine too (though
  you’d learn less). See lecture slides for more details.

</p>

* * *

## Assignment2 Fault-Tolerant Key-Value Store

<p>

In this MP, you will be building a fault-tolerant key-value store. We are providing you with the same
template provided for C3 Part 1 Programming Assignment (Membership Protocol), along with an almostcomplete
implementation of the key-value store, and a set of tests (which don’t pass on the released
code). This means first you need to use your working version of Membership Protocol from C3 Part 1
Programming Assignment and integrate it with this assignment. Then you need to fill in some key
methods to complete the implementation of the fault-tolerant key-value store and pass all the tests
</p>

<p>
Concretely, you will be implementing the following functionalities:          

* A key-value store supporting CRUD operations (Create, Read, Update, Delete).
* Load-balancing (via a consistent hashing ring to hash both servers and keys).
* Fault-tolerance up to two failures (by replicating each key three times to three successive nodes
  in the ring, starting from the first node at or to the clockwise of the hashed key).
* Quorum consistency level for both reads and writes (at least two replicas).
* Stabilization after failure (recreate three replicas after failure).

</p>

* * *
