/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <sstream>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for (int i = 0; i < 6; i++) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue <q_elt> *) env, (void *) buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if (initThisNode(&joinaddr) == -1) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if (!introduceSelfToGroup(&joinaddr)) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
//    int id = *(int *) (&memberNode->addr.addr);
//    int port = *(short *) (&memberNode->addr.addr[4]);

    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = TREMOVE;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if (0 == memcmp((char *) &(memberNode->addr.addr), (char *) &(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    } else {
        sendMessage(joinaddr, JOINREQ);
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
    /*
     * Your code goes here
     */
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if (!memberNode->inGroup) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;
#ifdef DEBUGLOG
    static char s[1024];
#endif
    // Pop waiting messages from memberNode's mp1q
    while (!memberNode->mp1q.empty()) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *) memberNode, (char *) ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
    /*
     * Your code goes here
     */
#ifdef DEBUGLOG
    static char s[1024];
#endif
    /*
     * Message format
     * |  enum MsgTypes msgType  |  messageCount |  id    |   port  | heartbeat |
     * |      1 byte             |   4 bytes     |4 bytes | 2 bytes |  4 bytes  |
     * |<------------ MessageHdr --------------->|<-------- MessageBody ------->|
     */
    MessageHdr *receivedMsg = (MessageHdr *) data;
    MessageBody *msgBody = (MessageBody *) (receivedMsg + 1);
    Address dstAddr;

    memset(&dstAddr, 0, sizeof(Address));
    *(int *) (&dstAddr.addr) = msgBody->id;
    *(short *) (&dstAddr.addr[4]) = msgBody->port;
    // #ifdef DEBUGLOG
    //    	sprintf(s, "recvCallBack: messageCount: %d,  %s. msgbody->id: %d, msgbody->port: %d", receivedMsg->messageCount, printAddress(&dstAddr).c_str(), msgBody->id, msgBody->port);
    //    	log->LOG(&memberNode->addr, s);
    // #endif

    /*
     * Add or update node in the member list
     */
    for (int i = 0; i < receivedMsg->messageCount; ++i) {
        addMember(msgBody->id, msgBody->port, msgBody->heartbeat);
        msgBody++;
    }

    if (receivedMsg->msgType == JOINREQ) {
        sendMessage(&dstAddr, JOINREP);
    } else if (receivedMsg->msgType == JOINREP) {
        memberNode->inGroup = true;
    }

    return true;
}


/**
 * FUNCTION NAME: sendMessage
 *
 * DESCRIPTION: send message
 */
void MP1Node::sendMessage(Address *dstAddr, MsgTypes msType) {
    MessageHdr *msg;
    MessageBody *body;
#ifdef DEBUGLOG
    static char s[1024];
#endif
    int n = 1;

    if (msType != JOINREQ) {
        n += memberNode->memberList.size();
    }

    size_t msgsize = sizeof(MessageHdr) + n * sizeof(MessageBody);
    msg = (MessageHdr *) malloc(msgsize);
    msg->msgType = msType;
    msg->messageCount = n;
    body = (MessageBody *) (msg + 1);
    body->id = *(int *) (&memberNode->addr.addr);
    body->port = *(short *) (&memberNode->addr.addr[4]);
    body->heartbeat = memberNode->heartbeat;
    if (msType == JOINREQ) {
        // #ifdef DEBUGLOG
        //       	sprintf(s, "Sending JOINREQ message... id: %d, port: %d, heartbeat: %ld to %s", body->id, body->port, body->heartbeat, printAddress(dstAddr).c_str());
        //       	log->LOG(&memberNode->addr, s);
        // #endif
    } else {
        // #ifdef DEBUGLOG
        // 	if (msType == JOINREP) {
        //       		sprintf(s, "Sending JOINREP message...");
        //       		log->LOG(&memberNode->addr, s);
        //       		sprintf(s, "size of memberlist: %ld\n", memberNode->memberList.size());
        //       		log->LOG(&memberNode->addr, s);
        //       	}
        //       #endif
        for (MemberListEntry &entry: memberNode->memberList) {
            body++;
            if (par->getcurrtime() - entry.gettimestamp() <= memberNode->pingCounter) {
                body->id = entry.getid();
                body->port = entry.getport();
                body->heartbeat = entry.getheartbeat();
            } else {
                // #ifdef DEBUGLOG
                //     			sprintf(s, "entry %d is expired.", entry.getid());
                //     			log->LOG(&memberNode->addr, s);
                // #endif
                body->id = *(int *) (&memberNode->addr.addr);
                body->port = *(short *) (&memberNode->addr.addr[4]);
                body->heartbeat = memberNode->heartbeat;
            }
        }
    }



    // send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, dstAddr, (char *) msg, msgsize);

    free(msg);
}

/**
 * FUNCTION NAME: addMember
 *
 * DESCRIPTION: Add or update member in member node list
 */
void MP1Node::addMember(int id, short port, long heartbeat) {
    if (!updateMember(id, port, heartbeat)) {
        memberNode->memberList.push_back(MemberListEntry(id, port, heartbeat, par->getcurrtime()));
        Address dstAddr;
        memset(&dstAddr, 0, sizeof(Address));
        *(int *) (&dstAddr.addr) = id;
        *(short *) (&dstAddr.addr[4]) = port;
        log->logNodeAdd(&(memberNode->addr), &dstAddr);
    }
}

/**
 * FUNCTION NAME: updateMember
 *
 * DESCRIPTION: Update member in member node list
 */
bool MP1Node::updateMember(int id, short port, long heartbeat) {
    for (MemberListEntry &entry: memberNode->memberList) {
        if (entry.id == id && entry.port == port) {
            if (entry.heartbeat < heartbeat) {
                entry.heartbeat = heartbeat;
                entry.timestamp = par->getcurrtime();
            }
            return true;
        }
    }
    return false;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    /*
     * Your code goes here
     */
#ifdef DEBUGLOG
    static char s[1024];
#endif

    memberNode->heartbeat++;
    Address dstAddr;

    /*
     * Randomly choose some nodes and send HEARTBEAT message
     */
    std::random_shuffle(memberNode->memberList.begin(), memberNode->memberList.end());
    for (int i = 0; i < 3 && i < memberNode->memberList.size(); i++) {
        memset(&dstAddr, 0, sizeof(Address));
        *(int *) (&dstAddr.addr) = memberNode->memberList[i].getid();
        *(short *) (&dstAddr.addr[4]) = memberNode->memberList[i].getport();
        // #ifdef DEBUGLOG
        //       	sprintf(s, "Sending HEARTBEAT to entry %s", printAddress(&dstAddr).c_str());
        //       	log->LOG(&memberNode->addr, s);
        // #endif
        sendMessage(&dstAddr, HEARTBEAT);
    }

    /*
     * Remove nodes which reach timeOut(T remove) counter
     */
    for (vector<MemberListEntry>::iterator entry = memberNode->memberList.begin();
         entry != memberNode->memberList.end();) {
        if (par->getcurrtime() - entry->gettimestamp() > memberNode->timeOutCounter) {
            // #ifdef DEBUGLOG
            //      		sprintf(s, "entry %d needs to be deleted.", entry->getid());
            //      		log->LOG(&memberNode->addr, s);
            // #endif
            memset(&dstAddr, 0, sizeof(Address));
            *(int *) (&dstAddr.addr) = entry->id;
            *(short *) (&dstAddr.addr[4]) = entry->port;
            log->logNodeRemove(&(memberNode->addr), &dstAddr);

            entry = memberNode->memberList.erase(entry);
        } else {
            entry++;
        }
    }

    // #ifdef DEBUGLOG
    //        sprintf(s, "%s", printMemberList().c_str());
    //        log->LOG(&memberNode->addr, s);
    // #endif

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *) (&joinaddr.addr) = 1;
    *(short *) (&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
string MP1Node::printAddress(Address *addr) {
    stringstream ss;
    ss << (int) (addr->addr[0]) << "."
       << (int) (addr->addr[1]) << "."
       << (int) (addr->addr[2]) << "."
       << (int) (addr->addr[3]) << ":"
       << *(short *) &addr->addr[4];
    return ss.str();
}

/**
 * FUNCTION NAME: printMemberList
 *
 * DESCRIPTION: Print the MemberList
 */
string MP1Node::printMemberList() {
    stringstream ss;
    ss << "\n--------------------------------------------------------------------------------------------------------------\n";
    ss << "current time: " << par->getcurrtime() << endl;
    for (MemberListEntry entry: memberNode->memberList) {
        ss << "id: " << entry.getid()
           << " | "
           << "port: " << entry.getport()
           << " | "
           << "heartbeat: " << entry.getheartbeat()
           << " | "
           << "timestamp: " << entry.gettimestamp()
           << " | "
           << "time diff: " << par->getcurrtime() - entry.gettimestamp() << endl;
    }
    ss << "\n--------------------------------------------------------------------------------------------------------------\n";
    return ss.str();
}
