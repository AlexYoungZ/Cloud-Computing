/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address) {
    this->memberNode = memberNode;
    this->par = par;
    this->emulNet = emulNet;
    this->log = log;
    ht = new HashTable();
    this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
    delete ht;
    delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
    /*
     * Implement this. Parts of it are already implemented
     */
    vector <Node> curMemList;
    bool change = false;

    /*
     *  Step 1. Get the current membership list from Membership Protocol / MP1
     */
    curMemList = getMembershipList();
    // for (Node node : curMemList) {
    // 	string logMsg("node in curMemList: " + to_string(node.getHashCode()) + " address: " + node.nodeAddress.getAddress());
    // 	log->LOG(&this->memberNode->addr, logMsg.c_str());
    // }

    /*
     * Step 2: Construct the ring
     */
    // Sort the list based on the hashCode
    sort(curMemList.begin(), curMemList.end());


    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
    if (ring.size() != curMemList.size()) {
        change = true;
    }

    for (int i = 0; i < ring.size(); i++) {
        if (ring[i].getHashCode() != curMemList[i].getHashCode()) {
            change = true;
            break;
        }
    }

    ring = curMemList;

    if (change) {
        stabilizationProtocol();
    }
    // Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
}

/**
 * FUNCTION NAME: getMembershipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector <Node> MP2Node::getMembershipList() {
    unsigned int i;
    vector <Node> curMemList;
    for (i = 0; i < this->memberNode->memberList.size(); i++) {
        Address addressOfThisMember;
        int id = this->memberNode->memberList.at(i).getid();
        short port = this->memberNode->memberList.at(i).getport();
        memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
        memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
        curMemList.emplace_back(Node(addressOfThisMember));
        //log->logNode(&this->memberNode->addr, &addressOfThisMember);
    }
    return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
    std::hash <string> hashFunc;
    size_t ret = hashFunc(key);
    return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
    /*
     * Implement this
     */
    dispatchMessages(CREATE, key, value);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key) {
    /*
     * Implement this
     */
    dispatchMessages(READ, key, "");
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value) {
    /*
     * Implement this
     */
    dispatchMessages(UPDATE, key, value);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key) {
    /*
     * Implement this
     */
    dispatchMessages(DELETE, key, "");
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
    /*
     * Implement this
     */
    Entry entry(value, par->getcurrtime(), replica);
    return ht->create(key, entry.convertToString());
    // Insert key, value, replicaType into the hash table
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
    /*
     * Implement this
     */
    // Read key from local hash table and return value
    string value = ht->read(key);
    if (value == "") {
        return value;
    } else {
        // Value in backend is stored in Entry format
        string logMsg("readKey key: " + key + " value: " + value);
        log->LOG(&this->memberNode->addr, logMsg.c_str());
        Entry entry(value);
        return entry.value;
    }
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
    /*
     * Implement this
     */
    // Update key in local hash table and return true or false
    Entry entry(value, par->getcurrtime(), replica);
    return ht->update(key, entry.convertToString());
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
    /*
     * Implement this
     */
    // Delete the key from the local hash table
    return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
    /*
     * Implement this. Parts of it are already implemented
     */
    char *data;
    int size;

    /*
     * Declare your local variables here
     */

    // dequeue all messages and handle them
    while (!memberNode->mp2q.empty()) {
        /*
         * Pop a message from the queue
         */
        data = (char *) memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();

        string message(data, data + size);

        Message msg(message);
        Message *replyMsg = nullptr;
        switch (msg.type) {
            case CREATE:
                replyMsg = new Message(msg.transID, memberNode->addr, REPLY, false);
                replyMsg->success = createKeyValue(msg.key, msg.value, msg.replica);
                if (replyMsg->success) {
                    log->logCreateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
                } else {
                    log->logCreateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
                }
                emulNet->ENsend(&memberNode->addr, &msg.fromAddr, replyMsg->toString());
                delete replyMsg;
                break;
            case READ:
                //READREPLY
                replyMsg = new Message(msg.transID, memberNode->addr, "");
                replyMsg->value = readKey(msg.key);
                if (replyMsg->value != "") {
                    log->logReadSuccess(&memberNode->addr, false, msg.transID, msg.key, replyMsg->value);
                } else {
                    log->logReadFail(&memberNode->addr, false, msg.transID, msg.key);
                }
                emulNet->ENsend(&memberNode->addr, &msg.fromAddr, replyMsg->toString());
                delete replyMsg;
                break;
            case UPDATE:
                replyMsg = new Message(msg.transID, memberNode->addr, REPLY, false);
                replyMsg->success = updateKeyValue(msg.key, msg.value, msg.replica);
                if (replyMsg->success) {
                    log->logUpdateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
                } else {
                    log->logUpdateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
                }
                emulNet->ENsend(&memberNode->addr, &msg.fromAddr, replyMsg->toString());
                delete replyMsg;
                break;
            case DELETE:
                replyMsg = new Message(msg.transID, memberNode->addr, REPLY, false);
                replyMsg->success = deletekey(msg.key);
                if (replyMsg->success) {
                    log->logDeleteSuccess(&memberNode->addr, false, msg.transID, msg.key);
                } else {
                    log->logDeleteFail(&memberNode->addr, false, msg.transID, msg.key);
                }
                emulNet->ENsend(&memberNode->addr, &msg.fromAddr, replyMsg->toString());
                delete replyMsg;
                break;
            case REPLY:
                processReply(msg);
                break;
            case READREPLY:
                processReply(msg);
                break;
            default:
                break;
        }
    }

    /*
     * Check timeout failure for all ongoing transaction
     */
    unordered_map<int, transactionInfo *>::const_iterator it = transactionMap.begin();
    vector < transactionInfo * > transactionInfos;
    while (it != transactionMap.end()) {
        transactionInfos.push_back(it->second);
        it++;
    }
    for (int i = 0; i < transactionInfos.size(); i++) {
        string logMsg2("Before calling checkTimeout");
        log->LOG(&this->memberNode->addr, logMsg2.c_str());
        checkTimeout(transactionInfos[i]);
    }
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given key function
 * 				This function is responsible for finding the replicas of a key
 */
vector <Node> MP2Node::findNodes(string key) {
    size_t pos = hashFunction(key);
    vector <Node> addr_vec;
    if (ring.size() >= 3) {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode()) {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        } else {
            // go through the ring until pos <= node
            for (int i = 1; i < ring.size(); i++) {
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode()) {
                    addr_vec.emplace_back(addr);
                    addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
                    addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
                    break;
                }
            }
        }
    }
    return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue <q_elt> *) env, (void *) buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
    /*
     * Implement this
     */
    int index = 0;
    for (index = 0; index < ring.size(); index++) {
        // Find myself in the ring list
        if (ring[index].nodeAddress == memberNode->addr) {
            break;
        }
    }
    vector <Node> replica;
    replica.push_back(ring[(index + 1) % ring.size()]);
    replica.push_back(ring[(index + 2) % ring.size()]);

    if (hasMyReplicas.size() < 2
        || !(hasMyReplicas[0].nodeAddress == replica[0].nodeAddress)
        || !(hasMyReplicas[1].nodeAddress == replica[1].nodeAddress)) {
        for (auto const &it: ht->hashTable) {
            dispatchMessages(CREATE, it.first, it.second);
        }
    }
    hasMyReplicas = replica;
}

/**
 * FUNCTION NAME: dispatchMessages
 *
 * DESCRIPTION: coordinator dispatches messages to corresponding nodes
 */
void MP2Node::dispatchMessages(MessageType type, string key, string value) {
    vector <Node> nodes = findNodes(key);
    if (!nodes.empty()) {
        int transactionID = getTransactionID();
        transactionInfo *info = new transactionInfo(0, 0, transactionID, par->getcurrtime(), type, key, value);
        if (transactionMap.find(transactionID) == transactionMap.end()) {
            transactionMap[transactionID] = info;
        } else {
            string logMsg("Transaction ID " + to_string(transactionID) + " already in map!");
            log->LOG(&this->memberNode->addr, logMsg.c_str());
            return;
        }
        Message primaryMessage(transactionID, memberNode->addr, type, key, value, PRIMARY);
        Message secondaryMessage(transactionID, memberNode->addr, type, key, value, SECONDARY);
        Message tertiaryMessage(transactionID, memberNode->addr, type, key, value, TERTIARY);
        string logMsg("client dispatch: primaryMessage: " + primaryMessage.toString());
        log->LOG(&this->memberNode->addr, logMsg.c_str());
        // Send primary message
        emulNet->ENsend(&memberNode->addr, &nodes.at(0).nodeAddress, primaryMessage.toString());
        // Send secondary message
        emulNet->ENsend(&memberNode->addr, &nodes.at(1).nodeAddress, secondaryMessage.toString());
        // Send tertiary message
        emulNet->ENsend(&memberNode->addr, &nodes.at(2).nodeAddress, tertiaryMessage.toString());
    } else {
        string logMsg("Cannot find replication nodes!");
        log->LOG(&this->memberNode->addr, logMsg.c_str());
    }
}

/**
 * FUNCTION NAME: processReply
 *
 * DESCRIPTION: Process reply message from server
 */
void MP2Node::processReply(Message receivedMessage) {
    int transactionID = receivedMessage.transID;
    string value = receivedMessage.value;
    bool success = receivedMessage.success;

    if (transactionMap.find(transactionID) != transactionMap.end()) {
        transactionInfo *info = transactionMap[transactionID];
        info->transactionCount++;
        // Create, Update, Delete case
        if (success) {
            info->transactionSuccess++;
        } else {
            // Read case
            if (value != "") {
                info->transactionSuccess++;
                info->value = value;
            }
        }
        /*
          * This function should also ensure all READ and UPDATE operation
          * get QUORUM replies
          */
        checkQuorum(info);

    }
}

/**
 * FUNCTION NAME: checkQuorum
 *
 * DESCRIPTION: Check quorum status
 */
void MP2Node::checkQuorum(transactionInfo *info) {
    int transactionID = info->transactionId;
    // We already got majority success
    if (info->transactionSuccess == 2) {
        switch (info->originalMsgType) {
            case CREATE:
                log->logCreateSuccess(&memberNode->addr, true, transactionID, info->key, info->value);
                break;
            case UPDATE:
                log->logUpdateSuccess(&memberNode->addr, true, transactionID, info->key, info->value);
                break;
            case READ:
                log->logReadSuccess(&memberNode->addr, true, transactionID, info->key, info->value);
                break;
            case DELETE:
                log->logDeleteSuccess(&memberNode->addr, true, transactionID, info->key);
                break;
            default:
                break;
        }
        string logMsg("Clean up transaction ID after success: " + to_string(transactionID));
        log->LOG(&this->memberNode->addr, logMsg.c_str());
        transactionMap.erase(transactionID);
        string logMsg2("Clean up transaction ID after success finished: " + to_string(transactionID) +
                       " transactionMap size: " + to_string(transactionMap.size()));
        log->LOG(&this->memberNode->addr, logMsg2.c_str());
        delete info;
    } else if (info->transactionCount == 3 && info->transactionSuccess < 2) {
        // We did not get majority success.
        switch (info->originalMsgType) {
            case CREATE:
                log->logCreateFail(&memberNode->addr, true, transactionID, info->key, info->value);
                break;
            case UPDATE:
                log->logUpdateFail(&memberNode->addr, true, transactionID, info->key, info->value);
                break;
            case READ:
                log->logReadFail(&memberNode->addr, true, transactionID, info->key);
                break;
            case DELETE:
                log->logDeleteFail(&memberNode->addr, true, transactionID, info->key);
                break;
            default:
                break;
        }
        string logMsg("Clean up transaction ID after failure: " + to_string(transactionID));
        log->LOG(&this->memberNode->addr, logMsg.c_str());
        transactionMap.erase(transactionID);
        string logMsg2("Clean up transaction ID after failure finished: " + to_string(transactionID) +
                       " transactionMap size: " + to_string(transactionMap.size()));
        log->LOG(&this->memberNode->addr, logMsg2.c_str());
        delete info;
    }
}

/**
 * FUNCTION NAME: checkTimeout
 *
 * DESCRIPTION: Process reply message from server
 */
void MP2Node::checkTimeout(transactionInfo *info) {
    if (par->getcurrtime() - info->createTime > 3) {
        info->transactionCount++;
        checkQuorum(info);
    }
}

/**
 * FUNCTION NAME: getTransactionId
 *
 * DESCRIPTION: Get and increase transaction id
 */
int MP2Node::getTransactionID() {
    string logMsg("Getting transactionID: " + to_string(g_transID));
    log->LOG(&this->memberNode->addr, logMsg.c_str());
    return g_transID++;
}