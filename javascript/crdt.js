"use strict";


var ItemID = function ItemID(logicalTs, peerId) {
    this.logicalTs = logicalTs;
    this.peerId = peerId;
};

ItemID.prototype.compareTo = function(other) {
    if (this.logicalTs > other.logicalTs)
        return 1;
    if (this.logicalTs < other.logicalTs)
        return -1;
    return this.peerId < other.peerId ? -1 : (this.peerId === other.peerId) ? 0 : 1;
};

ItemID.prototype.hashCode = function() {
    return this.logicalTs * 1337 + stringHashCode(this.peerId);
};

ItemID.prototype.equals = function(o) {
    return this.logicalTs === o.logicalTs && this.peerId === o.peerId;
};

ItemID.prototype.toString = function() {
    return "ItemID(logicalTs: " + this.logicalTs + ", peerId: " + this.peerId + ")";
};


var Message = function Message(originPeerId, msgCount, operations) {
    this.originPeerId = originPeerId;
    this.msgCount = msgCount;
    this.operations = operations;
};

var MessageProcessed = function MessageProcessed(msgCount) {
    this.msgCount = msgCount;
};

MessageProcessed.prototype.toString = function() {
    return "MessageProcessed (msgCount: " + this.msgCount + ")";
};

var Peer = function Peer(peerId) {
    this.peerId = peerId || this.createRandomPeerID();
    this.peerMatrix = new PeerMatrix(this.peerId);
    this.orderedList = new OrderedList(this);
    this.logicalTs = 0;
    this.sendBuf = [];
    this.recvBuf = new Map();
};

Peer.prototype.createRandomPeerID = function() {
    // TODO check if this is a good way to generate a random hex string code and whether we need a
    // cryptographically secure one.
    // From http://stackoverflow.com/questions/5398737/how-can-i-make-a-simple-wep-key-generator-in-javascript
    var length = 64;
    var ret = "";
    while (ret.length < length) {
        ret += Math.random().toString(16).substring(2);
    }
    return ret.substring(0, length);
};

Peer.prototype.anythingToSend = function() {
//    return !this.peerMatrix.localClockUpdate.empty() ||
    return this.sendBuf.length !== 0;
};

Peer.prototype.nextId = function() {
    return new ItemID(++this.logicalTs, this.peerId);
};

Peer.prototype.sendOperation = function(operation) {
    if (!this.peerMatrix.localClockUpdate.empty()) {
        this.sendBuf.push(this.peerMatrix.localClockUpdate);
        this.peerMatrix.resetClockUpdate();
    }

    this.sendBuf.push(operation);
};

Peer.prototype.makeMessage = function() {
    if (!this.peerMatrix.localClockUpdate.empty()) {
        this.sendBuf.push(this.peerMatrix.localClockUpdate);
        this.peerMatrix.resetClockUpdate();
    }

    var message = new Message(this.peerId, this.peerMatrix.getMessageCount(true), this.sendBuf);
    this.sendBuf = [];
    return message;
};

Peer.prototype.processMessage = function(message) {
    if (!(message instanceof Message))
        throw "Invalid message: " + message;
    if (this.recvBuf.get(message.originPeerId) === undefined)
        this.recvBuf.set(message.originPeerId, []);
    // append all elements in message.operations to this.recvBuf
    Array.prototype.push.apply(this.recvBuf.get(message.originPeerId), message.operations);

    this.recvBuf.get(message.originPeerId).push(new MessageProcessed(message.msgCount));
    while (this.applyOperationsIfReady())
        ;
};

Peer.prototype.applyOperationsIfReady = function() {
    var readyPeerId, readyOps;
    for (var peerId of this.recvBuf.keys()) {
        if (this.peerMatrix.isCausallyReady(peerId) && this.recvBuf.get(peerId).length > 0) {
            readyPeerId = peerId;
            readyOps = this.recvBuf.get(peerId);
            break;
        }
    }
    if (readyPeerId === undefined)
        return false;

    while (readyOps.length > 0) {
        var operation = readyOps.shift();
        console.log("peer " + this.peerId + ": Processing operation " + operation.constructor.name + ": " + operation);

        if (operation instanceof ClockUpdate) {
            this.peerMatrix.applyClockUpdate(readyPeerId, operation);

            // Applying the clock update might make the following operations causally non-ready, so we stop processing
            // operations from this peer and check again for causal readiness.
            return true;
        } else if (operation instanceof MessageProcessed) {
            this.peerMatrix.processedIncomingMsg(readyPeerId, operation.msgCount);
        } else {
            if (this.logicalTs < operation.logicalTs()) {
                this.logicalTs = operation.logicalTs();
            }
            this.orderedList.applyOperation(operation);
        }
    }

    return true; // Finished this peer, now another peer's operations might be causally ready
};


var PeerMatrix = function PeerMatrix(ownPeerId) {
    // matrix is an array of arrays (i.e. a 2D array). matrix[peer1Index][peer2Index] is a PeerVClockEntry object. Each
    // such object records how many operations peer1 has seen from peer2. peer1Index is according to this peer's local
    // index assignment (see indexByPeerId); peer2Index is according to peer1's index assignment.
    this.matrix = [[new PeerVClockEntry(ownPeerId, 0, 0)]];

// A hash, where the key is a peer ID (as hex string) and the value is the index that this peer has locally assigned to
// that peer ID. The indexes must be strictly sequential.
    this.indexByPeerId = new Map();
    this.indexByPeerId.set(ownPeerId, 0);

// This is used to record any operations we see from other peers, so that we can broadcast vector clock diffs to others.
    this.localClockUpdate = new ClockUpdate();
};

// The peer ID (globally unique hex string) for the local device.
PeerMatrix.prototype.ownPeerId = function() {
    return this.matrix[0][0].peerId;
};

// When we get a message from originPeerId, it may refer to another peer by an integer index remotePeerIndex. This
// method translates remotePeerIndex (which is meaningful only in the context of messages from originPeerId) to the
// corresponding peer Id (a hex string that is globally unique).
PeerMatrix.prototype.remoteIndexToPeerId = function(originPeerId, remotePeerIndex) {
    var entry = this.matrix[this.peerIdToIndex(originPeerId)][remotePeerIndex];
    if (entry)
        return entry.peerId;
    throw "remoteIndexToPeerId: No peer Id for index " + remotePeerIndex;
};

// Translates a globally unique peer ID into a local peer index. If the peer ID is not already known, it is added to the
// matrix and assigned a new index.
PeerMatrix.prototype.peerIdToIndex = function(peerId) {
    var index = this.indexByPeerId.get(peerId);
    if (index !== undefined) {
        return index;
    }

    if ((this.indexByPeerId.size !== this.matrix.length) ||
            (this.indexByPeerId.size !== this.matrix[0].length) ||
            this.matrix[0].some(function(entry) {
                return entry.peerId === peerId;
            })) {
        throw 'peerIdToIndex: Mismatch between vector clock and peer list';
    }

    index = this.indexByPeerId.size;
    this.indexByPeerId.set(peerId, index);
    this.matrix[0][index] = new PeerVClockEntry(peerId, index, 0);
    this.matrix[index] = [new PeerVClockEntry(peerId, 0, 0)];
    this.localClockUpdate.addPeer(peerId, index);
    return index;
};

// Indicates that the peer originPeerId has assigned an index of subjectPeerIndex to the peer subjectPeerId.
// Calling this method registers the mapping, so that subsequent calls to remoteIndexToPeerId can resolve the index.
// Returns the appropriate PeerVClockEntry.
PeerMatrix.prototype.peerIndexMapping = function(originPeerId, subjectPeerId, subjectPeerIndex) {
    var vclock = this.matrix[this.peerIdToIndex(originPeerId)];
    var entry = vclock[subjectPeerIndex];

    if (entry) {
        if (subjectPeerId !== null && subjectPeerId !== entry.peerId) {
            throw 'peerIndexMapping: Contradictory peer index assignment';
        }
        return entry;
    } else if (subjectPeerIndex !== vclock.length) {
        throw 'peerIndexMapping: Non-consecutive peer index assignment';
    } else if (subjectPeerId === null) {
        throw 'peerIndexMapping: New peer index assignment without Id';
    }

    entry = new PeerVClockEntry(subjectPeerId, subjectPeerIndex, 0);
    return vclock[subjectPeerIndex] = entry;
};

// Processes a clock update from a remote peer and applies it to the local state. The update indicates that
// originPeerId has received various operations from other peers, and also documents which peer indexes originPeerId
// has assigned to those peers.
PeerMatrix.prototype.applyClockUpdate = function(originPeerId, update) {
    for (var newEntry of update.entries()) {
        var oldEntry = this.peerIndexMapping(originPeerId, newEntry.peerId, newEntry.peerIndex);
        if (oldEntry.msgCount > newEntry.msgCount) {
            throw 'applyClockUpdate: Clock update went backwards';
        }
        oldEntry.msgCount = newEntry.msgCount;
    }
};

// Increments the message counter for the local peer, indicating that a message has been broadcast to other peers.
PeerMatrix.prototype.getMessageCount = function(increment) {
    if (increment)
        this.matrix[0][0].msgCount++;
    return this.matrix[0][0].msgCount;
};

// Increments the message counter for a particular peer, indicating that we have processed a message that originated on
// that peer. In other words, this moves the vector clock forward.
PeerMatrix.prototype.processedIncomingMsg = function(originPeerId, msgCount) {
    var originIndex = this.peerIdToIndex(originPeerId);
    var localEntry  = this.matrix[0][originIndex];
    var remoteEntry = this.matrix[originIndex][0];

// We normally expect the msgCount for a peer to be monotonically increasing. However, there's a possible scenario in
// which a peer sends some messages and then crashes before writing its state to stable storage, so when it comes back
// up, it reverts back to a lower msgCount. We should detect when this happens, and replay the lost messages from
// another peer.
    if (localEntry.peerId !== originPeerId) {
        throw "processedIncomingMsg: peerid mismatch: " + localEntry.peerId + " != " + originPeerId;
    }
    if (localEntry.msgCount + 1 > msgCount) {
        throw "processedIncomingMsg: msgCount for " + originPeerId + " went backwards";
    }
    if (localEntry.msgCount + 1 < msgCount) {
        throw "processedIncomingMsg: msgCount for " + originPeerId + " jumped forwards";
    }

    localEntry.msgCount = msgCount;
    remoteEntry.msgCount = msgCount;

    this.localClockUpdate.recordUpdate(originPeerId, originIndex, msgCount);
};

// Returns true if operations originating on the given peer ID are ready to be delivered to the application, and false
// if they need to be buffered. Operations are causally ready if all operations they may depend on (which had been
// processed by the time that operation was generated) have already been applied locally. We assume that pairwise
// communication between peers is totally ordered, i.e. that messages from one particular peer are received in the same
// order as they were sent.
PeerMatrix.prototype.isCausallyReady = function(remotePeerId) {
    var localVclock = new Map();
    for (var entry of this.matrix[0]) {
        localVclock.set(entry.peerId, entry.msgCount);
    }

    var remoteVclock = new Map();
    for (var entry of this.matrix[this.peerIdToIndex(remotePeerId)]) {
        remoteVclock.set(entry.peerId, entry.msgCount);
    }

    var allPeerIds = new Set([...localVclock.keys(), ...remoteVclock.keys()]);
    for (var peerId of allPeerIds) {
        if ((peerId !== remotePeerId) && ((localVclock.get(peerId) || 0) < (remoteVclock.get(peerId) || 0)))
            return false;
    }
    return true;
};

// Resets the tracking of messages received from other peers. This is done after a clock update has been broadcast to
// other peers, so that we only transmit a diff of changes to the clock since the last clock update.
PeerMatrix.prototype.resetClockUpdate = function() {
    this.localClockUpdate = new ClockUpdate();
};


// One entry in a vector clock. The peerId is the hex string representing a peer; the peerIndex is the number we have
// locally assigned to that peer; and msgCount is the number of messages we have received from that peer.
var PeerVClockEntry = function PeerVClockEntry(peerId, peerIndex, msgCount) {
    this.peerId = peerId;
    this.peerIndex = peerIndex;
    this.msgCount = msgCount;
};

PeerVClockEntry.prototype.toString = function() {
    return "PeerVClockEntry (peerId: " + this.peerId + ", peerIndex: " + this.peerIndex + ", msgCount: " + this.msgCount + ")";
};

// A clock update is a special kind of operation, which can be broadcast from one peer to other peers. When a
// ClockUpdate is sent, it reflects the messages received by the sender (i.e. which operations the sender has previously
// received from other peers). This is used to track the causal dependencies between operations. When building up
// locally, no argument is given. When received from a remote peer, the argument is an array of PeerV_clock_entry
// objects.
var ClockUpdate = function ClockUpdate() {
    this.updateByPeerId = new Map(); // key is a peer ID (hex string), value is a PeerVClockEntry object.
};

ClockUpdate.prototype.addPeer = function(peerId, peerIndex) {
    this.updateByPeerId.set(peerId, new PeerVClockEntry(peerId, peerIndex, 0));
};

ClockUpdate.prototype.recordUpdate = function(peerId, peerIndex, msgCount) {
    if (!this.updateByPeerId.has(peerId))
        this.updateByPeerId.set(peerId, new PeerVClockEntry(null, peerIndex, 0));
    this.updateByPeerId.get(peerId).msgCount = msgCount;
};

ClockUpdate.prototype.empty = function() {
    return this.updateByPeerId.size === 0;
};

ClockUpdate.prototype.entries = function() {
    var res = Array.from(this.updateByPeerId.values());
    res.sort(function(a, b) {return a.peerIndex - b.peerIndex;});
    return res;
};

ClockUpdate.prototype.toString = function() {
    return "ClockUpdate (" + Array.from(this.updateByPeerId.entries()) + ")";
};


var InsertOp = function InsertOp(referenceId, newId, value) {
    this.referenceId = referenceId;
    this.newId = newId;
    this.value = value;
};

InsertOp.prototype.logicalTs = function() {
    return this.newId.logicalTs;
};

var DeleteOp = function DeleteOp(deleteId, deleteTs) {
    this.deleteId = deleteId;
    this.deleteTs = deleteTs;
};

DeleteOp.prototype.logicalTs = function() {
    return this.deleteTs.logicalTs;
};

var Item = function Item(insertId, deleteTs, value, previous, next) {
    this.insertId = insertId;
    this.deleteTs = deleteTs;
    this.value = value;
    this.previous = previous;
    this.next = next;
};

var OrderedList = function OrderedList(peer) {
    if (peer === undefined)
        throw 'OrderedList: peer must be set';
    this.peer = peer;
    this.lamportClock = 0;
    this.itemsById = new HashMap();
    this.head = null;
    this.tail = null;
    this.eventListeners = new Set();
};

// Iterates over the items in the list in order, skipping tombstones
OrderedList.prototype[Symbol.iterator] = function() {
    var item = this.head;

    return {
        next: function() {
            while (item !== null && item.deleteTs !== null)
                item = item.next;

            var ret = item;
            if (ret === null)
                return {done: true};
                item = item.next;
                return { value: ret.value, done: false};
        }
    };
};

OrderedList.prototype.eachItem = function(callback) {
    var item = this.head;
    while (item) {
        callback(item);
        item = item.next;
    }
};
// Inserts a new item at the given index in the list (local operation). An index of 0 inserts at the head of the list;
// index out of bounds appends at the end.
OrderedList.prototype.insert = function(index, value) {
    var leftId = null;
    if (index > 0) {
        var rightItem = this.itemByIndex(index);
        leftId = rightItem ? rightItem.previous.insertId : this.tail.insertId;
    }
    var item = this.insertAfterId(leftId, this.peer.nextId(), value);
    var op = new InsertOp(item.previous && item.previous.insertId, item.insertId, item.value);
    this.peer.sendOperation(op);
    return this;
};
// Inserts a new item before the existing item identified by cursorId (local operation). If cursorId is nil, appends
// to the end of the list.
OrderedList.prototype.insertBeforeId = function(cursorId, value) {
    var leftId;
    if (cursorId === null) {
        leftId = this.tail && this.tail.insertId;
    } else {
        var rightItem = this.itemsById.get(cursorId);
        if (rightItem === undefined)
            throw 'insertBeforeId: unknown cursorId: ' + cursorId;
        leftId = rightItem.previous && rightItem.previous.insertId;
    }

    var item = this.insertAfterId(leftId, this.peer.nextId(), value);
    this.peer.sendOperation(new InsertOp(item.previous && item.previous.insertId, item.insertId, item.value));
    return item.insertId;
};

// Deletes the item at the given index in the list (local operation).
OrderedList.prototype.remove = function(index) {
    var item = this.itemByIndex(index);
    if (item === undefined) {
        throw 'remove: unknown item with index ' + index;
    }
    if (item === null) {
        throw 'remove: item is null with index ' + index;
    }
    item.deleteTs = this.peer.nextId();
    item.value = null;
    this.peer.sendOperation(new DeleteOp(item.insertId, item.deleteTs));
    return this;
};
// Deletes numItems items from the list (local operation). The items to be deleted are to the left of the item
// identified by cursorId (not including the item identified by cursorId itself). If cursorId is nil, deletes
// numItems from the end of the list. Returns the ID of the last non-deleted item before the sequence of deleted
// items.
OrderedList.prototype.removeBeforeId = function(cursorId, numItems) {
    var item, cursor;
    if (cursorId === null) {
        item = this.tail;
    } else {
        cursor = this.itemsById.get(cursorId);
        if (cursor === undefined)
            throw 'removeBeforeId: unknown cursorId: ' + cursorId;
        item = cursor.previous;
    }

    while (item && (numItems > 0 || item.deleteTs)) {
        if (item.deleteTs === null) {
            item.deleteTs = this.peer.nextId();
            item.value = null;
            this.peer.sendOperation(new DeleteOp(item.insertId, item.deleteTs));
            numItems--;
        }

        item = item.previous;
    }

    return item && item.insertId;
};
// Deletes numItems items from the list (local operation). The item identified by cursorId is the first item to be
// deleted, and the other deleted items are on its right. Returns the ID of the first non-deleted item after the
// sequence of deleted items.
OrderedList.prototype.removeAfterId = function(cursorId, numItems) {
    var item = this.itemsById.get(cursorId);
    if (item === undefined) {
        throw 'removeAfterId: unknown item with cursorId ' + cursorId;
    }
    if (item === null) {
        throw 'removeAfterId: item is null with cursorId ' + cursorId;
    }

    while (item && (numItems > 0 || item.deleteTs)) {
        if (item.deleteTs === null) {
            item.deleteTs = this.peer.nextId();
            item.value = null;
            this.peer.sendOperation(new DeleteOp(item.insertId, item.deleteTs));
            numItems--;
        }

        item = item.next;
    }

    return item && item.insertId;
};
// Applies a remote operation to a local copy of the data structure. The operation must be causally ready, as per the
// data structure's vector clock.
OrderedList.prototype.applyOperation = function(operation) {
    if (operation instanceof InsertOp) {
        this.insertAfterId(operation.referenceId, operation.newId, operation.value);
    } else if (operation instanceof DeleteOp) {
        var item = this.itemsById.get(operation.deleteId);
        if (item === undefined)
            throw 'applyOperation: unknown item with id ' + operation.deleteId;
        item.deleteTs = operation.deleteTs;
        item.value = null;
    } else {
        throw "applyOperation: Invalid operation: " + operation;
    }

    this.onOperation(operation);
};

// Inserts a new list item to the right of the item identified by leftId. If leftId is nil, inserts a new list item
// at the head. The new item has Id insertId and the given value. Returns the newly inserted item.
OrderedList.prototype.insertAfterId = function(leftId, insertId, value) {
    var leftItem = null;
    if (leftId !== null) {
        leftItem = this.itemsById.get(leftId);
        if (leftItem === undefined)
            throw 'insertAfterId: unknown item with id ' + leftId;
    } else if (this.head && this.head.insertId.compareTo(insertId) > 0) {
        leftItem = this.head;
    }

    while (leftItem && leftItem.next && leftItem.next.insertId.compareTo(insertId) > 0) {
        leftItem = leftItem.next;
    }
    var rightItem = leftItem ? leftItem.next : this.head;

    var item = new Item(insertId, null, value, leftItem, rightItem);
    this.itemsById.set(insertId, item);

    if (leftItem !== null)
        leftItem.next = item;
    if (rightItem !== null)
        rightItem.previous = item;
    if (leftItem === null)
        this.head = item;
    if (rightItem === null)
        this.tail = item;
    return item;
};

// Fetches the item with the given index in the list, skipping tombstones. Returns nil if the index is out of range.
// FIXME: O(n) complexity.
OrderedList.prototype.itemByIndex = function(index) {
    var item = this.head;
    while (item && (item.deleteTs || index > 0)) {
        if (item.deleteTs === null) {
            index--;
        }
        item = item.next;
    }
    return item;
};

OrderedList.prototype.addEventListener = function(listener) {
    this.eventListeners.add(listener);
}

OrderedList.prototype.removeEventListener = function(listener) {
    this.eventListeners.delete(listener);
}

OrderedList.prototype.onOperation = function(op) {
    for (var listener of this.eventListeners) {
        listener.onOperation(this, op);
    }
}
