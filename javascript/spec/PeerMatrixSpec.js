
describe("PeerMatrix", function() {
    "use strict";

    beforeEach(function() {

    });

//    function testRemaingMessagesCount(maxCount) {
//        var peers = Array.prototype.slice.call(arguments, 1);
//        var moreMessages = true;
//        var msgCount = 0;
//        while (moreMessages) {
//            moreMessages = false;
//            for (var peer of peers) {
//                if (peer.anythingToSend()) {
//                    var message = peer.makeMessage();
//                    moreMessages = true;
//                    msgCount++;
//                    if (msgCount > maxCount)
//                        return false;
//                    for (var recipient of peers) {
//                        if (recipient !== peer)
//                            recipient.processMessage(message);
//                    }
//                }
//            }
//        }
//
//        return true;
//    };


    it('should assign sequential message numbers', function() {
        var peer = new Peer();
        peer.orderedList.insert(0, 'a');
        expect(peer.makeMessage().msgCount).toBe(1);
        peer.orderedList.insert(1, 'b');
        expect(peer.makeMessage().msgCount).toBe(2);
        peer.orderedList.insert(2, 'c').remove(0);
        expect(peer.makeMessage().msgCount).toBe(3);
    });

    it('should assign peer indexes in the order they are seen', function() {
        var local = new Peer();
        var otherPeerIds = ['a', 'b', 'c'].map(function(letter) {
            var remote = new Peer();
            remote.orderedList.insert(0, letter);
            local.processMessage(remote.makeMessage());
            return remote.peerId;
        });

        expect(local.peerMatrix.peerIdToIndex(local.peerId)).toBe(0);
        expect(local.peerMatrix.peerIdToIndex(otherPeerIds[0])).toBe(1);
        expect(local.peerMatrix.peerIdToIndex(otherPeerIds[1])).toBe(2);
        expect(local.peerMatrix.peerIdToIndex(otherPeerIds[2])).toBe(3);
    });

    it('should generate clock update operations when messages are received', function() {
        var local = new Peer();
        var remote1 = new Peer();
        var remote2 = new Peer();
        remote1.orderedList.insert(0, 'a').insert(1, 'b');
        remote2.orderedList.insert(0, 'z');
        local.processMessage(remote1.makeMessage());
        local.processMessage(remote2.makeMessage());
        remote1.orderedList.insert(2, 'c');
        local.processMessage(remote1.makeMessage());

        var messageOperations = local.makeMessage().operations;
        expect(messageOperations.length).not.toBe(0);
        var clockUpdate = messageOperations[0];
        expect(clockUpdate instanceof ClockUpdate).toBe(true);
        expect(clockUpdate.entries()).toEqual([
                                               new PeerVClockEntry(remote1.peerId, 1, 2),
                                               new PeerVClockEntry(remote2.peerId, 2, 1)
                                               ]);
    });

    it('should include the peer ID only on the first clock update', function() {
        var local = new Peer();
        var remote1 = new Peer();
        remote1.orderedList.insert(0, 'a');
        local.processMessage(remote1.makeMessage());
        expect(local.makeMessage().operations[0].entries()).toEqual([new PeerVClockEntry(remote1.peerId, 1, 1)]);

        var remote2 = new Peer();
        remote2.orderedList.insert(0, 'a');
        local.processMessage(remote2.makeMessage());
        expect(local.makeMessage().operations[0].entries()).toEqual([new PeerVClockEntry(remote2.peerId, 2, 1)]);

        remote1.orderedList.insert(0, 'a');
        local.processMessage(remote1.makeMessage());
        expect(local.makeMessage().operations[0].entries()).toEqual([new PeerVClockEntry(null, 1, 2)]);
    });

    it('should decode remote peer indexes', function() {
        var peer1 = new Peer();
        var peer2 = new Peer();
        var peer3 = new Peer();
        peer1.orderedList.insert(0, 'a');
        var msg1 = peer1.makeMessage();

        peer2.processMessage(msg1);
        peer2.orderedList.insert(1, 'b');
        var msg2 = peer2.makeMessage();

        peer3.processMessage(msg2);
        peer3.processMessage(msg1);
        peer3.orderedList.insert(2, 'c');
        var msg3 = peer3.makeMessage();

        peer1.processMessage(msg2);
        peer1.processMessage(msg3);
        peer2.processMessage(msg3);
        peer1.processMessage(peer2.makeMessage());

        expect(peer1.peerMatrix.remoteIndexToPeerId(peer2.peerId, 0)).toEqual(peer2.peerId);
        expect(peer1.peerMatrix.remoteIndexToPeerId(peer2.peerId, 1)).toEqual(peer1.peerId);
        expect(peer1.peerMatrix.remoteIndexToPeerId(peer2.peerId, 2)).toEqual(peer3.peerId);

        expect(peer1.peerMatrix.remoteIndexToPeerId(peer3.peerId, 0)).toEqual(peer3.peerId);
        expect(peer1.peerMatrix.remoteIndexToPeerId(peer3.peerId, 1)).toEqual(peer2.peerId);
        expect(peer1.peerMatrix.remoteIndexToPeerId(peer3.peerId, 2)).toEqual(peer1.peerId);

        expect(peer2.peerMatrix.remoteIndexToPeerId(peer1.peerId, 0)).toEqual(peer1.peerId);
        expect(peer2.peerMatrix.remoteIndexToPeerId(peer3.peerId, 0)).toEqual(peer3.peerId);
        expect(peer2.peerMatrix.remoteIndexToPeerId(peer3.peerId, 1)).toEqual(peer2.peerId);
        expect(peer2.peerMatrix.remoteIndexToPeerId(peer3.peerId, 2)).toEqual(peer1.peerId);

        expect(peer3.peerMatrix.remoteIndexToPeerId(peer1.peerId, 0)).toEqual(peer1.peerId);
        expect(peer3.peerMatrix.remoteIndexToPeerId(peer2.peerId, 0)).toEqual(peer2.peerId);
        expect(peer3.peerMatrix.remoteIndexToPeerId(peer2.peerId, 1)).toEqual(peer1.peerId);
    });

    it('should track causal dependencies across peers', function() {
        var peer1 = new Peer();
        var peer2 = new Peer();
        var peer3 = new Peer();

        peer1.orderedList.insert(0, 'a');
        var msg1 = peer1.makeMessage();

        peer2.processMessage(msg1);
        peer2.orderedList.insert(1, 'b');
        var msg2 = peer2.makeMessage();

        peer3.processMessage(msg2);
        expect(peer3.peerMatrix.isCausallyReady(peer2.peerId)).toBe(false);
        peer3.processMessage(msg1);
        expect(peer3.peerMatrix.isCausallyReady(peer2.peerId)).toBe(true);
        expect(peer3.peerMatrix.isCausallyReady(peer1.peerId)).toBe(true);
    });


    it('should not send messages indefinitely after no more changes occur', function() {
        var peer1 = new Peer();
        var peer2 = new Peer();

        peer1.orderedList.insert(0, 'a');

        for (var i = 0; i < 5; i++) {  // 5 rounds should more than sufficient for peers to get into a stable state
            if (!(peer1.anythingToSend() || peer2.anythingToSend()))
                break;

            if (peer1.anythingToSend())
                peer2.processMessage(peer1.makeMessage());

            if (peer2.anythingToSend())
                peer1.processMessage(peer2.makeMessage());
        }

        expect(peer1.anythingToSend() || peer2.anythingToSend()).toBe(false);
    });

});