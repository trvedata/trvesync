"use strict";

function MapEntry(key, value) {
    this.key = key;
    this.value = value;
}

MapEntry.prototype.toString = function() {
    return this.key + "=" + this.value;
};

function HashMap() {
    this.map = new Map();
    this.allKeys = [];
}

HashMap.prototype.constructor = HashMap;

HashMap.prototype = {
        map : { writable: false, configurable: true, value: new Map() },
        allKeys : { writable: true, configurable: true, value: [] },

        set : function(key, value) {
            var hash = calculateHash(key);
            var list = this.map.get(hash);
            if (list) {
                for (var e of list) {
                    if (equal(e.key, key)) {
                        e.value = value;
                        return this;
                    }
                }
                list.push(new MapEntry(key, value));
            } else {
                this.map.set(hash, [new MapEntry(key, value)]);
            }
            this.allKeys.push(key);

            return this;
        },

        get : function(key) {
            var entry = this.getEntry(key);
            return entry ? entry.value : undefined;
        },

        getEntry : function(key) {
            var hash = calculateHash(key);
            var list = this.map.get(hash);
            if (list) {
                for (var e of list) {
                    if (equal(e.key, key)) {
                        return e;
                    }
                }
            }
            return undefined;
        },

        keys : function() {
            return this.allKeys[Symbol.iterator]();
        },

        size : {
            configurable: false,
            get: function() { return this.allKeys.length; }
        },
};

function stringHashCode(str) {
    var hash = 5381;
    for (var i = 0; i < str.length; i++) {
        hash = ((hash << 5) + hash) + str.charCodeAt(i);
    }
    return hash;
}

function calculateHash(o) {
    // TODO think about making this nicer
    if (o.hashCode)
        return o.hashCode();
    return stringHashCode(o.toString());
}

function equal(a, b) {
    if (a === null)
        return b === null;
    if (a.equals)
        return a.equals(b);
    return a === b;
}
