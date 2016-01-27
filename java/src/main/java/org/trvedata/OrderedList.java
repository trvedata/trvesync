package org.trvedata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class OrderedList<T> implements Iterable<T> {
	
	static class Item<T> {
		ItemID insertId;
		ItemID deleteTs;
		T value;
		Item<T> previous;
		Item<T> next;

		public Item(ItemID insertId, ItemID deleteTs, T value, Item<T> previous, Item<T> next) {
			this.insertId = insertId;
			this.deleteTs = deleteTs;
			this.value = value;
			this.previous = previous;
			this.next = next;
		}
	}
	
	private Peer<T> peer;
	private long lamportClock;
	private HashMap<ItemID, Item<T>> itemsById;
	private Item<T> head, tail;
	private Set<CRDTEventListener> eventListeners;

	public OrderedList(Peer<T> peer) {
		if (peer == null)
			throw new RuntimeException("OrderedList: peer must be set");
		this.peer = peer;
		this.lamportClock = 0;
		this.itemsById = new HashMap<ItemID, Item<T>>();
		this.head = null;
		this.tail = null;
		this.eventListeners = new HashSet<CRDTEventListener>();
	}

	// Iterates over the items in the list in order, skipping tombstones
	// iterator() {
	// var item = this.head;
	//
	// return {
	// next: function() {
	// while (item != null && item.deleteTs != null)
	// item = item.next;
	//
	// var ret = item;
	// if (ret == null)
	// return {done: true}
	// item = item.next;
	// return { value: ret.value, done: false}
	// }
	// }
	// }

	// eachItem(callback) {
	// var item = this.head;
	// while (item) {
	// callback(item);
	// item = item.next;
	// }
	// }
	// Inserts a new item at the given index in the list (local
	// operation). An index of 0 inserts at the head of the list;
	// index out of bounds appends at the end.
	public OrderedList<T> insert(int index, T value) {
		ItemID leftId = null;
		if (index > 0) {
			Item<T> rightItem = this.itemByIndex(index);
			leftId = rightItem != null ? rightItem.previous.insertId : this.tail.insertId;
		}
		Item<T> item = this.insertAfterId(leftId, this.peer.nextId(), value);
		InsertOp<T> op = new InsertOp<T>(item.previous == null ? null : item.previous.insertId, item.insertId, item.value);
		this.peer.sendOperation(op);
		return this;
	}

	// Inserts a new item before the existing item identified by cursorId
	// (local operation). If cursorId is nil, appends
	// to the end of the list.
	public ItemID insertBeforeId(ItemID cursorId, T value) {
		ItemID leftId;
		if (cursorId == null) {
			leftId = this.tail == null ? null : this.tail.insertId;
		} else {
			Item<T> rightItem = this.itemsById.get(cursorId);
			if (rightItem == null)
				throw new RuntimeException("insertBeforeId: unknown cursorId: " + cursorId);
			leftId = rightItem.previous == null ? null : rightItem.previous.insertId;
		}

		Item<T> item = this.insertAfterId(leftId, this.peer.nextId(), value);
		this.peer.sendOperation(new InsertOp<T>(item.previous == null ? null : item.previous.insertId, item.insertId, item.value));
		return item.insertId;
	}

	// Deletes the item at the given index in the list (local operation).
	public OrderedList<T> remove(int index) {
		Item<T> item = this.itemByIndex(index);
		if (item == null) {
			throw new RuntimeException("remove: unknown item with index " + index);
		}
		item.deleteTs = this.peer.nextId();
		item.value = null;
		this.peer.sendOperation(new DeleteOp(item.insertId, item.deleteTs));
		return this;
	}

	// Deletes numItems items from the list (local operation). The items
	// to be deleted are to the left of the item
	// identified by cursorId (not including the item identified by
	// cursorId itself). If cursorId is nil, deletes
	// numItems from the end of the list. Returns the ID of the last
	// non-deleted item before the sequence of deleted
	// items.
	public ItemID removeBeforeId(ItemID cursorId, int numItems) {
		Item<T> item, cursor;
		if (cursorId == null) {
			item = this.tail;
		} else {
			cursor = this.itemsById.get(cursorId);
			if (cursor == null)
				throw new RuntimeException("removeBeforeId: unknown cursorId: " + cursorId);
			item = cursor.previous;
		}

		while (item != null && (numItems > 0 || item.deleteTs != null)) {
			if (item.deleteTs == null) {
				item.deleteTs = this.peer.nextId();
				item.value = null;
				this.peer.sendOperation(new DeleteOp(item.insertId, item.deleteTs));
				numItems--;
			}

			item = item.previous;
		}

		return item == null ? null : item.insertId;
	}

	// Deletes numItems items from the list (local operation). The item
	public ItemID removeAfterId(ItemID cursorId, int numItems) {
		Item<T> item = this.itemsById.get(cursorId);
		if (item == null) {
			throw new RuntimeException("removeAfterId: unknown item with cursorId " + cursorId);
		}

		while (item != null && (numItems > 0 || item.deleteTs != null)) {
			if (item.deleteTs == null) {
				item.deleteTs = this.peer.nextId();
				item.value = null;
				this.peer.sendOperation(new DeleteOp(item.insertId, item.deleteTs));
				numItems--;
			}

			item = item.next;
		}

		return item == null ? null : item.insertId;
	}

	// Applies a remote operation to a local copy of the data structure. The
	// operation must be causally ready, as per the
	// data structure"s vector clock.
	void applyOperation(ChangingOperation operation) {
		if (operation instanceof InsertOp) {
			InsertOp<T> insertOp = (InsertOp<T>)operation;
			this.insertAfterId(insertOp.getReferenceId(), insertOp.getNewId(), insertOp.value);
		} else if (operation instanceof DeleteOp) {
			DeleteOp deleteOp = (DeleteOp)operation;
			Item<T> item = this.itemsById.get(deleteOp.getDeleteId());
			if (item == null)
				throw new RuntimeException("applyOperation: unknown item with id " + deleteOp.getDeleteId());
			item.deleteTs = deleteOp.getDeleteTs();
			item.value = null;
		} else {
			throw new RuntimeException("applyOperation: Invalid operation: " + operation);
		}

		this.onOperation(operation);
	}

	// Inserts a new list item to the right of the item identified by
	// leftId. If leftId is nil, inserts a new list item
	// at the head. The new item has Id insertId and the given value.
	// Returns the newly inserted item.
	Item<T> insertAfterId(ItemID leftId, ItemID insertId, T value) {
		Item<T> leftItem = null;
		if (leftId != null) {
			leftItem = this.itemsById.get(leftId);
			if (leftItem == null)
				throw new RuntimeException("insertAfterId: unknown item with id " + leftId);
		} else if (this.head != null && this.head.insertId.compareTo(insertId) > 0) {
			leftItem = this.head;
		}

		while (leftItem != null && leftItem.next != null && leftItem.next.insertId.compareTo(insertId) > 0) {
			leftItem = leftItem.next;
		}
		Item<T> rightItem = leftItem != null ? leftItem.next : this.head;

		Item<T> item = new Item<T>(insertId, null, value, leftItem, rightItem);
		this.itemsById.put(insertId, item);

		if (leftItem != null)
			leftItem.next = item;
		if (rightItem != null)
			rightItem.previous = item;
		if (leftItem == null)
			this.head = item;
		if (rightItem == null)
			this.tail = item;
		return item;
	}

	// Fetches the item with the given index in the list, skipping
	// tombstones. Returns nil if the index is out of range.
	// FIXME: O(n) complexity.
	Item<T> itemByIndex(int index) {
		Item<T> item = this.head;
		while (item != null && (item.deleteTs != null || index > 0)) {
			if (item.deleteTs == null) {
				index--;
			}
			item = item.next;
		}
		return item;
	}

	void addEventListener(CRDTEventListener listener) {
		this.eventListeners.add(listener);
	}

	void removeEventListener(CRDTEventListener listener) {
		this.eventListeners.remove(listener);
	}

	void onOperation(Operation op) {
		for (CRDTEventListener listener : this.eventListeners) {
			listener.onOperation(this, op);
		}
	}

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			Item<T> curr = head;
			
			@Override
			public boolean hasNext() {
				while (curr != null && curr.deleteTs != null)
					curr = curr.next;
				return curr != null;
			}

			@Override
			public T next() {
				hasNext();
				T res = curr.value;
				curr = curr.next;
				return res;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	public List<T> toList() {
		ArrayList<T> list = new ArrayList<T>();
		for (T val : this)
			list.add(val);
		return list;
	}
}