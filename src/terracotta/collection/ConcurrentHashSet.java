package terracotta.collection;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashSet<E> extends AbstractSet<E> implements Set<E>, Serializable {
	
	private static final long serialVersionUID = 1L;
	private final Map<E, Boolean> m = new ConcurrentHashMap<E, Boolean>();

    public void clear() {
        m.clear();
    }

    public int size() {
        return m.size();
    }

    public boolean isEmpty() {
        return m.isEmpty();
    }

    public boolean contains(Object o) {
        return m.containsKey(o);
    }

    public boolean remove(Object o) {
        return m.remove(o) != null;
    }

    public boolean add(E e) {
        return m.put(e, Boolean.TRUE) == null;
    }

    public Iterator<E> iterator() {
        return m.keySet().iterator();
    }

    public Object[] toArray() {
        return m.keySet().toArray();
    }

    public <T> T[] toArray(T[] a) {
        return m.keySet().toArray(a);
    }

    public String toString() {
        return m.keySet().toString();
    }

    public int hashCode() {
        return m.keySet().hashCode();
    }

    public boolean equals(Object o) {
        return o == this || m.keySet().equals(o);
    }

    public boolean containsAll(Collection<?> c) {
        return m.keySet().containsAll(c);
    }

    public boolean removeAll(Collection<?> c) {
        return m.keySet().removeAll(c);
    }

    public boolean retainAll(Collection<?> c) {
        return m.keySet().retainAll(c);
    }
}