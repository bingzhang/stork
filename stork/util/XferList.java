package stork.util;

import java.util.*;

// A list of files and directories to transfer.

public class XferList implements Iterable<XferList.Entry> {
  private LinkedList<Entry> list = new LinkedList<Entry>();
  private long size = 0;
  private int count = 0;  // Number of files (not dirs)
  public String sp, dp;

  // Create an XferList for a directory.
  public XferList(String src, String dest) {
    if (!src.endsWith("/"))  src  += "/";
    if (!dest.endsWith("/")) dest += "/";
    sp = src;
    dp = dest;
  }

  // Create an XferList for a file.
  public XferList(String src, String dest, long size) {
    sp = src;
    dp = dest;
    add("", size);
  }

  // An entry (file or directory) in the list.
  public class Entry {
    private String path;
    public final boolean dir;
    public boolean done = false;
    public final long size;
    public long off = 0, len = -1;  // Start/stop offsets.

    // Create a directory entry.
    Entry(String path) {
      if (path == null)
        path = "";
      else if (!path.endsWith("/"))
        path += "/";
      path = path.replaceFirst("^/+", "");
      this.path = path;
      size = off = 0;
      dir = true;
    }

    // Create a file entry.
    Entry(String path, long size) {
      this.path = path;
      this.size = (size < 0) ? 0 : size;
      off = 0;
      dir = false;
    }

    // Create an entry based off another entry with a new path.
    Entry(String path, Entry e) {
      this.path = path;
      dir = e.dir; done = e.done;
      size = e.size; off = e.off; len = e.len;
    }

    public long remaining() {
      if (done || dir) return 0;
      if (len == -1 || len > size)
        return size - off;
      return len - off;
    }

    public String path() {
      return XferList.this.sp + path;
    }

    public String dpath() {
      return XferList.this.dp + path;
    }
  }

  // Add a directory to the list.
  public void add(String path) {
    list.add(new Entry(path));
  }

  // Add a file to the list.
  public void add(String path, long size) {
    list.add(new Entry(path, size));
    this.size += size;
    this.count++;
  }

  // Add another XferList's entries under this XferList.
  public void addAll(XferList ol) {
    size += ol.size;
    count += ol.count;
    list.addAll(ol.list);
  }

  public long size() {
    return size;
  } 

  public int count() {
    return count;
  }

  public boolean isEmpty() {
    return list.isEmpty();
  }

  // Remove and return the topmost entry.
  public Entry pop() {
    Entry e = list.pop();
    if (e != null)
      size -= e.size;
    return e;
  }

  // Get the progress of the list in terms of bytes.
  public Progress byteProgress() {
    Progress p = new Progress();

    for (Entry e : list)
      p.add(e.remaining(), e.size);
    return p;
  } 

  // Get the progress of the list in terms of files.
  public Progress fileProgress() {
    Progress p = new Progress();

    for (Entry e : list)
      p.add(e.done?1:0, 1);
    return p;
  }

  // Split off a sublist from the front of this list which is
  // of a certain byte length.
  public XferList split(long len) {
    XferList nl = new XferList(sp, dp);
    Iterator<Entry> iter = iterator();

    if (len == -1 || size <= len) {
      // If the request is bigger than the list, empty into new list.
      nl.list = list;
      nl.size = size;
      list = new LinkedList<Entry>();
      size = 0;
    } else while (len > 0 && iter.hasNext()) {
      Entry e2, e = iter.next();
      
      if (e.done) {
        iter.remove();
      } else if (e.dir || e.remaining() <= len) {
        nl.list.add(e);
        nl.size += e.remaining();
        len -= e.remaining();
      } else {  // Need to split file...
        e2 = new Entry(e.path, e);
        e2.len = len;
        nl.list.add(e2);
        nl.size += len;

        if (e.len == -1)
          e.len = e.size;
        e.len -= len;
        e.off += len;
        len = 0;
      }
    } return nl;
  }

  public Iterator<Entry> iterator() {
    return list.iterator();
  }
}
