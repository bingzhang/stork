package stork.module;

import stork.ad.*;
import java.net.URI;

// Represents a connection to a remote end-point. A session should
// provide methods for starting a transfer, listing directories, and
// performing other operations on the end-point.

public abstract class StorkSession {
  protected Ad config;
  protected URI url;
  protected AdSink sink = null;
  protected StorkSession pair = null;
  protected boolean closed = false;

  ////////////////////////////////////////////////////////////////
  // The following methods should be implemented by subclasses: //
  ////////////////////////////////////////////////////////////////

  // Get a directory listing of a path from the session. opts may be null.
  protected abstract Ad listImpl(String path, Ad opts);

  // Get the size of a file given by a path.
  protected abstract long sizeImpl(String path);

  // Create a directory at the end-point, as well as any parent directories.
  // Returns whether or not the command succeeded.
  //protected abstract boolean mkdirImpl(String path);

  // Transfer from this session to a paired session. opts can be null.
  protected abstract void transferImpl(String src, String dest, Ad opts);

  // Close the session and free any resources.
  protected abstract void closeImpl();

  // Create an identical session with the same settings. Can optionally
  // duplicate its pair as well and pair the duplicates.
  //public abstract StorkSession duplicate(boolean both);

  // Check if this session can be paired with another session. Override
  // this in subclasses.
  public boolean pairCheck(StorkSession other) {
    return true;
  }

  ////////////////////////////////////////////////////////////////////
  // Everything below this point should be more or less left alone. //
  ////////////////////////////////////////////////////////////////////

  // Create a session from a URL. Generally the path is ignored.
  public StorkSession(URI url, Ad config) {
    this.config = config;
    this.url = url;
  }

  // Get the current configuration of the session. The returned ad
  // should be the configuration used live by the session, so that
  // changes to the ad result in changes in the session behavior.
  public Ad config() {
    return config;
  }

  // Set an ad sink to write update ads into.
  // TODO: This is a temporary hack, remove me.
  public void setSink(AdSink sink) {
    this.sink = sink;
  }

  // Public interfaces to abstract methods.
  public Ad list(String path, Ad opts) {
    checkConnected();
    return listImpl(path, opts);
  }

  public long size(String path) {
    checkConnected();
    return sizeImpl(path);
  }

  public void transfer(String src, String dest, Ad opts) {
    checkConnected();
    if (pair == null)
      throw new Error("session is not paired");
    transferImpl(src, dest, opts);
  }

  // Check if the session hasn't been closed. Throws exception if so.
  private void checkConnected() {
    if (!isConnected())
      throw new Error("session has been closed");
  }

  // Check if the session is connected or if it's closed.
  public synchronized boolean isConnected() {
    return !closed;
  } public synchronized boolean isClosed() {
    return closed;
  }

  // Close the session, cancel and transfers, free any resources.
  public synchronized void close() {
    pair(null);
    closeImpl();
    closed = true;
  }

  // Pair this session with another session. Calls pairCheck() first
  // to see if the sessions are compatible. Returns paired session.
  public synchronized StorkSession pair(StorkSession other) {
    if (pair == other)
      throw new Error("these sessions are already paired");
    if (other == this)
      throw new Error("cannot pair a session with itself");
    if (other != null && (!pairCheck(other) || !other.pairCheck(this)))
      throw new Error("other session is not compatible with this session");
    if (other != null && other.pair != null)
      throw new Error("other session is already paired");
    if (other == null)
      return null;
    if (pair != null)
      pair.pair(null);
    if (other != null)
      other.pair = this;
    return pair = other;
  }

  // Unpair the session. Equivalent to calling pair(null).
  public synchronized void unpair() {
    pair(null);
  }

  // Get the paired session.
  public synchronized StorkSession pair() {
    return pair;
  }

  // Get a directory listing of a path from the session.
  public Ad list(String path) {
    return list(path, null);
  }

  // Transfer a file from this session to a paired session. Throws an
  // error if the session has not been paired. opts may be null.
  public void transfer(String src, String dest) {
    transfer(src, dest, null);
  }

  // Get the protocol used by the session.
  public String protocol() {
    return url.getScheme();
  }

  // Get the authority for the session.
  public String authority() {
    return url.getAuthority();
  }

  // Represent this session as a string.
  public String toString() {
    return protocol()+":"+authority();
  }
}
