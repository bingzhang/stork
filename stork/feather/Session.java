package stork.feather;

import java.io.*;

/**
 * A session represents a connection to a remote endpoint and all associated
 * configuration and state. {@code Session}s are stateful and, once opened,
 * represent an established connection to an endpoint.
 * <p/>
 * Implementors should override all {@code protected} methods prefixed with
 * {@code do}. Clients will interact with the session using the {@code public}
 * wrappers around these methods which include checks that make the following
 * assurances:
 * <ol><li>
 *   Clients may call {@link #open()} multiple times, with subsequent calls
 *   having no effect and always returning a reference to the same bell.
 *   Implementors should assume {@link #doOpen()} will only ever be called
 *   once.
 * </li><li>
 *   Clients may perform operations through the session without explicitly
 *   calling {@link #open()}. Implementors should assume {@link #open()} will
 *   have already been called before any operations begin.
 * </li><li>
 *   Clients may call {@link #close()} multiple times, with subsequent calls
 *   having no effect. Implementors should assume {@link #doClose()} will be
 *   called at most once, the first time {@link #close()} is called.
 * </li></ol>
 */
public abstract class Session extends Resource {
  /** The authentication factor used for this endpoint. */
  protected final Credential credential;

  private Bell<Void> openBell = null;

  // Ring this with a IOException when it's time to close.
  private final Bell<Object> doCloseBell = new Bell<Object>() {
    public void always() { cleanup(); }
  };

  /**
   * Create a session with the given root URI.
   *
   * @param uri a {@link URI} representing the root of the session.
   */
  public Session(URI uri) {
    this(uri, null);
  }

  /**
   * Create a session with the given root URI and credential.
   *
   * @param uri a {@link URI} representing the root of the session.
   * @param credential a {@link Credential} used to authenticate with the
   * endpoint. May be {@code null} if no additional authentication factors are
   * required.
   * @throws NullPointerException if {@code uri} is {@code null}.
   */
  public Session(URI uri, Credential credential) {
    super(uri);
    this.credential = credential;
  }

  // Constructor used solely by SessionDecorator.
  private Session(Session other) {
    this(other.uri, other.credential);
  }

  /**
   * Subclasses should override this to get metadata for the given URI, which
   * includes a list of subresources. By default, this throws {@code
   * UnsupportedOperationException}.
   *
   * @param resource the resource to stat.
   * @return (via bell) A {@link Stat} containing resource metadata.
   * @throws Exception (via bell) if there was an error retrieving
   * metadata for the resource.
   * @throws UnsupportedOperationException if metadata retrieval is not
   * supported.
   */
  protected Bell<Stat> doStat(Resource resource) {
    throw new UnsupportedOperationException();
  }

  /**
   * Subclasses should override this to create the resource specified by the
   * given Resource as a directory. If the resource cannot be created, or already
   * exists and is not a directory, the returned {@link Bell} will be resolved
   * with an {@link Exception}. By default, this throws {@code
   * UnsupportedOperationException}.
   *
   * @param resource the resource to create as a directory.
   * @return (via bell) {@code null} if successful.
   * @throws Exception (via bell) if the directory could not be created or
   * already exists and is not a directory.
   * @throws UnsupportedOperationException if creating directories is not
   * supported.
   */
  protected Bell<Void> doMkdir(Resource resource) {
    throw new UnsupportedOperationException();
  }

  /**
   * Subclasses should override this to delete the resource specified by a Resource
   * and all subresources from the storage system. If the resource cannot be
   * removed, the returned {@link Bell} will be resolved with a {@code
   * Exception}. By default, this throws {@code UnsupportedOperationException}.
   *
   * @param resource the Resource of the resource to remove.
   * @return (via bell) {@code null} if successful.
   * @throws Exception (via bell) if the resource could not be fully removed.
   * @throws UnsupportedOperationException if removal is not supported.
   */
  protected Bell<Void> doRm(Resource resource) {
    throw new UnsupportedOperationException();
  }

  /**
   * Initiate a transfer from {@code source} to {@code destination} using
   * whatever method is most efficient. {@code source} is guaranteed to have
   * been selected through this session. The implementation should try to
   * transfer the resource as efficiently as possible, and so should inspect
   * {@code destination} to determine if more efficient alternatives to proxy
   * transferring can be done. This method should perform a proxy transfer only
   * as a catch-all last resort. By default, this method always uses proxy
   * transfer.
   *
   * @param source the resource to transfer to {@code destination}.
   * @param destination the resource to transfer {@code source} to.
   * @return (via bell) A {@link Transfer} on success; the returned {@code
   * Transfer} object can be used to control and monitor the transfer.
   * @throws IllegalArgumentException if {@code source} does not belong to this
   * session.
   * @throws UnsupportedOperationException if the direction of transfer is not
   * supported by one of the resources.
   */
  protected Bell<Transfer> doTransfer(Resource source, Resource destination) {
    return new ProxyTransfer(tap(), resource.sink());
  }

  /**
   * Subclasses should override this to open a sink to the resource at the
   * specified Resource. Any setup operation, if necessary, should begin as soon as
   * this method is called. The returned bell should be rung once the sink is
   * ready to accept data. By default, this throws {@code
   * UnsupportedOperationException}.
   *
   * @param resource the Resource of the resource to open a sink to.
   * @return (via bell) A sink which drains to the named resource.
   * @throws ResourceException (via bell) if opening the sink fails
   * @throws UnsupportedOperationException if the resource does not support
   * writing
   */
  protected Bell<Sink> doSink(Resource resource) {
    throw new UnsupportedOperationException();
  }

  /**
   * Subclasses should override this to open a tap on the resource at the
   * specified Resource. Any setup operation, if necessary, should begin, as soon as
   * this method is called. The returned bell should be rung once the tap is
   * ready to emit data. By default, this throws {@code
   * UnsupportedOperationException}.
   *
   * @param resource the Resource of the resource to open a tap on.
   * @return (via bell) A tap which emits slices from this resource and its
   * subresources.
   * @throws ResourceException (via bell) if opening the tap fails
   * @throws UnsupportedOperationException if the resource does not support
   * reading
   */
  protected Bell<Tap> doTap(Resource resource) {
    throw new UnsupportedOperationException();
  }

  /**
   * Subclasses should override this to begin the connection and authentication
   * procedure with the endpoint system. This method will be called at most
   * once.
   *
   * @return (via bell) {@code null}, once opening has completed. If this bell
   * is cancelled before the session has been established, the opening
   * procedure should be terminated.
   */
  protected Bell<Void> doOpen() { return new Bell<Void>().ring(); }

  /**
   * Subclasses should override this to start the cleanup procedure during
   * closing. Cleanup should be performed asynchronously and this method should
   * return immediately.
   */
  protected void doClose() { }

  // Everything below here is for decoration...

  // This really means "opening has definitely completed". This may return true
  // even if the session is closed, and opening may be in process even if this
  // returns false.
  private synchronized boolean isOpen() {
    return openBell != null && openBell.isDone();
  }

  // Simple helper class so we don't have TOO much boilerplate for the public
  // decorator methods. Basically proxies for another bell from an operation
  // called after open is complete.
  private abstract class OnOpen<T> extends Bell<T> {
    OnOpen() {
      open().promise(new Bell<Void>() {
        protected void done() {
          onClose(task().promise(OnOpen.this));
        } protected void fail(Throwable t) {
          OnOpen.this.ring(t);
        }
      });
    } abstract Bell<T> task();
  }

  /**
   * Get metadata for the given resource, which includes a list of
   * subresources. If the session is not open, it will be opened.
   *
   * @param resource the Resource of the resource to stat.
   * @return (via bell) A {@link Stat} containing resource metadata.
   * @throws Exception (via bell) if there was an error retrieving metadata for
   * the resource.
   * @throws UnsupportedOperationException if metadata retrieval is not
   * supported.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public final synchronized Bell<Stat> stat(Resource resource) {
    final Resource r = resource.reselect(this);
    if (isClosed()) return (Bell<Stat>) onClose();
    if (isOpen())   return onClose(doStat(r));
    return new OnOpen<Stat>() {
      Bell<Stat> task() { return doStat(r); }
    };
  }

  /**
   * Create the specified resource as a directory. If the resource cannot be
   * created, or already exists and is not a directory, the returned {@link
   * Bell} will be resolved with an {@link Exception}. If the session is not
   * open, it will be opened.
   *
   * @param resource the Resource of the resource to create as a directory.
   * @return (via bell) {@code null} if successful.
   * @throws Exception (via bell) if the directory could not be created or
   * already exists and is not a directory.
   * @throws UnsupportedOperationException if creating directories is not
   * supported.
   * @throws NullPointerException if {@code resource} is {@code null}.
   * @throws IllegalArgumentException if {@code resource} cannot be accessed by
   * this session.
   */
  public final synchronized Bell<Void> mkdir(Resource resource) {
    final Resource r = resource.reselect(this);
    if (resource == null)
      throw new NullPointerException();
    if (isClosed()) return (Bell<Void>) onClose();
    if (isOpen())   return onClose(doMkdir(r));
    return new OnOpen<Void>() {
      Bell<Void> task() { return doMkdir(r); }
    };
  }

  /**
   * Delete the specified resource and all subresources from the storage
   * system. If the resource cannot be removed, the returned {@link Bell} will
   * be resolved with a {@code Exception}. If the session is not open, it will
   * be opened.
   *
   * @param resource the Resource of the resource to remove.
   * @return (via bell) {@code null} if successful.
   * @throws Exception (via bell) if the resource could not be fully removed.
   * @throws UnsupportedOperationException if removal is not supported.
   * @throws NullPointerException if {@code resource} is {@code null}.
   * @throws IllegalArgumentException if {@code resource} cannot be accessed by
   * this session.
   */
  public final synchronized Bell<Void> rm(Resource resource) {
    final Resource r = resource.reselect(this);
    if (isClosed()) return (Bell<Void>) onClose();
    if (isOpen())   return onClose(doRm(r));
    return new OnOpen<Void>() {
      Bell<Void> task() { return doRm(r); }
    };
  }

  /**
   * Open a sink to the specified resource. Any connection operation, if
   * necessary, should begin as soon as this method is called. The returned
   * bell should be rung once the sink is ready to accept data. If the session
   * is not open, it will be opened.
   *
   * @param resource the resource to open a sink to.
   * @return (via bell) A sink which drains to the named resource.
   * @throws Exception (via bell) if opening the sink fails.
   * @throws UnsupportedOperationException if the resource does not support
   * writing.
   * @throws NullPointerException if {@code resource} is {@code null}.
   * @throws IllegalArgumentException if {@code resource} cannot be accessed by
   * this session.
   */
  public final synchronized Bell<Sink> sink(Resource resource) {
    final Resource r = resource.reselect(this);
    if (isClosed()) return (Bell<Sink>) onClose();
    if (isOpen())   return onClose(doSink(r));
    return new OnOpen<Sink>() {
      Bell<Sink> task() { return doSink(r); }
    };
  }

  /**
   * Open a tap on the specified resource. Any connection operation, if
   * necessary, should begin, as soon as this method is called. The returned
   * bell should be rung once the tap is ready to emit data. If the session is
   * not open, it will be opened.
   *
   * @param resource the resource to open a tap on.
   * @return (via bell) A tap which emits slices from this resource and its
   * subresources.
   * @throws Exception (via bell) if opening the tap fails.
   * @throws UnsupportedOperationException if the resource does not support
   * reading.
   * @throws NullPointerException if {@code resource} is {@code null}.
   * @throws IllegalArgumentException if {@code resource} cannot be accessed by
   * this session.
   */
  public final synchronized Bell<Tap> tap(Resource resource) {
    final Resource r = resource.reselect(this);
    if (isClosed()) return (Bell<Tap>) onClose();
    if (isOpen())   return onClose(doTap(r));
    return new OnOpen<Tap>() {
      Bell<Tap> task() { return doTap(r); }
    };
  }

  /**
   * Initiate a transfer from {@code source} to {@code destination}. If {@code
   * source} does not belong to this session and cannot be reselected to this
   * session, {@code IllegalArgumentException} will be thrown.
   *
   * @param source the resource to transfer to {@code destination}.
   * @param destination the resource to transfer {@code source} to.
   * @return (via bell) A {@link Transfer} on success; the returned {@code
   * Transfer} object can be used to control and monitor the transfer.
   * @throws IllegalArgumentException if {@code source} does not belong to this
   * session.
   * @throws UnsupportedOperationException if the direction of transfer is not
   * supported by one of the resources.
   * @throws NullPointerException if {@code source} or {@code destination} is
   * {@code null}.
   */
  public final synchronized Bell<Transfer> transfer(
      Resource source, final Resource destination)
  {
    if (source == null || destination == null)
      throw new NullPointerException();
    final Resource src = source.reselect(this);
    if (isClosed()) return (Bell<Transfer>) onClose();
    if (isOpen())   return onClose(doTransfer(src, destination));
    return new OnOpen<Transfer>() {
      Bell<Transfer> task() { return doTransfer(src, destination); }
    };
  }

  /**
   * Begin the connection and authentication procedure with the endpoint
   * system. Subsequent calls to this method will return the same bell.
   *
   * @return (via bell) {@code null}, once opening has completed. If this bell
   * is cancelled before the session has been established, the opening
   * procedure should be terminated.
   */
  public final synchronized Bell<Void> open() {
    if (openBell != null)
      return openBell;
    openBell = open();
    if (openBell == null)
      openBell = new Bell<Void>().ring();
    return openBell;
  }

  /**
   * Select a {@link Resource} from the session, based on a URI.
   *
   * @param uri the URI of the resource to be selected.
   * @return The {@link Resource} identified by the URI.
   * @throws IllegalArgumentException if {@code uri} cannot be opened by this
   * session.
   */
  public final Resource select(URI uri) {
    if (uri == null)
      return this;
    return new Resource(this, uri);
  }

  /**
   * Inform the session that it is no longer needed and should close any open
   * resources. This will also cancel the opening procedure if it has begun.
   */
  public final void close() { close(null, null); }

  /**
   * Inform the session that it is no longer needed and should close any open
   * resources, using {@code reason} as the explanation for closing. This will
   * also cancel the opening procedure if it has begun.
   *
   * @param reason the reason for closing this session.
   */
  public final void close(String reason) { close(reason, null); }

  /**
   * Inform the session that it is no longer needed and should close any open
   * resources, using {@code cause} as the cause for closing. This will
   * also cancel the opening procedure if it has begun.
   *
   * @param cause the exception that caused this session to close.
   */
  public final void close(Throwable cause) { close(null, cause); }

  /**
   * Inform the session that it is no longer needed and should close any open
   * resources, using {@code reason} as the explanation for closing and {@code
   * cause} as the cause for closing. This will also cancel the opening
   * procedure if it has begun.
   *
   * @param reason the reason for closing this session.
   * @param cause the exception that caused this session to close.
   */
  public final void close(String reason, Throwable cause) {
    doCloseBell.ring(new IOException(reason, cause));
  }

  /**
   * Check if the closing procedure has begun.
   *
   * @return {@code true} if closing has begun; {@code false} otherwise.
   */
  public final synchronized boolean isClosed() {
    return doCloseBell.isDone();
  }

  /**
   * Return a bell that will be rung with a {@code IOException} when
   * the session is closed. This is the same is calling {@code onClose(new
   * Bell<T>())}.
   *
   * @return The value passed in for {@code bell}.
   * @throws IOException (via bell) when the session has begun closing.
   */
  public final Bell<?> onClose() {
    return onClose(new Bell());
  }

  /**
   * Register a bell to be rung with a {@code IOException} when the
   * session has begun its closing procedure.
   *
   * @return The value passed in for {@code bell}.
   * @param bell the bell to ring with a {@code IOException} when
   * the session is closed.
   * @throws IOException (via {@code bell}) when the session has
   * begun closing.
   */
  public final <T> Bell<T> onClose(Bell<T> bell) {
    final Bell<Object> b = (Bell<Object>) bell;
    return (b != null) ? (Bell<T>) doCloseBell.promise(b) : null;
  }

  /**
   * If all references to this session have been lost, begin the closing
   * procedure.
   */
  protected final void finalize() { close(); }

  public String toString() {
    return uri.toString();
  }

  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Session)) return false;
    Session s = (Session) o;
    if (!uri.equals(s.uri))
      return false;
    if (credential == null)
      return s.credential == null;
    return credential.equals(s.credential);
  }

  public int hashCode() {
    return 1 + 13*uri.hashCode() +
           (credential != null ? 17*credential.hashCode() : 0);
  }
}
