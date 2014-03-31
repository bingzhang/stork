package stork.feather;

/**
 * A handle on a remote resource, such as a file or directory. A {@code
 * Resource} should essentially be a wrapper around a URI, and its
 * instantiation should have no side-effects. All of the methods specified by
 * this interface should return immediately. In the case of methods that return
 * {@link Bell}s, the result should be determined asynchronously and given to
 * the caller through the returned {@code Bell}. Implementations that do not
 * support certain operations are allowed to throw an {@link
 * UnsupportedOperationException}.
 *
 * @see Session
 * @see Stat
 * @see URI
 */
public class Resource {
  protected final Session session;
  protected final URI uri;

  /**
   * The {@code Session} class calls this constructor.
   *
   * @param uri the URI referring to this resource.
   * @throws NullPointerException if {@code uri} is {@code null}.
   * @throws ClassCastException if the calling subclass is not a {@code
   * Session}.
   */
  protected Resource(URI uri) {
    if (uri == null)
      throw new NullPointerException();
    this.uri = uri.makeImmutable();
    this.session = ((Session) this);
  }

  /**
   * Create a resource with the same session and URI as another resource.
   *
   * @param resource the resource to make a copy of.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public Resource(Resource resource) {
    this.uri = resource.uri;
    this.session = resource.session;
  }

  /**
   * Create a resource wrapping the given URI and session.
   *
   * @param session the session through which this resource can be accessed.
   * @param uri the URI referring to this resource.
   * @throws NullPointerException if either {@code session} or {@code uri} are
   * {@code null}.
   */
  public Resource(Session session, URI uri) {
    if (session == null || uri == null)
      throw new NullPointerException();
    this.uri = uri.makeImmutable();
    this.session = session;
  }

  /**
   * Reselect this resource through an equivalent session. Assuming the
   * sessions are actually equivalent, the returned resource will point to the
   * same physical resource. This can, for instance, be used to select the same
   * resource through an already existing session.
   * <p/>
   * If this resource already belongs to the given session, this resource is
   * returned.
   *
   * @param session the session to reselect this resource on.
   * @return An equivalent resource which can be accessed through an equivalent
   * session.
   * @throws IllegalArgumentException if {@code session} is not equivalent to
   * this resource's session.
   * @throws NullPointerException if {@code session} is {@code null}.
   */
  public Resource reselect(Session session) {
    if (session == this.session)
      return this;
    if (!session.equals(this.session))
      throw new IllegalArgumentException();
    return session.select(uri);
  }

  /**
   * Get the {@link Session) associated with this resource.
   *
   * @return The {@link Session} associated with this resource.
   * @see Session
   */
  public Session session() { return session; }

  /**
   * Get the {@link URI} associated with this resource.
   * 
   * @return A {@link URI} which identifies this resource.
   */
  public URI uri() { return uri; }

  /**
   * Get metadata for this resource, which includes a list of subresources.
   *
   * @return (via bell) A {@link Stat} containing resource metadata.
   * @throws Exception (via bell) if there was an error retrieving
   * metadata for the resource
   * @throws UnsupportedOperationException if metadata retrieval is not
   * supported
   */
  public Bell<Stat> stat() { return session.stat(uri); }

  /**
   * Create this resource as a directory on the storage system. If the resource
   * cannot be created, or already exists and is not a directory, the returned
   * {@link Bell} will be resolved with an {@link Exception}.
   *
   * @return (via bell) {@code null} if successful.
   * @throws Exception (via bell) if the directory could not be created or
   * already exists and is not a directory.
   * @throws UnsupportedOperationException if creating directories is not
   * supported.
   */
  public Bell<Void> mkdir() { return session.mkdir(uri); }

  /**
   * Delete the resource and all subresources from the storage system. If the
   * resource cannot be removed, the returned {@link Bell} will be resolved
   * with an {@code Exception}.
   *
   * @return (via bell) {@code null} if successful.
   * @throws Exception (via bell) if the resource could not be fully removed.
   * @throws UnsupportedOperationException if removal is not supported.
   */
  public Bell<Void> rm() { return session.rm(uri); }

  /**
   * Select a subresource by name relative to this resource. By default, this
   * simply appends {@code name} to the URI of this resource. Subclasses may
   * override this to introduce special behavior.
   *
   * @param name the unescaped name of a subresource to select.
   * @return A subresource of this resource.
   */
  public Resource select(String name) {
    return new Resource(uri.appendSegment(name));
  }

  /**
   * Select a subresource relative to this resource using the given path.
   *
   * @param path the path to the subresource, relative to this resource.
   * @return A subresource relative to this resource.
   */
  public final Resource select(Path path) {
    Resource r = this;
    for (String n : path.explode())
      r = r.select(n);
    return r;
  }

  /**
   * Initiate a transfer from this {@code Resource} to {@code resource} using
   * whatever method is deemed most appropriate by the implementation.
   * The implementation should try to transfer the resource as efficiently as
   * possible, and so should inspect the destination resource to determine if
   * more efficient alternatives to proxy transferring can be done. This method
   * should perform a proxy transfer as a catch-all last resort.
   *
   * @param resource the destination resource to transfer this resource to
   * @return (via bell) A {@link Transfer} on success; the returned {@code
   * Transfer} object can be used to control and monitor the transfer.
   * @throws UnsupportedOperationException if the direction of transfer is not
   * supported by one of the resources.
   * @throws NullPointerException if {@code resouce} is {@code null}.
   */
  public Bell<Transfer> transferTo(Resource resource) {
    return session.transfer(this, resource);
  }

  /**
   * Open a sink to the resource. Any connection operation, if necessary,
   * should begin as soon as this method is called. The returned bell should be
   * rung once the sink is ready to accept data.
   *
   * @return (via bell) A sink which drains to the named resource.
   * @throws Exception (via bell) if opening the sink fails.
   * @throws UnsupportedOperationException if the resource does not support
   * writing.
   */
  public Bell<Sink> sink() { return session.sink(uri); }

  /**
   * Open a tap on the resource. Any connection operation, if necessary, should
   * begin, as soon as this method is called. The returned bell should be rung
   * once the tap is ready to emit data.
   *
   * @return (via bell) A tap which emits slices from this resource and its
   * subresources.
   * @throws Exception (via bell) if opening the tap fails.
   * @throws UnsupportedOperationException if the resource does not support
   * reading.
   */
  public Bell<Tap> tap() { return session.tap(uri); }

  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Resource)) return false;
    Resource r = (Resource) o;
    if (!uri.equals(r.uri))
      return false;
    return true;
  }

  public int hashCode() {
    return 1 + 13*uri.hashCode() +
           (credential != null ? 17*credential.hashCode() : 0);
  }
}
