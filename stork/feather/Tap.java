package stork.feather;

/**
 * A tap emits {@link Slice}s to an attached {@link Sink}. The tap should
 * provide methods for regulating the flow of data (see {@link #pause()} and
 * {@link #resume()}) to allow attached sinks to prevent themselves from being
 * overwhelmed.
 *
 * @see Resource
 * @see Sink
 * @see Slice
 */
public abstract class Tap implements ProxyEnd {
  private ProxyTransfer transfer;

  /** The root resource of this tap. */
  public final Resource resource;

  /**
   * Create a {@code Tap} with the given {@code Resource} as the root resource.
   *
   * @param resource the {@code Resource} this {@code Tap} emits data from.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public Tap(Resource resource) {
    if (resource == null)
      throw new NullPointerException();
    this.resource = resource;
  }

  /**
   * Attach this tap to a sink. Once this method is called, {@link #start()}
   * will be called and the tap may begin reading data from the upstream
   * channel and emitting {@link Slice}s.
   *
   * @param sink a {@link Sink} to attach.
   * @throws NullPointerException if {@code sink} is {@code null}.
   * @throws IllegalStateException is a sink has already been attached.
   */
  public final synchronized ProxyTransfer attach(Sink sink) {
    if (sink == null)
      throw new NullPointerException();
    if (transfer != null)
      throw new IllegalStateException("a sink is already attached");
    transfer = new ProxyTransfer().tap(this).sink(sink);
  }

  // Get the transfer, or throw an IllegalStateException if the transfer is not
  // ready.
  private final void transfer() {
    if (transfer == null)
      throw new IllegalStateException();
    return transfer;
  }

  public final synchronized Bell<?> initialize(Path path) {
    return transfer().initialize(path);
  }

  public final synchronized void drain(Path path, Slice slice) {
    transfer().drain(path, slice);
  }

  public final Bell<?> finalize(Path path) {
    transfer().finalize(path);
  }

  public boolean random() { return false; }

  public int concurrency() { return 1; }
}
