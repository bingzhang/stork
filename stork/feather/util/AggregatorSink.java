package stork.feather.util;

import java.util.*;

import io.netty.buffer.*;

import stork.feather.*;

/**
 * A {@code Sink} which aggregates multiple, randomly-ordered {@code Slices}
 * into a single {@code Slice} which it rings a bell with on finalization.
 */
public class AggregatorSink extends Sink<AnonymousResource> {
  private Bell<Slice> bell = new Bell<Slice>();
  private List<ByteBuf> list = new LinkedList<ByteBuf>();

  public AggregatorSink() {
    super((AnonymousResource) Resource.ANONYMOUS);
  }

  public void drain(Slice slice) {
    list.add(slice.asByteBuf());
  }

  public void finish() {
    ByteBuf[] array = list.toArray(new ByteBuf[0]);
    ByteBuf buf = Unpooled.wrappedBuffer(array);
    bell.ring(new Slice(buf));
  }

  public Bell<Slice> bell() {
    return bell;
  }
}
