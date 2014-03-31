package stork.net;

import io.netty.buffer.*;
import io.netty.handler.codec.*;
import io.netty.channel.*;
import io.netty.channel.socket.*;

import java.net.*;

import stork.*;
import stork.ad.*;
import stork.feather.URI;
import stork.scheduler.*;

// Basic TCP interface.

public class TCPInterface extends StorkInterface {
  public TCPInterface(Scheduler s, URI uri) {
    super(s, uri);
  }

  public String name() { return "TCP"; }

  public void init(ServerChannel channel) {
    channel.pipeline().addLast(new AdCodec());
  }

  public int port(URI uri) {
    return 57024;
  }
}
