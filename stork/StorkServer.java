package stork;

import stork.ad.*;
import stork.net.*;
import stork.util.*;
import stork.scheduler.*;
import java.net.URI;

// This is basically a standalone wrapper around Scheduler that lets
// the scheduler run as a standalone process (as opposed to as a client
// of another JVM). This will create server interfaces for the scheduler.

public class StorkServer extends Command {
  public StorkServer() {
    super("server");

    args = new String[] { "[option]..." };
    desc = new String[] {
      "The Stork server is the core of the Stork system, handling "+
        "connections from clients and scheduling transfers. This command "+
        "is used to start a Stork server.",
        "Upon startup, the Stork server loads stork.conf and begins "+
          "listening for clients."
    };
    add('d', "daemonize", "run the server in the background, "+
      "redirecting output to a log file (if specified)");
    add('l', "log", "redirect output to a log file at PATH").new
      SimpleParser("PATH", true);
    add("state", "load/save server state at PATH").new
      SimpleParser("PATH", true);
  }

  public void execute(Ad env) {
    // TODO: Ugh, this is really idiotic, but we can deal with it later.
    env.unmarshal(Stork.settings);
    env.addAll(Ad.marshal(Stork.settings));

    Scheduler s = Scheduler.start(env);
    URI[] listen = Stork.settings.listen;

    if (listen == null || listen.length < 1)
      listen = new URI[] { Stork.settings.connect };

    for (URI u : listen) try {
      // Fix shorthand URIs.
      if (u.getScheme() == null)
        u = new URI(u.getSchemeSpecificPart(), "-1", u.getFragment());
      u = u.normalize();
      StorkInterface.create(s, u).start();
    } catch (Exception e) {
      e.printStackTrace();
      Log.warning("could not create interface: "+e.getMessage());
    }
  }
}
