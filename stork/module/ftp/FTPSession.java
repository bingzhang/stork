package stork.module.ftp;

import io.netty.buffer.*;

import stork.cred.*;
import stork.feather.*;
import stork.module.*;
import stork.scheduler.*;
import stork.util.*;

public class FTPSession extends Session {
  transient FTPChannel ch;

  // Listing commands in order of priority.
  private static enum ListCommand {
    MLSC, STAT, MLSD, LIST(true), NLST(true);
    private boolean dataChannel;

    ListCommand() {
      this(false);
    } ListCommand(boolean dataChannel) {
      this.dataChannel = dataChannel;
    } ListCommand next() {
      final int n = ordinal()+1;
      return (n < values().length) ? values()[n] : null;
    } boolean requiresDataChannel() {
      return dataChannel;
    }
  }

  // Transient state related to the channel configuration.
  private transient boolean mlstOptsAreSet = false;

  public FTPSession(URI uri, Credential cred) {
    super(uri, cred);
  }

  public Bell<Void> doOpen() {
    final Bell<Void> bell = new Bell<Void>();
    String user = "anonymous";
    String pass = "";

    ch = new FTPChannel(uri) {
      public void onClose() { FTPSession.this.close(); }
    };

    // Pull userinfo from URI.
    if (uri.username() != null)
      user = uri.username();
    if (uri.password() != null)
      pass = uri.password();

    if (credential == null) {
      // Do nothing.
    } else if (credential instanceof StorkGSSCred) try {
      StorkGSSCred cred = (StorkGSSCred) credential;
      ch.authenticate(cred.data()).sync();
      user = ":globus-mapping:";  // FIXME: This is GridFTP-specific.
    } catch (Exception ex) {
      // Couldn't authenticate with the given credentials...
      throw new RuntimeException(ex);
    } else if (credential instanceof StorkUserinfo) {
      StorkUserinfo cred = (StorkUserinfo) credential;
      user = cred.getUser();
      pass = cred.getPass();
    }

    ch.authorize(user, pass).promise(new Bell() {
      public void done() {
        bell.ring();
      } public void fail(Throwable t) {
        bell.ring(t);
      }
    });

    return bell;
  }

  // Perform a listing of the given path relative to the root directory.
  public synchronized Bell<Stat> doStat(final URI uri) {
    return doStat(uri, null);
  } private synchronized Bell<Stat> doStat(final URI uri, final Stat base) {
    final Path path = (uri.path() != null) ? uri.path() : Path.ROOT;
    return new Bell<Stat>() {
      { tryListing(ListCommand.values()[0]); }

      // This will call itself until it finds a supported command.
      private void tryListing(final ListCommand cmd) {
        if (cmd == null)
          ring(new Exception("Listing is not supported."));
        else ch.supports(cmd.toString()).promise(new Bell<Boolean>() {
          public void done(Boolean supported) {
            if (supported) {
              actuallyDoListing(cmd);
            } else {
              tryListing(cmd.next());
            }
          }
        });
      }

      // This will get called to send the listing command.
      private void actuallyDoListing(final ListCommand cmd) {
        char hint = cmd.toString().startsWith("M") ? 'M' : 0;
        final FTPListParser parser = new FTPListParser(base, hint);

        parser.name(StorkUtil.basename(path.name()));
        parser.promise(this);

        // When doing MLSx listings, we can reduce the response size with this.
        if (hint == 'M' && !mlstOptsAreSet) {
          ch.new Command("OPTS MLST Type*;Size*;Modify*;UNIX.mode*");
          mlstOptsAreSet = true;
        }

        // Do a control channel listing, if specified.
        if (!cmd.requiresDataChannel())
          ch.new Command(cmd.toString(), makePath()) {
            public void handle(FTPChannel.Reply r) {
              if (r.code/100 > 2)
                parser.ring(r.asError());
              parser.write(r.message().getBytes());
              if (r.isComplete())
                parser.finish();
            }
          };

        // Otherwise we're doing a data channel listing.
        else ch.new DataChannel() {{
          attach(parser);
          new Command(cmd.toString(), makePath());
          unlock();
        }};
      }

      // Do this every time we send a command so we don't have to store a whole
      // concatenated path string for every outstanding listing command.
      private String makePath() {
        String p = path.toString();
        if (p.startsWith("/~"))
          return p.substring(1);
        else if (!p.startsWith("/") && !p.startsWith("~"))
          return "/"+p;
        return p;
      }
    };
  }

  // Create a directory at the end-point, as well as any parent directories.
  public Bell<Void> doMkdir(URI uri) {
    return null;
  }

  // Remove a file or directory.
  public Bell<Void> doRm(URI uri) {
    return null;
  }

  public Bell<Sink> doSink(final URI uri) {
    return (Bell<Sink>) ch.new DataChannel() {{
      new Command("STOR", uri.path());
      unlock();
    }}.bell();
  }

  public Bell<Tap> doTap(final URI uri) {
    return (Bell<Tap>) ch.new DataChannel() {{
      new Command("RETR", uri.path());
      unlock();
    }}.bell();
  }

  // Close the session and free any resources.
  public void doClose() {
    ch.close();
  }
}
