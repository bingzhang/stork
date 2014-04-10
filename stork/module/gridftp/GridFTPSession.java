package stork.module.gridftp;

import stork.ad.*;
import stork.module.*;
import stork.util.*;
import stork.scheduler.*;
import static stork.util.StorkUtil.Static.*;
import static stork.module.ModuleException.*;
import stork.optimizers.*;
import stork.cred.*;

import java.net.*;
import java.util.*;
import java.io.*;

import org.globus.ftp.*;
import org.globus.ftp.vanilla.*;
import org.globus.ftp.extended.*;
import org.ietf.jgss.*;
import org.gridforum.jgss.*;

// A custom GridFTP client that implements some undocumented
// operations and provides some more responsive transfer methods.

public class GridFTPSession extends StorkSession {
  private transient StorkCred<?> cred = null;
  private transient Optimizer optimizer = null;
  private transient ControlChannel cc;
  
  private transient FTPServerFacade local;

  // Module-specific options.
  public int concurrency = 1;
  public int parallelism = 4;
  public int pipelining = 20;

  private transient volatile boolean aborted = false;
  private transient boolean dcau = true;

  // Add a command to this if we find we cannot use it for listing. E.g.,
  // STAT does not list on some Globus GridFTP servers.
  private transient Set<String> does_not_list = new HashSet<String>();

  // Create a new session connected to an end-point specified by a URL.
  // opts may be null.
  public GridFTPSession(EndPoint e) {
    super(e);
    cc = new ControlChannel(e);
    cc.setPipelining(pipelining);
    setParallelism(parallelism);

    // Add a few things we already know can't be used for listing.
    does_not_list.add("MLST");
  }

  public void setParallelism(int p) {
    cc.pipe("OPTS RETR Parallelism="+p+","+p+","+p);
  }

  // Close control channel.
  protected void closeImpl() {
    cc.close();
  }

  // Recursively list directories and return as a file tree relative to the
  // passed path.
  protected ListBell listImpl(String path, Ad opts) {
    // Use channel pair until we have a time to do things better.
    ChannelPair pair = new ChannelPair(cc);

    // Turn off DCAU.
    disableDCAU();
    final GridFTPServerFacade f = pair.oc.facade;
    f.setDataChannelAuthentication(DataChannelAuthentication.NONE);

    // We don't want all the information the MLSx commands return.
    if (cc.supports("MLSC") || cc.supports("MLSD"))
      cc.pipe("OPTS MLST Type*;Size*;Modify*;UNIX.mode*");

    // This bell will be rung when the listing has finished, and will
    // contain the root file tree.
    ListBell bell = new ListBell(path, new FileTree(StorkUtil.basename(path)));
    bell.opts = opts;
    bell.cp   = pair;

    if (opts.has("depth")) {
      bell.depth = opts.getInt("depth", 1);
    } else if (opts.getBoolean("recursive")) {
      bell.depth = -1;
    } else {
      bell.depth = 1;
    }

    // This will handle choosing a listing command and ringing the bell
    // with the list when it's done.
    pipeListing(bell);
    return bell;
  }

  // Ring this when the listing and all sublistings are complete.
  private class ListBell extends Bell<FileTree> {
    String name;
    ListBell parent;  // This will be null if we're root.
    FileTree tree;    // This will be set for all sublists.
    int depth;
    int sublists = 0;
    Ad opts;
    ChannelPair cp;

    public ListBell(String name, FileTree tree) {
      this(name, tree, null);
    } public ListBell(String name, FileTree tree, ListBell p) {
      this.name = Intern.string(name);
      this.tree = tree;
      if ((parent = p) != null) {
        depth = (p.depth > 0) ? p.depth-1 : p.depth;
        opts = p.opts;
        cp = p.cp;
      }
    }

    private String path() {
      return (parent == null) ? name : parent.path()+"/"+name;
    }

    // Filter rings until we've been rung for every sublist.
    public boolean filter(FileTree ft, Throwable t) {
      return --sublists <= 0;
    }

    // Return whether or not we can use a command to list this.
    boolean canListWith(String s) {
      return cc.supports(s) &&
             (depth == 0 || !does_not_list.contains(s));
    }

    // Pipe a listing command for everything in the list then ring the
    // list bell when it's done. This should only be called if this bell
    // hasn't been rung and if the passed file tree has been listed.
    void doSublisting(FileTree ft) {
      assert !isRung();

      int fc = 0;
      tree = ft;

      // First count the number of files
      if (ft.files != null && ft.files.length > 0)
        for (FileTree sft : ft.files) if (sft.dir) fc++;

      // Then see if we have anything to actually do.
      if (fc == 0) {
        ring(ft);
      } else {
        sublists = fc;
        for (FileTree sft : ft.files)
          if (sft.dir) pipeListing(new ListBell(sft.name, sft, this));
      }
    }

    // Once we've rung, close the channel pair.
    protected void done(FileTree f) {
      if (depth == 0)
        f.setFiles((FileTree[]) null);
    } protected void always(FileTree f, Throwable t) {
      if (parent != null)
        parent.ring(parent.tree, null);
    }
  }

  // Pipe the appropriate listing command that will ring the list bell
  // when it's done.
  //private static String[] cc_list_cmds = {};
  private static String[] cc_list_cmds = { "MLST", "MLSC", "STAT" };
  private static String[] dc_list_cmds = { "MLSD", "LIST" };
  private Bell<Reply> pipeListing(ListBell lb) {
    String p = lb.path();

    // Try all of the command channel-based alternatives.
    for (String c : cc_list_cmds) if (lb.canListWith(c))
      return cc.pipe(c+" "+p, new CCListBell(lb, c));

    // Other alternatives require a data channel to list, so let's
    // pipe a passive command.
    for (String c : dc_list_cmds) if (cc.supports(c)) {
      DCListBell dlb = new DCListBell(lb, c);
      pipePassive(dlb.sink());
      return cc.pipe(c+" "+p, dlb);
    }

    throw abort("server does not support listing");
  }

  // A handler for reading lists over the control channel.
  private class CCListBell extends Bell<Reply> {
    ListBell lb;  // Ring me when we're done.
    int type;
    String cmd;
    FTPListParser p;

    CCListBell(ListBell lb, String cmd) {
      this.lb = lb;
      this.cmd = cmd;
      type = cmd.startsWith("M") ? 'M' : 0;
      p = new FTPListParser(null, (char)type);
    }

    public boolean filter(Reply r, Throwable t) {
      if (r != null && r.getCode() < 200) {
        p.write(r.getMessage().getBytes());
        return false;
      } else return true;
    }

    protected void done(Reply r) {
      // Parse the list from the reply.
      FileTree ft = p.parseAll(r.getMessage().getBytes());

      System.out.println(Ad.marshal(ft));

      if (ft.name == null) {
        // This can happen if "." was not found in the listing. This can
        // be the case if a file was given as the target, or if the server
        // simply doesn't include it in the listing.
        if (lb.tree != null)
          ft = lb.tree.copy(ft.files[0]);
        else
          ft.copy(ft.files[0]);
        if (ft.dir)
          does_not_list.add(cmd);  // Seems this command just stats, not lists.
        if (lb.depth == 0 || !ft.dir)
          lb.ring(ft);
        else
          pipeListing(lb);  // Pipe again using different command.
      } else {
        // If listing worked, this satisfies depth 0 and 1. If depth is
        // anything else, we need to pipe more listing commands.
        if (lb.depth == 0)
          ft.files = null;

        if (lb.tree != null) {
          if (ft.files != null)
            lb.tree.setFiles(ft.files);
          ft = lb.tree.copy(ft);
        }

        if (!ft.dir || lb.depth == 0 || lb.depth == 1)
          lb.ring(ft);
        else
          lb.doSublisting(ft);
      }
    } protected void fail(Reply r, Throwable t) {
      // Either we got a bad reply or a parse error.
      lb.ring(null, t);
    }
  }

  // A handler for reading lists over a data channel.
  private class DCListBell extends TransferBell {
    ListBell lb;  // Ring me when we're done.
    Bell<FileTree> sb;
    int type;

    DCListBell(ListBell lb, String cmd) {
      this.lb = lb;
      sb = new Bell<FileTree>();
      type = cmd.startsWith("M") ? 'M' : 0;
    }

    // Get a list sink for this thing.
    public TreeSink sink() {
      return new TreeSink(sb, lb.tree, type);
    }

    public void done(Reply r) {
      // Assumably, if we're here, passive mode worked. Now get the list.
      FileTree ft = sb.waitFor();

      // See if we need to do sublisting.
      if (lb.depth == 0 || lb.depth == 1) {
        if (lb.depth == 0) ft.files = null;
        lb.ring(ft);
      } else {
        lb.doSublisting(ft);
      }
    } public void fail(Reply r, Throwable t) {
      // Either we got a bad reply or a parse error.
      lb.ring(null, t);
    }
  }

  // Get the size of a file.
  protected Bell<Long> sizeImpl(final String path) {
    return cc.pipe("SIZE "+path).new Link<Long>() {
      public Long transform(Reply r) {
        return Long.parseLong(r.getMessage());
      }
    };
  }

  // Make a directory.
  protected Bell<Reply> mkdirImpl(String path) {
    return cc.pipe("MKD "+path);
  }

  // Remove a file or directory.
  protected Bell<Reply> rmImpl(String path) {
    if (path.endsWith("/"))
      return cc.pipe("RMD "+path);
    else
      return cc.pipe("DELE "+path);
  }

  public GridFTPChannel openImpl(String base, FileTree ft) {
    return new GridFTPChannel(base, ft);
  }

  // Bell for doing file transfers.
  class TransferBell extends Bell<Reply> {
    long size = -1;
    ProgressListener pl = new ProgressListener();
    GridFTPSession sess;
    private TransferBell other;
    RuntimeException failed = null;

    // Hack until we get something better.
    public TransferBell() {
      this(null, -1);
    } public TransferBell(GridFTPSession sess) {
      this(sess, -1);
    } public TransferBell(GridFTPSession sess, long size) {
      this.sess = sess;
      this.size = size;
    }

    // Filter markers from ringing the bell.
    public boolean filter(Reply r, Throwable t) {
      if (t != null || r == null) return true;
      int c = r.getCode();
      switch (c) {
        case 111:  // Restart marker
          return false;  // Just ignore for now...
        case 112:  // Progress marker
          if (sess != null) reportProgress(pl.parseMarker(r));
      } return c >= 200;
    }

    public TransferBell pairWith(TransferBell tb) {
      if (tb == this || tb == null) {
        other = null;
      } else {
        other = tb;
        tb.other = this;
      } return this;
    }

    public void done(Reply r) {
      int c = r.getCode();

      if (failed != null) {
        throw failed;
      } if (c == 226) {
        if (sess != null)
          sess.reportProgress(pl.done(size));
      } else {
        throw abort("unexpected reply: "+c);
      }
    }

    // Abort the other transfer if this fails.
    public void fail(Reply r, Throwable t) {
      if (other != null)
        other.failed = (RuntimeException) t;
    }
  }

  // Channel implementation.
  public class GridFTPChannel extends StorkChannel {
    GridFTPSession gs = GridFTPSession.this;
    ControlChannel cc = GridFTPSession.this.cc;

    // The URI to the channel resource.
    public GridFTPChannel(String base, FileTree ft) {
      super(base, ft);
    }

    // Return the path to the file represented by this channel.
    private String path() {
      String b = base;
      if (!b.endsWith("/")) b = b+"/";
      return URI.create(b).resolve(file.path()).normalize().getPath();
    }

    // Quick hack to open relativized channels.
    private GridFTPChannel openSub(FileTree f) {
      return new GridFTPChannel(base, f);
    }

    // FIXME: Temporary hack.
    public boolean exists() {
      try {
        sizeImpl(path());
        return true;
      } catch (Exception e) {
        return false;
      }
    }

    // Send data to another channel. The two channels should coordinate to
    // produce a bell that will be rung when the exchange is complete. A
    // matching recvFrom should be called on the other channel. Sending
    // with a length of -1 will cause data to be sent until the source is
    // out of data.
    // FIXME: This is a pretty hacky thing.
    public Bell<?> sendTo(StorkChannel c, long off, long len) {
      if (!(c instanceof GridFTPChannel))
        throw abort("can only send to other FTP channels");
      final GridFTPChannel gc = (GridFTPChannel) c;
      final ControlChannel dc = gc.cc;

      // TODO: Implement encryption and compression negotiation.

      disableDCAU();
      gc.gs.disableDCAU();

      reportProgress(new Ad("bytes_total", file.size())
                       .put("files_total", file.count()));

      return sendTo(c, null);
    } private Bell<Reply> sendTo(StorkChannel c, final Bell<Reply> meta) {
      if (!(c instanceof GridFTPChannel))
        throw abort("can only send to other FTP channels");
      final GridFTPChannel gc = (GridFTPChannel) c;
      final ControlChannel dc = gc.cc;

      Log.info("Transferring from ", path(), " to ", gc.path());
      Log.info("Transfer type is ", file.dir ? "dir" : "file",
               " to ", gc.file.dir ? "dir" : "file");

      // For directories, pipe all contained files.
      if (file.dir) {
        final Bell<Reply> b = new Bell<Reply>() {
          public int count = 1;
          public boolean first = true;
          public boolean filter(Reply r, Throwable t) {
            if (first) {
              // Pipe all the subfiles.
              if (file.files != null) for (FileTree f : file.files) {
                count++;
                openSub(f).sendTo(gc.openSub(f), this);
              } first = false;
            }
            return --count <= 0;
          } public void always(Reply r, Throwable t) {
            if (meta != null)
              meta.ring();
          }
        };
        dc.pipe("MKD "+gc.path(), new Bell<Reply>() {
          public void always(Reply r, Throwable t) {
            b.ring();
          }
        });
        return b;
      }

      // Create a bell for each transfer, where one will fail the other
      // if a problem happens in either.
      final TransferBell sb = new TransferBell(GridFTPSession.this, file.size) {
        public void always(Reply r, Throwable t) {
          if (meta != null)
            meta.ring();
        }
      };
      final TransferBell db = new TransferBell().pairWith(sb);

      // TODO: Pipe checksum verification commands.

      // TODO: More robust third-party transfer checking.
      dc.pipe("PASV", gc.gs.new PassiveBell(
        new ActiveBell(new Bell<Reply>() {
          public void done(Reply r) {
            cc.pipe("RETR "+path(), db);
          }
        })
      ));
      dc.pipe("STOR "+gc.path(), sb);

      return sb;
    }

    // Close the channel, finalizing any resources held by the channel,
    // and ending any ongoing transfers.
    public void close() { }
  }

  // Disable data channel authentication if it's not already done.
  // FIXME: Don't pipe mode E here, this is just for hacky purposes.
  private void disableDCAU() {
    if (dcau && cc.supports("DCAU")) {
      cc.pipe("DCAU N");
      cc.pipe("MODE E");
    } dcau = false;
  }

  // Put this channel into passive mode and put the other channel into
  // active mode, or attach a local sink.
  private Bell<Reply> pipePassive(GridFTPSession other) {
    Bell<Reply> reply = new Bell<Reply>();
    cc.pipe("PASV", new PassiveBell(other.new ActiveBell(reply)));
    return reply;
  } private Bell<Reply> pipePassive(DataSink sink) {
    return cc.pipe("PASV", new PassiveBell(sink));
  }

  // Ringing this with a PASV reply will parse the reply and ring the
  // associated bell with the HostPort.
  private class PassiveBell extends Bell<Reply> {
    Bell<HostPort> next;
    DataSink sink;
    public PassiveBell(DataSink sink) {
      this.sink = sink;
    } public PassiveBell(Bell<HostPort> next) {
      this.next = next;
    } public void done(Reply r) {
      Log.finer("Got passive reply: ", r);
      String s = r.getMessage().split("[()]")[1];
      BetterHostPort hp = new BetterHostPort(s);

      hp.subnetHack(cc.getIP().getAddress());

      if (sink != null) try {
        cc.facade.setActive(hp);
        cc.facade.store(sink);
        //next.ring();
      } catch (Exception e) {
        throw abort(e);
      } else if (next != null) {
        next.ring(hp);
      }
    } public void fail(Reply r, Throwable t) {
      if (next != null)
        next.ring(t);
    }
  }

  // Ringing this bell with a HostPort will put the channel into
  // active mode.
  private class ActiveBell extends Bell<HostPort> {
    Bell<Reply> next;
    public ActiveBell(Bell<Reply> next) {
      this.next = next;
    } public void done(HostPort p) {
      Log.fine("Making active connection to: ", p.getHost());
      cc.pipe("PORT "+p.toFtpCmdArgument(), next);
    } public void fail(HostPort p, Throwable t) {
      if (next != null)
        next.ring(t);
    }
  }
}
