package stork.module.gridftp;

import stork.ad.*;
import stork.module.*;
import stork.util.*;
import static stork.util.StorkUtil.Static.*;
import stork.stat.*;
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
// TODO: Document supported options.

public class GridFTPSession extends StorkSession {
  private StorkCred cred = null;
  private Optimizer optimizer = null;
  private ControlChannel cc;

  // The default options for a GridFTP session.
  private static Ad DEFAULT_CONFIG = new Ad()
    .put("parallelism", 4)
    .put("concurrency", 1)
    .put("pipelining", 20);
  
  private TransferProgress progress = null;
  private AdSink sink = null;
  private FTPServerFacade local;

  private int parallelism = 1;
  private int concurrency = 1;

  volatile boolean aborted = false;

  // Create a new session connected to an end-point specified by a URL.
  // opts may be null.
  public GridFTPSession(URI uri, Ad opts) {
    super(uri, new Ad().merge(DEFAULT_CONFIG, opts));

    // Check if we've been given a credential to use.
    // TODO: Automatic instantiation from MyProxy as well.
    if (opts.has("cred_token"))
      cred = CredManager.instance().getCred(opts.get("cred_token"));
    if (cred == null && opts.has("x509_file"))
      cred = StorkGSSCred.fromFile(opts.get("x509_file"));
    if (cred == null && opts.has("x509_proxy"))
      cred = StorkGSSCred.fromBytes(opts.get("x509_proxy").getBytes());

    // Check if we've been given an optimizer to use.
    // TODO: Replace with query to optimizer manager.
    String optim = opts.get("optimizer", "none");
    if (optim.equals("full_2nd"))
      optimizer = new Full2ndOptimizer();
    else if (optim.equals("full_c"))
      optimizer = new FullCOptimizer();
    else
      optimizer = new Optimizer();

    // Establish a control channel connection to remote server.
    try {
      cc = new ControlChannel(new FTPURI(uri, cred));
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  // StorkSession interface implementations
  // --------------------------------------
  // We can only pair with other GridFTPSessions for now.
  public boolean pairCheck(StorkSession other) {
    return other instanceof GridFTPSession;
  }

  // Close control channel.
  protected void closeImpl() {
    cc.close();
  }

  // Convert an ad listing to an XferList.
  // FIXME: Temporary hack.
  XferList mlsr(String sp, String dp) {
    Ad ad = listImpl(sp, new Ad("depth", -1));
    return new XferList(sp, dp, ad);
  }

  // Recusively list directories and return as an ad.
  protected Ad listImpl(final String path, Ad opts) {
    try {
      return listImpl2(path, opts);
    } catch (Exception e) {
      throw new Error(e);
    }
  } protected Ad listImpl2(final String path, Ad opts) throws Exception {
    int depth = 0;
    int cur_depth = 0;
    final Ad list = new Ad("path", path);
    final LinkedList<String> work = new LinkedList<String>();
    final LinkedList<Ad> work_ads = new LinkedList<Ad>();
    final String cmd;
    final boolean is_mlsd;

    // Get options from passed ad.
    if (opts != null) {
      depth = opts.getInt("depth", depth);
    }

    // Check if we need to do a local listing.
    if (cc.local)
      return null;
      //return StorkUtil.list(path);

    // Use channel pair until we have a time to do things better.
    final ChannelPair cp = new ChannelPair(cc);

    // Check if we can do MLSD or should use LIST instead.
    if (cp.rc.supports("MLST")) {
      cmd = "MLSD ";
      is_mlsd = true;
      cp.rc.write("OPTS MLST type;size;", true);
    } else if (cp.rc.supports("LIST")) {
      cmd = "LIST ";
      is_mlsd = false;
    } else {
      // Boy I sure hope this can never happen.
      cmd = "LIST ";
      is_mlsd = false;
    }

    D("Doing list command: "+cmd);

    // Turn off DCAU.
    if (cp.rc.supports("DCAU")) try {
      GridFTPServerFacade f = (GridFTPServerFacade) cp.oc.facade;
      f.setDataChannelAuthentication(DataChannelAuthentication.NONE);
      cp.rc.write("DCAU N", true);
    } catch (Exception e) {
      // Couldn't cast to GridFTPServerFacade probably, oh well.
    }

    work.add("");
    work_ads.add(list);

    // Keep listing and building subdirectory lists.
    // XXX This is uglier than it should be, I know.
    int total = 0;
    while ((depth < 0 || cur_depth++ <= depth) && !work.isEmpty()) {
      for (int i = work.size(); i > 0; i--) {
        final String p = work.pop();
        final Ad ad = work_ads.pop();

        total++;
        cp.pipePassive();
        cp.rc.write(cmd+path+"/"+p, true, cp.rc.new XferHandler(null) {
          public Reply handleReply() throws Exception {
            ListAdSink sink = new ListAdSink(ad, is_mlsd);
            cp.oc.facade.store(sink);
            Reply r = super.handleReply();
            D("Got reply: "+r);
            sink.waitFor();
            D("Ad is: "+ad);

            if (ad.has("dirs")) for (Ad a : ad.getAd("dirs")) {
              work.add(p+"/"+a.get("name"));
              work_ads.add(a);
            } return r;
          }
        });
      } try {
        cp.sync();
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }

    return list;
  }

  // Get the size of a file.
  protected long sizeImpl(String path) {
    try {
      if (cc.local)
        return StorkUtil.size(path);
      Reply r = cc.exchange("SIZE "+path);
      if (!Reply.isPositiveCompletion(r))
        throw E("file does not exist: "+path);
      return Long.parseLong(r.getMessage());
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  // Transfer a file with the given options.
  // TODO: Check opts.
  protected void transferImpl(String src, String dest, Ad opts) {
    XferList xl;

    // Some quick sanity checking.
    if (src == null || src.isEmpty())
      throw E("src path is empty");
    if (dest == null || dest.isEmpty())
      throw E("dest path is empty");

    System.out.println("Transferring: "+src+" -> "+dest);

    // See if we're doing a directory transfer and need to build
    // a directory list.
    if (src.endsWith("/")) {
      xl = mlsr(src, dest);
    } else {  // Otherwise it's just one file.
      xl = new XferList(src, dest, sizeImpl(src));
    }

    // Create a new progress tracker.
    progress = new TransferProgress();
    if (sink != null)
      progress.attach(sink);

    // Pass the list off to the transfer() which handles lists.
    try {
      transfer(xl);
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  // Transfer files from XferList.
  void transfer(XferList xl) throws Exception {
    // For now, use channel pair class.
    ControlChannel pcc = ((GridFTPSession) pair()).cc;
    ChannelPair cc = new ChannelPair(this.cc, pcc);

    System.out.println("Setting mode and type...");
    //if (cc.dc.local || !cc.gridftp)
    if (cc.dc.local)
      cc.setTypeAndMode('I', 'S');
    else
      cc.setTypeAndMode('I', 'E'); 

    // Initialize optimizer.
    System.out.println("Initializing optimizer...");
    if (optimizer == null) {
      optimizer = new Optimizer();
    } else if (xl.size() >= 1E7 || xl.size() <= 0) {
      optimizer.initialize(xl.size(), new Range(1,64));  // FIXME
    } else {
      System.out.println("File size < 1M, not optimizing...");
      optimizer = new Optimizer();
    } sink.mergeAd(new Ad("optimizer", optimizer.name()));

    // Connect source and destination server.
    System.out.println("Setting passive mode...");
    cc.pipePassive();

    // Make sure we were able to set passive mode.
    cc.rc.read();

    // mkdir dest directory.
    D("Piping root transfer...");
    cc.pipeXfer(xl.root, null);

    // Let the progress monitor know we're starting.
    progress.transferStarted(xl.size(), xl.count());
    
    // Begin transferring according to optimizer.
    while (!xl.isEmpty()) {
      Ad b = optimizer.sample(), update;
      TransferProgress prog = new TransferProgress();
      int s = b.getInt("pipelining");
      int p = b.getInt("parallelism");
      int c = b.getInt("concurrency");
      long len = b.getLong("size");
      XferList xs;

      System.out.println("Sample ad: "+b);
      System.out.println("Sample size: "+len);

      update = b.filter("pipelining", "parallelism", "concurrency");

      if (s > 0) cc.setPipelining(s);
      else update.remove("pipelining");

      // FIXME: Parallelism and concurrency don't work well together.
      /*
      if (p > 0) {
        System.out.println("Got parallelism: "+p);
        if (p != parallelism) {
          cc.parallelism = p;
          cc.close();
          cc = cc.duplicate();
        } else setParallelism(p);
      } else {
        update.put("parallelism", parallelism);
      }

      if (c > 0) setConcurrency(c);
      else update.remove("concurrency");
      */

      if (sink != null && update.size() > 0)
        sink.mergeAd(update);

      if (len >= 0)
        xs = xl.split(len);
      else
        xs = xl.split(-1);
      System.out.println("xl size: "+xl.size());
      System.out.println("xs size: "+xs.size());

      prog.transferStarted(xs.size(), xs.count());
      transferList(cc, xs);
      prog.transferEnded(true);

      b.put("throughput", prog.throughputValue(true)/1E6);
      optimizer.report(b);
    }

    // Now let it know we've ended.
    progress.transferEnded(true);
  }

  // Split the list over all control channels and call
  // transferList(cc, xl).
  // FIXME: This will wait on the slowest thread.
  void transferList(ChannelPair cc, XferList xl) throws Exception {
    System.out.println("Transferring list! size: "+xl.size());

    while (!xl.isEmpty()) {
      //if (!extended mode) cc.pipePassive();
      cc.pipeXfer(xl.pop(), progress);
    } cc.sync();
    return;
  }
}
