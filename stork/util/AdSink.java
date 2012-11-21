package stork.util;

// Ad sink to allow for ads from multiple sources.

public class AdSink {
  volatile boolean closed = false;
  volatile boolean more = true;
  volatile ClassAd ad = null;

  public synchronized void close() {
    closed = true;
    System.out.println("Closing ad sink...");
    notifyAll();
  }

  public synchronized void putAd(ClassAd ad) {
    if (closed) return;
    this.ad = ad;
    notifyAll();
  }

  public synchronized void mergeAd(ClassAd a) {
    putAd((ad != null) ? ad.merge(a) : a);
  }

  // Block until an ad has come in, then clear the ad.
  public synchronized ClassAd getAd() {
    if (more) try {
      wait();
      if (closed) more = false;
      return ad;
    } catch (Exception e) { }
    return null;
  }
}
