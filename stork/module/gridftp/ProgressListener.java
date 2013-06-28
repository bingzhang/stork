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

// Listens for markers from GridFTP servers and updates transfer
// progress statistics accordingly.

public class ProgressListener {
  long last_bytes = 0;

  // When we've received a marker from the server. Return ad
  // describing progress.
  public Ad parseMarker(Reply r) {
    return parseMarker(new PerfMarker(r.getMessage()));
  } public Ad parseMarker(Marker m) {
    if (m instanceof PerfMarker) try {
      PerfMarker pm = (PerfMarker) m;
      long cur_bytes = pm.getStripeBytesTransferred();
      long diff = cur_bytes-last_bytes;

      last_bytes = cur_bytes;
      return new Ad("bytes_done", diff);
    } catch (Exception e) {
      // Couldn't get bytes transferred...
    } return null;
  }
}