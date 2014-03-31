package stork.scheduler;

import stork.ad.*;
import stork.feather.*;
import java.util.*;

public class Request extends Bell<Object> {
  public Ad ad;
  public String command;
  public User user;
  public Scheduler.CommandHandler handler;

  public Request(Ad ad) {
    cmd = ad.get("command");
    this.ad = ad.remove("command");
  }
}
