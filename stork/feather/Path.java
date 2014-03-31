package stork.feather;

import java.util.*;
import java.io.*;

import stork.feather.util.*;

/**
 * A representation of an absolute path adhering to RFC 3986's definition of
 * URI path components. This class is designed to be memory-efficient for
 * storing very large path trees. The instantiation of new {@code Path}s is
 * controlled internally, and {@link #create(String)} must be used to parse
 * strings into {@code Path} objects.
 * <p/>
 * All paths are absolute paths and will never contain components with the
 * names {@code ".."} or {@code "."}.
 * <p/>
 * {@code Path} objects are immutable and are safe to use as keys in a map or
 * entries in a set.
 */
public class Path {
  private final String name;
  private final Path up;
  private transient int hash = 0;

  private static final Intern<Path> INTERN = new Intern<Path>();
  private static final String[] EMPTY_SEGMENT_ARRAY = new String[0];

  // Only root should be created with this.
  private Path() {
    up = this;
    name = null;
  }

  private Path(Path up, String name) {
    this.up = up;
    this.name = Intern.string(name);
  }

  /**
   * The top-level parent of all paths. This is the only path whose parent is
   * itself, and the only path whose component length is 0.
   */
  public static final Path ROOT = new Path();

  /**
   * Return the parent of this path segment. If this path has a length of 1 or
   * less, this method will return the root path.
   *
   * @return The parent of this path segment.
   */
  public final Path up() { return up; }

  /**
   * Paths should be created using this static method, which will take care of
   * interning segments and unescaping segment names.
   *
   * @param path an escaped string representation of a path. 
   * @return The {@code Path} represented by {@code path}.
   */
  public static Path create(String path) {
    return create(ROOT, path);
  }

  private static Path create(Path par, String path) {
    String[] ps = popSegment(path);
    return (ps == null) ? par : create(par, ps[0]).appendSegment(ps[1]);
  }

  // Helper method for trimming trailing slashes and splitting the last
  // segment. This method returns null if the path represents the root
  // directory. Otherwise, it returns an array containing the remaining path
  // string and the last segment.
  private static String[] popSegment(String p) {
    int s, e = p.length()-1;
    while (e > 0 && p.charAt(e) == '/') e--;  // Trim slashes.
    if (e <= 0) return null;  // All slashes (or empty).
    s = p.lastIndexOf('/', e);
    if (s < 0)
      return new String[] { "", p.substring(0, e+1) };
    return new String[] { p.substring(0, s), p.substring(s+1, e+1) };
  }

  /**
   * Return a new path with the given path appended.
   *
   * @param path an escaped string representation of a path
   */
  public final Path appendPath(String path) {
    return create(this, path);
  }

  /**
   * Return a new path with the given path appended.
   *
   * @param path a path to append.
   * @return A path with the given path appended.
   */
  public final Path append(Path path) {
    Path p = this;
    for (String n : path.explode())
      p = p.appendSegment(n);
    return p;
  }

  /**
   * Create a path segment with the given name whose parent is this path. If
   * {@code name} is {@code "."}, this method returns {@code this}. If {@code
   * name} is {@code ".."}, this method returns the same thing as {@link
   * #up()}.
   *
   * @param name the unescaped name of a segment.
   */
  public Path appendSegment(final String name) {
    if (name.equals("."))
      return this;
    if (name.equals(".."))
      return up();
    return INTERN.intern(new Path(this, name));
  }

  /**
   * Return the unescaped name of this path segment.
   *
   * @return The unescaped name of this path segment.
   */
  public final String name() { return name; }

  /**
   * Return the name of this path segment, either escaped or unescaped.
   *
   * @param escaped whether or not the escape the segment name
   */
  public final String name(boolean escaped) {
    return escaped ? URI.escape(name) : name;
  }

  /**
   * Check if this path is the prefix of another path.
   *
   * @param path the path to check if this path is a prefix of.
   */
  public boolean prefixes(Path path) {
    if (path == this)
      return true;
    if (this.isRoot())  // 
      return true;
    if (path.isRoot())
      return false;
    if (this.name().equals(path.name()))
      return this.up().prefixes(path.up());
    return this.prefixes(path.up());
  }

  /**
   * Check if this path segment is the root path.
   */
  public final boolean isRoot() { return this == ROOT; }

  /**
   * Return a string representation of the path.
   *
   * @param escaped whether or not the escape the segment names
   */
  public String toString(boolean escaped) {
    return isRoot() ? "/" :
           up.isRoot() ? "/"+name(escaped) :
           up+"/"+name(escaped);
  }

  /**
   * Return an escaped string representation of the path.
   */
  public String toString() {
    return toString(true);
  }

  /**
   * Return an unescaped string representation of the path to be read by a
   * human. Note that this representation cannot be always be unambiguously
   * converted back into the original path, and should only be used for
   * presenting the path to a human for information purposes.
   *
   * @return An unescaped string representation of this path.
   */
  public final String readable() {
    return toString(false);
  }

  /**
   * Explode this path into its unescaped component names.
   *
   * @return An array of the names of this path's components.
   */
  public final String[] explode() {
    if (isRoot())
      return EMPTY_SEGMENT_ARRAY;
    String[] list = new String[length()];
    Path p = this;
    for (int i = list.length-1; i >= 0; i--) {
      list[i] = p.name();
      p = p.up();
    }
  }

  /**
   * Implode an array of unescaped component names into a {@code Path}.
   *
   * @param names component names to implode into a {@code Path}.
   * @return {@code names} merged into a {@code Path}.
   */
  public static Path implode(String... names) {
    Path p = ROOT;
    for (String n : names)
      p = p.appendSegment(names);
    return p;
  }

  /**
   * Returns whether this is a globbed path. That is, whether or not this path
   * has a glob segment in it somewhere.
   *
   * @return {@code true} if this path has a glob segment in it somewhere;
   * {@code false} otherwise.
   */
  public boolean isGlob() {
    //return up().isGlob();
    return false;
  }

  /**
   * Return the number of segments in the path.
   * @return The number of segments in the path.
   */
  public final int length() { return this == ROOT ? 0 : up.length()+1; }

  /**
   * The hash code of a path should be equal to the hash code of the string
   * representation of the path.
   */
  public int hashCode() {
    // TODO: Of course we should do this without actually stringifying the
    // entire path, which we can with a little bit of math. The hash code of a
    // string is defined in the Java documentation.
    return (hash != 0) ? hash : (hash = toString().hashCode());
  }

  /**
   * Check if two paths are equal. That is, check that two paths are
   * component-wise equal.
   */
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (!(o instanceof Path))
      return false;
    return equals((Path)o);
  } private boolean equals(Path p) {
    if (p == this)
      return true;
    if (p.isRoot() || isRoot())  // ROOT.equals(ROOT) caught by above.
      return false;
    if (p.hash != 0 && hash != 0 && p.hash != hash)  // Small optimization.
      return false;
    return name.equals(p.name) && up.equals(p.up);
  }

  /**
   * Check if the paths match through globbing.
   */
  public boolean matches(Path p) {
    return equals((Object)p);
  }

  public static void main(String args[]) throws Exception {
    Path p1 = Path.create("/home/globus");
    while (true) {
      p1 = p1.append(p1);
      System.out.println(p1.length());
    }

    /*
    BufferedReader r = new BufferedReader(new InputStreamReader(System.in));

    String sp = r.readLine();
    Path p = Path.create(sp);
    while (true) {
      sp = r.readLine();
      p = p.append(Path.create(sp));
      System.out.println(p);
    }
    */
  }
}
