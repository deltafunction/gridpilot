/*
 * Created on Nov 17, 2004
 *
 */
package gridpilot;

import java.util.StringTokenizer;

/**
 * @author Frederik.Orellana@cern.ch
 *
 */

public class Util {

  public static String prefix = "";
  public static String url = "";

  public static String [] split(String s) {
    StringTokenizer tok = new StringTokenizer(s);
    int len = tok.countTokens();
    String [] res = new String[len];
    for (int i = 0 ; i < len ; i++) {
      res[i] = tok.nextToken();
    }
    return res ;
  }
  
  public static String [] split(String s, String delim) {
    StringTokenizer tok = new StringTokenizer(s, delim);
    int len = tok.countTokens();
    String [] res = new String[len];
    for (int i = 0 ; i < len ; i++) {
      res[i] = tok.nextToken();
    }
    return res ;
  }

  /**
   * Converts an array of object in a String representing this array.
   * Example : {"a", new Integer(32), "ABCE", null} is converted in "a 32 ABCE null"
   */
  public static String arrayToString(Object [] values){
    String res = "";
    if(values == null)
      return "(null)";
    for(int i=0; i< values.length ; ++i){
      res += (values[i] == null ? "null" : values[i].toString()) + " ";
    }
    return res;
  }

  /**
   * Converts a local path (<code>file</code>) into a absolute path by prepending the "prefix" attribute
   * of the AtCom section in config file. <br>
   * If the file name begins by '/' or the prefix is not defined, nothing is prepend, <br>
   * If prepend doesn't end by '/', a '/' is added between <code>file</code> and
   * <code>prepend</code>.
   */
  public static String getFullPath(String file){
    prefix = GridPilot.getClassMgr().getConfigFile().getValue("gridpilot","prefix");
    url = GridPilot.getClassMgr().getConfigFile().getValue("gridpilot","url");
    if(file.startsWith("/"))
      return file;

    if(prefix == null)
      return file;
    else
      return prefix + file;
  }

  /**
   * Converts a local path (<code>file</code>) into a URL by prepending the "url" attribute
   * of the AtCom section in config file. <br>
   * If the file name begins by 'http://' or the prefix is not defined, nothing is prepend, <br>
   * If prepend doesn't end by '/', a '/' is added between <code>file</code> and
   * <code>prepend</code>.
   */
  public static String getURL(String file){
    prefix = GridPilot.getClassMgr().getConfigFile().getValue("gridpilot","prefix");
    url = GridPilot.getClassMgr().getConfigFile().getValue("gridpilot","url");
    if(file.startsWith("http://") || file.startsWith("https://"))
      return file;

    if(url == null)
      return file;
    else
      return url + file;
  }

}
