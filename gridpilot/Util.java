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

}
