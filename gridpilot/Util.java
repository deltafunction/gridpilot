package gridpilot;

import java.util.StringTokenizer;

/**
 * @author Frederik.Orellana@cern.ch
 *
 */

public class Util{

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
   * of the gridpilot section in config file. <br>
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
   * of the gridpilot section in config file. <br>
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

  /**
   * Converts an unqualified path (<code>log</code>) into a remote path by prepending the "prefix" attribute
   * of the replica section in config file. <br>
   */
  public static String logToPhys(String log){
    Debug.debug(log, 2);

    if(log.indexOf("(") != -1){
      //log contains placement hint
      int left = log.indexOf("(");
      int right = log.indexOf(")");
      String path = log.substring(left+1,right);
      if(!path.endsWith("/")) path += "/";
      // FO: if path is absolute, don't use prefix
      //return prefix+path+log.substring(0,left) ;
      if(path.startsWith("/")){
        return path+log.substring(0,left) ;
      }
      else{
        return GridPilot.getReplicaPrefix()+path+log.substring(0,left) ;
      }
    }
    
    // FO: if log is absolute, don't use prefix
    // return log.
    if(log.startsWith("/")){
      return log;
    }

    // FO: if prefix ends with slash, don't use any of the algorithms below.
    if(GridPilot.getReplicaPrefix().endsWith("/")){
      return GridPilot.getReplicaPrefix()+log;
    }

   // Changed log -> log1 below
    String[] logArr = log.split("/");
    String log1=logArr[logArr.length-1];
    Debug.debug(log1, 2);
    return logToPhys(log, GridPilot.getReplicaPrefix());
  }

  private static String logToPhys(String log, String prefix) {
    String[] logArr = log.split("/");
    String log1=logArr[logArr.length-1];

      int dot1 = log1.indexOf(".");
      int dot2 = log1.indexOf(".",dot1+1);
      int dot3 = log1.indexOf(".",dot2+1);
      if (!((-1<dot1) && (dot1<dot2) && (dot2<dot3))) return prefix+log;
      String project = log1.substring(0,dot1);
      String dsnr = log1.substring(dot1+1,dot2);
      String phase = log1.substring(dot2+1,dot3);
    String res =  prefix+"project/"+project+"/"+phase+"/data/"+dsnr+"/"+log1 ;
    Debug.debug("logToPhys: "+log+"||"+prefix+" ->" +res,3);
    return res;
    }

}
