package gridpilot.csplugins.glite;

import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;


/**
 * This class parses an LB job status page like e.g.
  <html>
  <body>
   <h2>https://lb106.cern.ch:9000/7i9Xzn1PbR8YBt4POv2ZEA</h2>
   <table halign="left">
   <tr><th align="left">Status:</th><td>Cleared</td></tr>
   <tr><th align="left">owner:</th><td>/O=Grid/O=NorduGrid/OU=nbi.dk/CN=Frederik Orellana</td></tr>
   <tr><th align="left">Condor Id:</th><td>699002</td></tr>
   <tr><th align="left">Reason:</th><td>user retrieved output sandbox</td></tr>
   <tr><th align="left">State entered:</th><td>Mon Oct 13 14:14:30 2008</td></tr>
   <tr><th align="left">Last update:</th><td>Mon Oct 13 14:14:30 2008</td></tr>
   <tr><th align="left">Expect update:</th><td>NO</td></tr>
   <tr><th align="left">Location:</th><td>none</td></tr>
   <tr><th align="left">Destination:</th><td>ce-2-fzk.gridka.de:2119/jobmanager-pbspro-atlasL</td></tr>
   <tr><th align="left">Cancelling:</th><td>NO</td></tr>
   </table>
  </body>
  </html>
 * @author fjob
 *
 */
public class LBInfo {
  
  private String url;
  private HashMap<String, String> map;
  
  public LBInfo(String _url) throws Exception{
    url = _url;
    map = new HashMap<String, String>();
    parseURL();
  }
  
  /**
   * For some reason Java cannot open the wms urls - the ssl handshake fails.
   * In the future this may start working...
   * @throws Exception
   */
  private void parseURL() throws Exception{
    //MyUtil.checkAndActivateSSL(GridPilot.getClassMgr().getGlobalFrame(), new String[]{url}, false);
    // This is to trust certs with wrong host name, expired certs - seems to be necessary
    URL thisUrl = new URL(url);
    //SecureWebServiceConnection sws = new SecureWebServiceConnection(thisUrl.getHost(), thisUrl.getPort(), thisUrl.getPath());
    /*File tmpFile = File.createTempFile(MyUtil.getTmpFilePrefix(), "");
    GridPilot.getClassMgr().getTransferControl().httpsDownload(url, tmpFile);
    String str = LocalStaticShell.readFile(tmpFile.getAbsolutePath());
    tmpFile.delete();*/
    String str = (String) thisUrl.getContent();
    StringBuffer sb = new StringBuffer();
    String key = null;
    String val = null;
    int start;
    int keyEnd;
    int end;
    String keyStartStr = "<tr><th align=\"left\">";
    String keyEndStr = ":</th><td>";
    String valEndStr = "</td></tr>";
    for(int i=0; i<str.length(); ++i){
      sb.append(str.substring(i, i+1));
      if((start=sb.indexOf(keyStartStr))>-1 && (keyEnd=sb.indexOf(keyEndStr))>-1 &&
           (end=sb.indexOf(valEndStr))>-1){
        key = sb.substring(start+keyStartStr.length(), keyEnd);
        val = sb.substring(keyEnd+keyEndStr.length(), end);
        sb.delete(start, end+valEndStr.length());
      }
      if(key!=null&&val!=null){
        map.put(key.trim().toLowerCase(), val.trim());
      }
    }
  }
  
  public HashMap<String, String> getMap(){
    return map;
  }
  
  public String getString(){
    StringBuffer sb = new StringBuffer();
    String key;
    for(Iterator<String> it=map.keySet().iterator(); it.hasNext();){
      key = it.next();
      sb.append(key);
      sb.append(": ");
      sb.append(map.get(key));
      if(it.hasNext()){
        sb.append("\n");
      }
    }
    return sb.toString();
  }
}
