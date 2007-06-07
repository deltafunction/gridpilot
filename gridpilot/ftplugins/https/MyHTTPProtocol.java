package gridpilot.ftplugins.https;

import org.globus.util.http.HTTPProtocol;

public class MyHTTPProtocol extends HTTPProtocol {
  public static String createCmdHeader(String path, String host, String cmd) {
    String newPath = null;
    
    newPath = "/" + path;    
    StringBuffer head = new StringBuffer();
    head.append(cmd+" " + newPath + " " + HTTPProtocol.HTTP_VERSION + HTTPProtocol.CRLF);
    head.append(HTTPProtocol.HOST + host + HTTPProtocol.CRLF);
    head.append(HTTPProtocol.CONNECTION_CLOSE);
    head.append(USER_AGENT + USER_AGENT + HTTPProtocol.CRLF);
    head.append(HTTPProtocol.CRLF);

    return head.toString();
  }
}
