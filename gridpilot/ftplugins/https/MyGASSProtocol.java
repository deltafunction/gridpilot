package gridpilot.ftplugins.https;

import org.globus.io.gass.client.internal.GASSProtocol;

public class MyGASSProtocol extends GASSProtocol {
 
    /** This method concatenates a properly formatted header for performing
     * Globus Gass GETs with the given information.
     *
     * @param path the path of the file to get 
     * @param host the host which contains the file to get
     *
     * @return <code>String</code> the properly formatted header to be sent to a
     * gass server
     */
    public static String EXECUTE(String path, String host, String cmd) {
      path = path.replaceFirst("^/+", "");
      return MyHTTPProtocol.createCmdHeader(path, host, cmd);
    }
    
}
