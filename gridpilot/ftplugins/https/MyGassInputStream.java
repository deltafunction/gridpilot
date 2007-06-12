package gridpilot.ftplugins.https;

import java.io.IOException;
import java.net.Socket;

import org.globus.common.ChainedIOException;
import org.globus.io.gass.client.GassException;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.GssSocketFactory;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.SelfAuthorization;

import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSContext;

import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;

public class MyGassInputStream extends MyHTTPInputStream {

  private GSSCredential cred;
  private Authorization auth;

  public MyGassInputStream(GSSCredential cred,
       Authorization auth,
       String host,
       int port,
       String file) 
  throws GassException, GSSException, IOException {
    super();
    this.cred = cred;
    this.auth = auth;
    get(host, port, file);
  }

  public MyGassInputStream(String host, int port, String file, String cmd) 
	   throws GassException, GSSException, IOException {
    this(null, SelfAuthorization.getInstance(),
       host, port, file, cmd);
     }
     
    public MyGassInputStream(GSSCredential cred, Authorization auth,
      String host, int port, String file, String cmd) 
       throws GassException, GSSException, IOException {
      super();
      this.cred = cred;
      this.auth = auth;
      execute(host, port, file, cmd);
    }

  protected Socket openSocket(String host, int port) 
	   throws IOException {
	
    GSSManager manager = ExtendedGSSManager.getInstance();

    ExtendedGSSContext context = null;
    try { 
        context = 
      (ExtendedGSSContext)manager.createContext(
                                         null,
                 GSSConstants.MECH_OID,
                 this.cred,
                 GSSContext.DEFAULT_LIFETIME
              );
    
        context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_SSL);
    } catch (GSSException e) {
        throw new ChainedIOException("Security error", e);
    }
    
    GssSocketFactory factory = GssSocketFactory.getDefault();
    
    socket = factory.createSocket(host, port, context);

    ((GssSocket)socket).setAuthorization(this.auth);
    
    return socket;
  }
    
}
