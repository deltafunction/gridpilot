package gridpilot.dbplugins.atlas;

import gridfactory.common.Debug;
import gridpilot.GridPilot;
import gridpilot.MySSL;
import gridpilot.MyUtil;

import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.globus.gsi.GlobusCredential;

/**
 * The SecureWebServiceConnection class implements access to a webservice via get or post
 * It inherits from WebServiceConnection, but makes it secure.
 * @author  Cyril Topfel
 */
public class SecureWebServiceConnection extends WebServiceConnection {

	/**
	 * Instantiates a connection 
	 * @param   host   IP address or hostname to connect to
	 * @param   port   the port the web service is running on
	 * @param   path	   Root if relative path is used i.e. /dq2 (with begining slash) , "", or NULL
	 */	
	public SecureWebServiceConnection(String host, int port, String path)
	{
		super(host, port, path);
    protocolname="https";
		try{
      init();
    }
		catch (Exception e) {
      e.printStackTrace();
    }
	}


	/**
	 * this initialized the SSLContext and sets the Trust- and KeyManagers
	 * 
	 */	
	private void init() throws Exception {
    trustWrongHostName();
    trustAllCerts();
    MyUtil.checkAndActivateSSL(new String[] {"https://atlddm.cern.ch/"});
    KeyManagerFactory keyManagerFactory;
    try{
      //keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
      keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      MySSL ssl = GridPilot.getClassMgr().getSSL();
      keyManagerFactory.init(ssl.getKeyStore(), ssl.getKeyPassword().toCharArray());
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keyManagerFactory.getKeyManagers(), null, new SecureRandom());
    }
    catch(Exception e) {
      e.printStackTrace();
      GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not activate SSL for HTTPS. DQ2 will not work properly.", e);
    }
	}

	/**
	 * Use this method to allow connections without certificate authentication
	 * Use before init
	 * @throws Exception 
	 */		
	private void trustAllCerts() throws Exception {
	  KeyManager[] km;
	  KeyStore ks;
	  final String tmppwd="whateva";
	  
	  TrustManager[] tm = new TrustManager[]{
				new X509TrustManager() {
					public java.security.cert.X509Certificate[] getAcceptedIssuers() {
						return null;
					}
					public void checkClientTrusted(
							java.security.cert.X509Certificate[] certs, String authType) {
					}
					public void checkServerTrusted(
							java.security.cert.X509Certificate[] certs, String authType) {
					}
				}
		};
	  
    ks = KeyStore.getInstance("JKS");
    ks.load(null,null);
    GlobusCredential thecreds = new GlobusCredential(MySSL.getProxyFile().getAbsolutePath());
    X509Certificate[] chain = thecreds.getCertificateChain();
    PrivateKey mypriv=thecreds.getPrivateKey();
    ks.setKeyEntry("mycert", mypriv, tmppwd.toCharArray(), chain);  
    //KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, tmppwd.toCharArray());   
    km = kmf.getKeyManagers();  

    SSLContext ctx = SSLContext.getInstance("SSL");
    SecureRandom secran = SecureRandom.getInstance("SHA1PRNG");
    ctx.init(km, tm, secran);
    SSLSocketFactory sfcy= ctx.getSocketFactory();
    HttpsURLConnection.setDefaultSSLSocketFactory (sfcy);
	}

	/**
	 * Use this method to allow certificate vs real hostname mismatch
	 */	
	private void trustWrongHostName() {
		HostnameVerifier hv = new HostnameVerifier() {
			public boolean verify(String urlHostName, SSLSession session) {
			  if(!urlHostName.trim().equalsIgnoreCase(session.getPeerHost())){
	        Debug.debug("WARNING: URL Host: "+urlHostName+" vs. "+session.getPeerHost(), 2);
			  }
				return true;
			}
		};
		HttpsURLConnection.setDefaultHostnameVerifier(hv);
	}

}
