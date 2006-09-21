package gridpilot.dbplugins.atlas;


import gridpilot.Debug;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.globus.gsi.GlobusCredential;

/**
 * The SecureWebServiceConnection class implements access to a webservice via get or post
 * It inherits from WebServiceConnection, but makes it secure.
 * @author  Cyril Topfel
 * @version 1.0, August 2006 
 */
public class SecureWebServiceConnection extends WebServiceConnection {

//	private final String protocolname="https"; //read by superclass
	private HttpsURLConnection huc; //set and used by superclass
	private TrustManager [] tm;
	private KeyManager[] km;
	private KeyStore ks;
	private KeyStore serks;
	private final String tmppwd="whateva";

	/**
	 * Instantiates a connection 
	 * @param   host   IP address or hostname to connect to
	 * @param   port   the port the web service is running on
	 * @param   rOR	   Root if relative path is used i.e. /dq2 (with begining slash) , "", or NULL
	 */	
	public SecureWebServiceConnection(String host, int port, String rOR)
	{
		super(host, port, rOR);
		protocolname="https";
	}


	/**
	 * this initialized the SSLContext and sets the Trust- and KeyManagers
	 * 
	 */	
	public void init() throws Exception
	{
		SSLContext ctx = SSLContext.getInstance("SSL");
		SecureRandom secran=SecureRandom.getInstance("SHA1PRNG");
		ctx.init (km, tm, secran);
		SSLSocketFactory sfcy= ctx.getSocketFactory();
		HttpsURLConnection.setDefaultSSLSocketFactory (sfcy);
	}

	/**
	 * initialized the Key Manager(s)
	 */	
	private void createKeyManager() throws Exception
	{
		KeyManagerFactory kmf =	KeyManagerFactory.getInstance("SunX509");
		kmf.init(ks, tmppwd.toCharArray());		
		km = kmf.getKeyManagers();	
	}

	/**
	 * Loads Certificates from a java keystorefile
	 * @param pathToFile The Path to the jks File
	 */	
	public void loadTrustedKeyStoreFromJKSFile(String pathToFile) throws Exception
	{
		serks=KeyStore.getInstance("JKS");
		serks.load(new FileInputStream(pathToFile),null);
		TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
		tmf.init(serks);
		tm = tmf.getTrustManagers();
	}

	//TODO: load Certifictas, in any way
	public void loadCertificates(String yetUnknown)
	{}

	/**
	 * loads the proxy certificate from th proxy file in the file system
	 * @param   pathToFile path to the proxy file, usually /tmp/x509....
	 */
	public void loadLocalProxyCertificate(String pathToFile) throws Exception
	{
		
		ks=KeyStore.getInstance("JKS");
		ks.load(null,null);
		GlobusCredential thecreds = new GlobusCredential(pathToFile);
		X509Certificate[] chain = thecreds.getCertificateChain();
		PrivateKey mypriv=thecreds.getPrivateKey();
		ks.setKeyEntry("mycert", mypriv, tmppwd.toCharArray(), chain);	
		createKeyManager();
	}

	/**
	 * loads the proxy certificate from a Globus Credetnial
	 * @param   globusCred The globusCred to read the proxy from
	 */
	public void loadGlobusCredentialCertificate(GlobusCredential globusCred) throws Exception
	{
		KeyStore ks=KeyStore.getInstance("JKS");
		ks.load(null,null);

		X509Certificate[] chain = globusCred.getCertificateChain();
		PrivateKey mypriv=globusCred.getPrivateKey();
		ks.setKeyEntry("mycert", mypriv, tmppwd.toCharArray(), chain);

		createKeyManager();
	}

	/**
	 * sets global proxy
	 * @param   host proxy host
	 * @param   port proxy port (usually 80)
	 */
	public void setProxy(String host, String port)
	{
		Properties systemProperties = System.getProperties();
		systemProperties.put("https.proxyHost",host);
		systemProperties.put("https.proxyPort",port);	
	}


	/**
	 * Needed for polymorphism and Code reusability
	 * @param   theURL   URL to connect to
	 */	
	private HttpsURLConnection getConnectiontoUrl(URL url) throws IOException //used by superclass
	{
    Debug.debug("creating httpsurlconectionsecure", 2);
		return (HttpsURLConnection)url.openConnection();
	}

	/**
	 * Use this method to allow connections without certificate auhtentication
	 * Use before init
	 */		
	public void trustAllCerts()
	{
		tm = new TrustManager[]{
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

	}


	/**
	 * Use this method to allow certificate vs real hostname mismatch
	 */	
	public void trustWrongHostName()
	{
		HostnameVerifier hv = new HostnameVerifier() {
			public boolean verify(String urlHostName, SSLSession session) {
        Debug.debug("WARNING: URL Host: "+urlHostName+" vs. "+session.getPeerHost(), 1);
				return true;
			}
		};
		HttpsURLConnection.setDefaultHostnameVerifier(hv);
	}

	public static void main(String args[]) throws IOException
	{
		SecureWebServiceConnection thSC 
		= new SecureWebServiceConnection("atlddmpro.cern.ch",8443,"/dq2");
		try 
		{
			thSC.loadLocalProxyCertificate("/tmp/x509up_u501");
			thSC.trustWrongHostName();
			//thSC.loadTrustedKeyStoreFromJKSFile("/Users/ct/Desktop/keytest/mykes.jks");
			thSC.trustAllCerts();
			thSC.init();
		}
		catch (Exception e)
		{
      Debug.debug("Exception", 1);
      e.printStackTrace();
		}
    Debug.debug("test start", 1);
    Debug.debug(thSC.post("ws_repository/dataset", "dsn=cyril-test-java"), 1);


	}


}
