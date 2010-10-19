package gridpilot.dbplugins.atlas;

import gridfactory.common.Debug;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;


/**
 * The WebServiceConnection class implements acces to a webservice via get or post
 * It is an insecure connection
 * @author  Cyril Topfel
 * @version 1.0, August 2006 
 */
public class WebServiceConnection {

	private String host;
	private int port;
	private String rootOfRelative; 
	protected String protocolname;
	private HttpsURLConnection huc;
	
	/**
	 * Instantiates a connection 
	 * @param   host   IP address or host name to connect to
	 * @param   port   the port the web service is running on
	 * @param   path	   Root if relative path is used i.e. /dq2 (with begining slash) , "", or NULL
	 */
	public WebServiceConnection(String host, int port, String path) {
		protocolname = "http";
		if(path==null){
		  path = "";
		}
		this.host = host;
		this.port = port;
		this.rootOfRelative = path;
	}

	/**
	 * sets global proxy
	 * @param   host proxy host
	 * @param   port proxy port (usually 80)
	 */
	public void setProxy(String host, String port) {
		Properties systemProperties = System.getProperties();
		systemProperties.put("http.proxyHost",host);
		systemProperties.put("http.proxyPort",port);	
	}

	
	/**
	 * creates a path without parameters, of the form http://the.host.com:8000/bla/bli
	 * @param   path absolute or realtiv path, no host, no protocol, no port
	 */
	private String createFullPath(String path) {
		if (path==null) path="/";
		String rpath=null;
		if (path.startsWith("/")) //it is an absolute path
			{ rpath=protocolname + "://" + this.host + ":"+this.port+path;}
		else //it is a relative path
			{ rpath=protocolname + "://" + this.host + ":"+this.port+rootOfRelative+"/"+path;}
		return rpath;
	}
	
	/**
	 * Encodes a String array doublet to urlencoded data: key1=data1&key2=data2 etc
	 * @param   keys   Array of Strings, prooviding the keys for the data
	 * @param   values Array os Strings, providing the data belongig to the keys (not urlencoded)
	 */
	private String urlencodeArray(String[] keys, String[] values) throws IOException {
		if (keys.length!=values.length){
			throw new IOException("Number of keys and values differ");
		}
		StringBuffer params=new StringBuffer("");
		for (int a=0; a< keys.length; a++){
			if(a!=0){
			  params.append("&"); //don't do the & on first
			}
			params.append(URLEncoder.encode(keys[a].trim(), "utf-8") 
					+ "=" + URLEncoder.encode(values[a].trim(), "utf-8"));
		}
		return params.toString();	
	}
	
	/**
	 * Needed for polymorphism and Code reusability
	 * @param   theURL   URL to connect to
	 */	
	private HttpsURLConnection getConnectiontoUrl(URL url) throws IOException {
    //String tuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
	  HttpsURLConnection conn = (HttpsURLConnection)url.openConnection();
    //conn.setRequestProperty("TUID", tuid);
    conn.setRequestProperty("User-Agent", "dqcurl");
		return conn;
	}
	
	
	/**
	 * Use http GET method to access the Webservice
	 * @param   theURL   URL to connect to
	 */
	private String get(URL theURL) throws IOException {
		huc = getConnectiontoUrl(theURL); 
		huc.setRequestMethod("GET");
		huc.setDoOutput(true);
		huc.connect(); 
		StringBuffer result=new StringBuffer(); 
		InputStream is=null;
		try{
		  is = huc.getInputStream();
		}
		catch (IOException e){
		  Debug.debug(e.getMessage(), 2);
			is=huc.getErrorStream();
		}
		int code = huc.getResponseCode(); 
    BufferedReader in=null;
    in = new BufferedReader(new InputStreamReader(is));
    String line=null;
    while((line=in.readLine())!=null){
      result.append(line+"\n");
    }
    in.close();
    huc.disconnect();
		if(code != HttpURLConnection.HTTP_OK){ 
		  throw new IOException (result.toString());
		}
		return result.toString();
	}
	
	/**
	 * Use http GET method to access the Webservice
	 * @param   path   the path on the Webservice that provides the information (can be absolute, realtiv or NULL)
	 * @param   ue_data   the parameters sent to the webservice, urlencoded
	 */
	public String get(String path, String ue_data) throws IOException {
		if (ue_data==null) ue_data="";
		String physicalAccessName = createFullPath(path) + "?" + ue_data;
		URL getURL = new URL(physicalAccessName);
		return  get(getURL);
	}

	/**
	 * Use http get method to access the Webservice with key=>value
	 * @param   path   the path on the Webservice that provides the information (can be absolute, realtiv or NULL)
	 * @param   keys   Array of Strings, prooviding the keys for the data
	 * @param   values Array os Strings, providing the data belongig to the keys (not urlencoded)
	 */	
	public String get(String path, String[] keys, String[] values) throws IOException{
		String params = urlencodeArray(keys, values);
		String physicalAccessName = createFullPath(path);
		physicalAccessName = physicalAccessName + (physicalAccessName.indexOf("?")>-1?"&":"?") + params;
		URL getURL = new URL(physicalAccessName);
		return  get(getURL);		
	}

	
  /**
   * Use http DELETE method to access the Webservice
   * @param   theURL   URL to connect to
   * @param   data   urlencoded data
   */
  private String delete(URL theURL, String data) throws IOException {
    Debug.debug(theURL.toString() + " " + data, 2);
    huc = getConnectiontoUrl(theURL); 
    Debug.debug(huc.toString(), 2);
    huc.setRequestMethod("DELETE");
    huc.setDoInput(true);
    huc.setDoOutput(true);
    OutputStream aout = huc.getOutputStream(); 
    DataOutputStream out = new DataOutputStream(aout);
    out.writeBytes (data);
    out.flush ();
    out.close ();
    StringBuffer result=new StringBuffer(); 
    InputStream is=null;
    try{
      is = huc.getInputStream();
    }
    catch(IOException e){
      Debug.debug(e.getMessage(), 2);
      is=huc.getErrorStream();
    }
    int code = huc.getResponseCode();
    BufferedReader in=null;
    in = new BufferedReader(new InputStreamReader(is));
    String line=null;
    while((line=in.readLine())!=null){
      result.append(line+"\n");
    }
    in.close();
    huc.disconnect();
    if(code != HttpURLConnection.HTTP_OK){ 
      throw new IOException (result.toString());
    }
    return result.toString();
  }
    
  /**
   * Use http DELETE method to access the Webservice with key=>value
   * @param   path   the path on the Webservice that provides the information (can be absolute, realtiv or NULL)
   * @param   keys   Array of Strings, prooviding the keys for the data
   * @param   values Array os Strings, providing the data belongig to the keys (not urlencoded)
   */
  public String delete(String path, String[] keys, String[] values) throws IOException {
    String params = urlencodeArray(keys, values);
    String physicalAccessName = createFullPath(path);
    Debug.debug("Using delete URL "+physicalAccessName, 3);
    URL postURL = new URL(physicalAccessName);
    return delete(postURL,params);        
  }

  /**
	 * Use http POST method to access the Webservice
	 * @param   theURL   URL to connect to
	 * @param   data   urlencoded data
   * @throws Exception 
   * @throws  
	 */
	private String post(URL theURL, String data) throws IOException {	  
    Debug.debug(theURL.toString() + " " + data, 2);
		huc = getConnectiontoUrl(theURL); 
    //huc.setRequestProperty("POST", theURL.getPath()+" HTTP/1.1");
    //huc.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=utf-8");
    huc.setRequestMethod("POST");
    //huc.setRequestProperty("Content-Type","multipart/form-data; boundary="+Integer.toString(data.length()));
    //huc.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    //huc.setRequestProperty("Content-Length", Integer.toString(data.length()));
    //huc.setChunkedStreamingMode(1024);
    //huc.setFixedLengthStreamingMode(data.length());
    //huc.setRequestProperty("connection","Keep-Alive");
    //huc.setRequestProperty("Transfer-Encoding", "Chunked");
    //huc.setRequestProperty("Transfer-Encoding", "-");
    //huc.setRequestProperty("Accept", "*/*");
    //huc.setRequestProperty("HOST", theURL.getHost());
    huc.setDoInput(true);
    huc.setDoOutput(true);
    //huc.setUseCaches(false);
    //huc.setInstanceFollowRedirects(true);
    //huc.setAllowUserInteraction(true);
    //Debug.debug("HTTP headers-->"+huc.getRequestProperties(), 2);

		OutputStream aout = huc.getOutputStream();
    //DataOutputStream out = new DataOutputStream(aout);
    //out.writeBytes(data);
		//OutputStreamWriter out = new OutputStreamWriter(aout);
		//out.write(data);
    aout.write(data.getBytes("UTF-8"));
    /*byte[] dataBytes = data.getBytes();
		for(int i=0; i<dataBytes.length; ++i){
		  aout.write(dataBytes[i]);
		}*/
		/*BufferedOutputStream out = new BufferedOutputStream(aout);
		ByteArrayInputStream inp = new ByteArrayInputStream(data.getBytes());
    int i;
    while((i = inp.read())!=-1){
      //Debug.debug("Writing byte "+i, 3);
      out.write(i);
    }*/
    aout.flush();
    aout.close();
    //inp.close();

		StringBuffer result = new StringBuffer();
		InputStream is = null;
		try{
		  is = huc.getInputStream();
		}
		catch(IOException e){
		  Debug.debug(e.getMessage(), 2);
			is = huc.getErrorStream();
		}
		int code = huc.getResponseCode(); 
		BufferedReader in=null;
		in = new BufferedReader(new InputStreamReader(is));
		String line=null;
		while((line = in.readLine())!=null){
			result.append(line+"\n");
		}
		
    in.close();
    huc.disconnect();
    if(code!=HttpURLConnection.HTTP_OK){ 
      throw new IOException(result.toString());
    }
		return result.toString().trim();
	}
	
  /**
	 * Use http POST method to access the Webservice
	 * @param   path   the path on the Webservice that provides the information (can be absolute, realtiv or NULL)
	 * @param   ue_data   the parameters sent to the webservice, urlencoded (data1=test%20file&data2=otherthing)
	 */
	public String post(String path, String ue_data) throws Exception {
		if(ue_data==null){
		  ue_data = "";
		}
		String physicalAccessName = createFullPath(path);
		URL postURL = new URL(physicalAccessName);
		return post(postURL, ue_data);
	}

	/**
	 * Use http POST method to access the Webservice with key=>value
	 * @param   path   the path on the Webservice that provides the information (can be absolute, realtiv or NULL)
	 * @param   keys   Array of Strings, prooviding the keys for the data
	 * @param   values Array os Strings, providing the data belongig to the keys (not urlencoded)
	 */
	public String post(String path, String[] keys, String[] values) throws Exception
	{
		String params = urlencodeArray(keys, values);
		String physicalAccessName = createFullPath(path);
    Debug.debug("Using post URL "+physicalAccessName, 3);
		URL postURL = new URL(physicalAccessName);
		return post(postURL, params);		

	}

		/*
    WebServiceConnection a=new WebServiceConnection("www.daf.in",80,"/");
		//WebServiceConnection b=new WebServiceConnection("lheppc6.unibe.ch",80,null);
		//a.setProxy("proxy.unibe.ch", "80");
		//b.setProxy("proxy.unibe.ch", "80");
		
		String[] keys={"SUBJECT","actualid","which_set"};		
		String[] values={"dafin","55","55"};
		
    Debug.debug("test2", 3);
    Debug.debug(a.get(null,"SUBJECT=dafin&actualid=55&which_set=62"), 3);
    Debug.debug("test3", 3);
    Debug.debug(a.get(null,keys,values), 2);
		//Debug.debug("test4", 3);
		//Debug.debug(b.post("postecho.php",keys,values), 3);
		//Debug.debug("test5", 3);
		//Debug.debug(b.post("postecho.php","key1=data1&key2=data2"), 3);
	  */
}
