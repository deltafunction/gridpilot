package gridpilot.dbplugins.atlas;

import java.io.IOException;
import java.net.URLDecoder;

import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.Util;

//import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;

/**
 * Methods for manipulating DQ2 database records.
 * @author  Cyril Topfel
 */
public class DQ2Access {

	//private WebServiceConnection wsPlain;
	private SecureWebServiceConnection wsSecure;
	private String baseUrl="dq2/";

	private final String addFilesToDatasetURL="ws_content/dataset";
	private final String createDatasetURL="ws_repository/dataset";
	private final String deleteDatasetURL="ws_repository/dataset";
	private final String locationDatasetURL="ws_location/dataset"; 
	/**
	 * Instantiates a DQ2Acces object
	 * @param httpServer insecure DQ2WebServer   
	 * @param httpPort insecure Server Port
	 * @param httpsServer secure DQ2WebServer
	 * @param httpsPort secure Server Port
	 */
  public DQ2Access(/*String httpServer, int httpPort,*/
      String httpsServer, int httpsPort)
  {
    new DQ2Access(httpsServer, httpsPort, baseUrl);
  }
	public DQ2Access(/*String httpServer, int httpPort,*/
			String httpsServer, int httpsPort, String path)
	{
		try 
		{
			//wsPlain = new WebServiceConnection(httpServer, httpPort, baseUrl);
      Debug.debug("New web service connection: "+httpsServer+" - "+httpsPort+" - "+path, 1);
			wsSecure = new SecureWebServiceConnection(httpsServer, httpsPort, path);

			GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
			//GlobusCredential globusCred = null;
			if(credential instanceof GlobusGSSCredentialImpl){
				//globusCred =
					((GlobusGSSCredentialImpl)credential).getGlobusCredential();
			}

      //wsSecure.loadGlobusCredentialCertificate(globusCred);
      wsSecure.loadLocalProxyCertificate(Util.getProxyFile().getCanonicalPath());
			wsSecure.trustWrongHostName();
			wsSecure.trustAllCerts();
			wsSecure.init();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * creates Dataset
	 * @param dsn The Name of the DataSet to be created
	 * returns the vuid of the created Dataset   
	 */
	public String createDataset(String dsn) throws IOException
	{
		String keys[]={"dsn"};
		String values[]={dsn};
    Debug.debug("Creating dataset with web service on "+createDatasetURL, 1);
		String response=wsSecure.post(createDatasetURL, keys, values);
    String ret = parseVuid(URLDecoder.decode(response, "utf-8"));
    if(ret.indexOf("DQDatasetExistsException")>-1 && ret.indexOf("'")>-1){
      throw new IOException("ERROR: Dataset exists");
    }
    else if(ret.indexOf("Exception")>-1 && ret.indexOf("'")>-1){
      throw new IOException("ERROR: exception from DQ2: "+
          ret.replaceFirst(".*\\W+(\\w*Exception).*", "$1"));
    }    
    return ret;
	}

    /**
     * creates Dataset
     * @param dsn The Name of the DataSet to be created
     * @param vuid The ID of the DataSet to be created
     * NOTICE: this does not work: the vuid is ignored
     */
    public String createDataset(String dsn, String vuid) throws IOException
    {
        String keys[]={"dsn", "vuid"};
        String values[]={dsn, vuid};
    Debug.debug("Creating dataset with web service on "+createDatasetURL, 1);
        String response=wsSecure.post(createDatasetURL, keys, values);
    String ret = parseVuid(URLDecoder.decode(response, "utf-8"));
    if(ret.indexOf("DQDatasetExistsException")>-1 && ret.indexOf("'")>-1){
      throw new IOException("ERROR: Dataset exists");
    }
    else if(ret.indexOf("Exception")>-1 && ret.indexOf("'")>-1){
      throw new IOException("ERROR: exception from DQ2: "+
          ret.replaceFirst(".*\\W+(\\w*Exception).*", "$1"));
    }    
    return ret;
    }

	/**
	 * adds Files to Dataset
	 * @param lfns Logical File Names
	 * @param guids Grid Unique Identfiers
	 * @param vuid Version Unique Identifier of the Dataset to add the Files
	 */
	public boolean addLFNsToDataset(String[] lfns, String[] guids, String vuid) throws IOException
	{	
		if (lfns.length!=guids.length) 
			throw new IOException("Number of LFNs must be the same as Number of GUIDs. " +
					"Was "+lfns.length+" vs "+guids.length);
		
		StringBuffer data=new StringBuffer("");
		for (int c=0; c<guids.length; c++)
		{
			data.append(lfns[c]+"@"+guids[c]);
			if (c!=guids.length-1) data.append("@");
		}
		String keys[]={"vuid","data","update"};
		String values[]={vuid,data.toString(),"yes"};
		
		//DQ2Client claims it to be a PUT request, but it is actually a POST request with update=yes
		//se DQCurl at lxplus: /afs/cern.ch/atlas/offline/external/GRID/ddm/pro02/common/client/DQCurl.py
		
		wsSecure.post(addFilesToDatasetURL, keys, values);
		return true;
	}	

	/**
	 * deletes a Dataset
	 * @param lfn Logical Dataset Name of the Dataset to erase
	 */
	public boolean deleteDataset(String dsn) throws IOException
	{
		String keys[]= {"dsn","delete"};
		String values[] = {dsn,"yes"};
    Debug.debug("Deleting "+dsn, 2);
    Debug.debug(" on "+deleteDatasetURL+" : "+wsSecure.protocolname, 2);
		wsSecure.get(deleteDatasetURL, keys, values);
		return true;
	}
	
	/**
	 * registers Dataset in Location
	 * @param vuid the vuid of the dataset being registered in a site
	 * @param complete set the complete Status to yes (true) or no(false)
	 */
	public boolean registerLocation(String vuid, String dsn, boolean complete, String location) throws IOException
	{
		String com;
		if (complete) com = "1";
		else com = "0";
		String keys[]={"complete","vuid","dsn","site"};
		String values[]={com,vuid,dsn,location};
		wsSecure.post(locationDatasetURL, keys, values);
		return true;
	}
	  
	/**
	 * parses one vuid out of a dq2 output
	 * @param toParse String output from dq webserice access
	 */
	public String parseVuid(String toParse)
	{
    // We have to parse something like
    // {'version': 1, 'vuid': '709b2ae9-f827-4800-b577-e3a38a763983', 'duid': 'b98adabc-c7f5-4354-a0ac-7d65d95c2f16'}
    Debug.debug("Parsing "+toParse, 3);
    String ret = toParse.replaceFirst(".*'vuid': '([^']+)'.*", "$1");
    Debug.debug("--> "+ret, 3);
    return(ret);
	}

	/**
	 * deletes files from dataset by creating a new version returns new vuid
	 * 
	 * @param dsn Dataset name
	 */
	public String createNewDatasetVersion(String dsn) throws IOException
	{
		String keys[]= {"dsn","update"};
		String values[] = {dsn,"yes"};
		String newvuidmess = wsSecure.post(deleteDatasetURL, keys, values);
		return	parseVuid(newvuidmess);	
	}
	
  /**
   * deletes a site registered with a dataset identified by vuid
   * 
   * @param vuid    Dataset identifier
   * @param site    Site name
   */
	public void deleteFromSite(String vuid, String site) throws IOException
	{
		String keys[]= {"vuid","site","delete"};
		String values[] = {vuid,"[\"" + site + "\"]","yes"};
		wsSecure.get(locationDatasetURL, keys, values);	
	}

}
