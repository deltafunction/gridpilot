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

  //TODO TODO TODO TODO : I've tried to adapt to v 0.3, but not tested,
  //all this has to be retested and fixed

  private final String addFilesToDatasetURL = "ws_content/rpc?operation=addFilesToDataset&API=0_3_0";
  private final String createDatasetURL = "ws_location/rpc?operation=addDataset&API=0_3_0";
  //private final String deleteDatasetURL = "ws_repository/rpc?operation=eraseDataset&API=0_3_0";
  private final String deleteDatasetURL = "repository/dataset";
  //private final String deleteDatasetURL1 = "ws_content/rpc?operation=deleteDataset&API=0_3_0";
  //private final String deleteDatasetURL2 = "ws_location/rpc?operation=deleteDataset&API=0_3_0";
  private final String getLocationsURL = "ws_location/rpc?operation=queryDatasetLocations&API=0_3_0";
  private final String getDatasetsURL = "ws_repository/rpc?operation=queryDatasetByVUIDs&API=0_3_0";
  private final String getFilesURL = "ws_content/rpc?operation=queryFilesInDataset&API=0_3_0";
  private final String addLocationsURL = "ws_location/rpc?operation=addDatasetReplica&API=0_3_0";
  private final String deleteLocationsURL = "ws_location/rpc?operation=deleteDatasetReplica&API=0_3_0";
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
            wsSecure.loadLocalProxyCertificate(Util.getProxyFile().getAbsolutePath());
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
      String keys[]={"dsn", "update"};
      String values[]={dsn, "yes"};
      Debug.debug("Creating dataset with web service on "+createDatasetURL, 1);
      String response=wsSecure.post(createDatasetURL, keys, values);
      Debug.debug("createDataset response: "+response, 3);
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
   * Find dataset names
   * @param vuidString The vuid of the DataSet to be located
   * returns the raw response from the DQ2 web server  
   */
  public String getDatasets(String vuidString) throws IOException
  {
    String keys[]={"vuids"};
    String values[]={vuidString};
    Debug.debug("Finding datasets with web service on "+getDatasetsURL, 1);
    String response = wsSecure.post(getDatasetsURL, keys, values);
    return response;
  }

  /**
   * Find the locations of a dataset
   * @param dsn The Name of the DataSet to be located
   * returns the raw response from the DQ2 web server  
   */
  public String getDatasetLocations(String vuidsString) throws IOException
  {
    String keys[]={"vuids"};
    String values[]={vuidsString};
    Debug.debug("Finding dataset locations with web service on "+getLocationsURL, 1);
    String response = wsSecure.post(getLocationsURL, keys, values);
    return response;
  }

  /**
   * Find the LFNs of a dataset
   * @param vuid The VUID of the DataSet
   * returns the raw response from the DQ2 web server  
   */
  public String getDatasetFiles(String vuidsString) throws IOException
  {
    String keys[]={"vuids"};
    String values[]={vuidsString};
    Debug.debug("Finding files of "+vuidsString+" with web service on "+getFilesURL, 1);
    String response = wsSecure.post(getFilesURL, keys, values);
    return response;
  }

  /**
   * creates Dataset
   * @param dsn The Name of the DataSet to be created
   * @param vuid The ID of the DataSet to be created
   * NOTICE: this does not work: the vuid is ignored
   */
  public String createDataset(String dsn, String vuid) throws IOException
  {
    /*
    curl --user-agent "dqcurl" --silent --insecure --cert /tmp/x509up_u3804 --key /tmp/x509up_u3804 --config 8f543c27-ec95-4c02-9294-338cc4e29a36 https://atlddmcat.cern.ch:443/dq2/ws_repository/rpc
    data="vuid=b2a80235-0be0-4b9d-9785-b12565c21fcd"
    data="duid=e6304c8a-8cb6-4af1-885c-343f141903b7"
    data="update=yes"
    data="dsn=user.FrederikOrellana5894-ATLAS.csc11.002.Gee_500_pythia_photos_reson"
    data="API=0_3_0"
    data="tuid=ac25f251-2afd-493e-a642-102e7c908135"
    data="operation=addDataset"
   */
    String keys[] = {"dsn", "vuid"};
    String values[] = {dsn, vuid};
    Debug.debug("Creating dataset with web service on "+createDatasetURL, 1);
    String response = wsSecure.post(createDatasetURL, keys, values);
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
     * @param vuid Version Unique Identifier of the Dataset to add the Files
     * @param guids Grid Unique Identfiers
	 * @param lfns Logical File Names
     * @param sizes File sizes in bytes. May be null
     * @param checkSums <checksum type>:<checksum>. May be null
	 */
	public boolean addLFNsToDataset(String vuid, String[] lfns, String[] guids,
        String[] sizes, String [] checkSums) throws IOException
	{	
		if (lfns.length!=guids.length) 
			throw new IOException("Number of LFNs must be the same as Number of GUIDs. " +
					"Was "+lfns.length+" vs "+guids.length);
		
		/* e.g. files=[{'checksum': 'md5:68589db6b28b0758e96a0e07444c44fc', 
         * 'guid': 'ee8ffcb3-a97f-4b77-8e8c-11f923648b82', 
         * 'lfn': 'user.FrederikOrellana5894-ATLAS.testdataset1-some.file.1', 
         * 'size': 41943040L}]
        */

        StringBuffer data=new StringBuffer("[");
		for(int c=0; c<guids.length; c++){
            data.append("{");
            if(checkSums!=null && checkSums[c]!=null && !checkSums[c].equals("")){
              data.append("'checksum': '"+checkSums[c]+"'");
            }
            else{
              data.append("'checksum': None");
            }
			data.append(", 'guid': '"+guids[c]+"'");
            data.append(", 'lfn': '"+lfns[c]+"'");
            if(sizes!=null && sizes[c]!=null && !sizes[c].equals("")){
              // the size is of the form <bytes>L
              data.append(", 'size': "+sizes[c]+(sizes[c].endsWith("L")?"":"L"));
            }
            else{
              data.append(", 'size': None");
            }
            data.append("}");
		}
        data.append("]");
		String keys[]={"files","vuid","vuids","update"};
		String values[]={data.toString(),vuid,"[]","yes"};
        
		wsSecure.post(addFilesToDatasetURL, keys, values);
		return true;
	}	

	/**
	 * deletes a Dataset
	 * @param lfn Logical Dataset Name of the Dataset to erase
	 */
	public boolean deleteDataset(String dsn, String vuid) throws IOException
	{
		// Delete from each catalog
        String [] keys= new String [] {"dsn"};
        String [] values = new String [] {dsn};
        Debug.debug("Deleting "+dsn, 2);
        Debug.debug(" on "+deleteDatasetURL+" : "+wsSecure.protocolname, 2);
		wsSecure.delete(deleteDatasetURL, keys, values);
        //keys = new String [] {"vuid","delete"};
        //values = new String [] {vuid,"yes"};
        //wsSecure.post(deleteDatasetURL1, keys, values);
        //wsSecure.post(deleteDatasetURL2, keys, values);
		return true;
	}

  /**
   * deletes a Dataset version
   * @param vuid VUID of the Dataset to erase
   */
  public boolean deleteDatasetVersion(String vuid) throws IOException
  {
    String keys[]= {"vuid","delete"};
    String values[] = {vuid,"yes"};
    Debug.debug("Deleting "+vuid, 2);
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
		wsSecure.post(addLocationsURL, keys, values);
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
		wsSecure.get(deleteLocationsURL, keys, values);	
	}

}
