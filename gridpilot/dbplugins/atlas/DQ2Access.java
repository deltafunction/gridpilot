package gridpilot.dbplugins.atlas;

import java.io.IOException;
import java.net.URLDecoder;

import gridfactory.common.Debug;
import gridfactory.common.ResThread;
import gridpilot.GridPilot;
import gridpilot.MyUtil;

import org.safehaus.uuid.UUIDGenerator;

/**
 * Methods for manipulating DQ2 database records.
 * @author  Cyril Topfel
 */
public class DQ2Access {

  private SecureWebServiceConnection wsSecure;

  //private final String addFilesToDatasetURL = "ws_content/rpc?operation=addFilesToDataset&API="+ATLASDatabase.DQ2_API_VERSION;
  private final String addFilesToDatasetURL = "ws_content/rpc";
  private final String createDatasetURL = "ws_repository/rpc?operation=addDataset&API="+ATLASDatabase.DQ2_API_VERSION;
  //private final String createDatasetURL = "ws_repository/rpc";
  // delete all files of this dataset in DQ2
  private final String deleteDatasetURL = "ws_content/rpc?operation=deleteDataset&API="+ATLASDatabase.DQ2_API_VERSION;
  // delete all locations of this dataset in DQ2
  private final String deleteDatasetURL1 = "ws_location/rpc";
  // Clears all dataset, dataset versions and dataset metadata for the given dataset
  private final String deleteDatasetURL2 = "ws_repository/rpc";
  // Delete subscriptions to this dataset
  private final String deleteDatasetURL3 = "ws_subscription/rpc";
  private final String getLocationsURL = "ws_location/rpc?operation=queryDatasetLocations&API="+ATLASDatabase.DQ2_API_VERSION;
  //private final String getLocationsURL = "ws_location/rpc";
  private final String getDatasetsByVUIDsURL = "ws_repository/rpc?operation=queryDatasetByVUIDs&API="+ATLASDatabase.DQ2_API_VERSION;
  private final String getDatasetsByNameURL = "ws_repository/rpc?operation=queryDatasetByName&API="+ATLASDatabase.DQ2_API_VERSION;
  private final String getFilesURL = "ws_content/rpc?operation=queryFilesInDataset&API="+ATLASDatabase.DQ2_API_VERSION;
  private final String addLocationsURL = "ws_location/rpc?operation=addDatasetReplica&API="+ATLASDatabase.DQ2_API_VERSION;
  private final String deleteLocationsURL = "ws_location/rpc?operation=deleteDatasetReplica&API="+ATLASDatabase.DQ2_API_VERSION;
  private final String deleteFilesURL = "ws_content/rpc?operation=deleteFilesFromDataset&API="+ATLASDatabase.DQ2_API_VERSION;
  private boolean checkingProxy = false;
  private boolean proxyOk = false;
  
  /**
   * Instantiates a DQ2Acces object
   * @param host secure DQ2WebServer
   * @param host secure Server Port
   * @param path path on the server
   */
  public DQ2Access(String host, int port, String path) {
    try{
      Debug.debug("New web service connection: "+host+" - "+port+" - "+path, 1);
      wsSecure = new SecureWebServiceConnection(host, port, path);
    }
    catch (Exception e){
      e.printStackTrace();
    }
  }
  
  private void checkProxy() throws Exception{
    ResThread t = (new ResThread(){
      public void run(){
        if(checkingProxy){
          while(!proxyOk){
            try{
              this.wait(1000);
            }
            catch (InterruptedException e) {
              e.printStackTrace();
              return;
            }
          }
          return;
        }
        else{
          checkingProxy = true;
          proxyOk = false;
        }
        try{
          GridPilot.getClassMgr().getSSL().getGridCredential();
        }
        catch(Exception ee){
          ee.printStackTrace();
        }
        finally{
          checkingProxy = false;
          proxyOk = true;
        }
      }
    });     
    MyUtil.myWaitForThread(t, "checkProxy", 0, "checkProxy");
  }

  /**
   * Find dataset names
   * @param vuidString The vuid of the DataSet to be located
   * returns the raw response from the DQ2 web server  
   */
  public String getDatasetsByVUIDs(String vuidString) throws Exception {
    Debug.debug("Checking proxy", 3);
    checkProxy();
    String keys[]={"vuids"};
    String values[]={vuidString};
    Debug.debug("Finding datasets with web service on "+getDatasetsByVUIDsURL, 1);
    String response = wsSecure.post(getDatasetsByVUIDsURL, keys, values);
    return response;
  }

  private String getDatasetsByName(String dsnString) throws Exception {
    Debug.debug("Checking proxy", 3);
    checkProxy();
    String keys[]={"dsn"};
    String values[]={dsnString};
    Debug.debug("Finding datasets with web service on "+getDatasetsByNameURL, 1);
    String response = wsSecure.get(getDatasetsByNameURL, keys, values);
    return response;
  }

  /**
   * Find the locations of a dataset
   * @param dsn The Name of the DataSet to be located
   * returns the raw response from the DQ2 web server  
   */
  public String getDatasetLocations(String vuidsString) throws Exception
  {
    Debug.debug("Checking proxy", 3);
    checkProxy();
    String keys[]={"vuids"/*, "API", "operation"*/};
    String values[]={vuidsString/*, ATLASDatabase.DQ2_API_VERSION, "queryDatasetLocations"*/};
    Debug.debug("Finding dataset locations with web service on "+getLocationsURL, 1);
    // Try 3 times.
    String response = null;
    for(int i=0; i<3; ++i){
      try{
        response = wsSecure.post(getLocationsURL, keys, values);
        if(response!=null){
          break;
        }
        Thread.sleep(3000);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    return response;
  }

  /**
   * Find the LFNs of a dataset
   * @param vuid The VUID of the DataSet
   * returns the raw response from the DQ2 web server  
   */
  public String getDatasetFiles(String vuidsString) throws Exception {
    Debug.debug("Checking proxy", 3);
    checkProxy();
    String keys[]={"vuids"};
    String values[]={vuidsString};
    Debug.debug("Finding files of "+vuidsString+" with web service on "+getFilesURL, 1);
    String response = wsSecure.post(getFilesURL, keys, values);
    return response;
  }

  /**
   * creates Dataset
   * @param dsn name of the dataset to be created
   * @param duid ID of the dataset to be created
   * @param vuid ID of the dataset to be created
   * NOTICE: this does not work: the vuid is ignored
   */
  public String createDataset(String dsn, String duid, String vuid) throws Exception {
    Debug.debug("Checking proxy", 3);
    checkProxy();
    /*
    curl --user-agent "dqcurl" --silent --insecure --cert /tmp/x509up_u3804 --key /tmp/x509up_u3804 --config 8f543c27-ec95-4c02-9294-338cc4e29a36 https://atlddmcat.cern.ch:443/dq2/ws_repository/rpc
    data="vuid=b2a80235-0be0-4b9d-9785-b12565c21fcd"
    data="duid=e6304c8a-8cb6-4af1-885c-343f141903b7"
    data="update=yes"
    data="dsn=user.FrederikOrellana5894-ATLAS.csc11.002.Gee_500_pythia_photos_reson"
    data="API=3_0"
    data="tuid=ac25f251-2afd-493e-a642-102e7c908135"
    data="operation=addDataset"
    
    curl -i --silent --max-time 600  --max-redirs 5  -H "User-Agent: dqcurl"  
    -H "TUID: 9dd66285-da07-11df-9812-90fba62a3218"  --key /tmp/x509up_u9649  
    --cert /tmp/x509up_u9649  -k  --url 'https://atlddmcat.cern.ch:443//dq2/ws_repository/rpc' 
    -d "vuid=e54b3d7a-da06-11df-8787-90fba62a3208&duid=e54b3bae-da06-11df-b36c-90fba62a3208&update=yes&dsn=user.forellan.ganga.JD.191919.ZmumuGamma_atlfast2_recon.D3PD.jid000293&API=0_3_0&operation=addDataset"
    */
    if(vuid==null || vuid.equals("")){
      vuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
    }
    if(duid==null || duid.equals("")){
      duid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
    }
    String keys[] = {"dsn", "vuid", "duid"/*, "update", "API", "operation"*/};
    String values[] = {dsn, vuid, duid/*, "yes", ATLASDatabase.DQ2_API_VERSION, "addDataset"*/};
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
    return vuid;
  }

  /**
   * adds Files to Dataset
     * @param vuid Version Unique Identifier of the Dataset to add the Files
     * @param guids Grid Unique Identfiers
   * @param lfns Logical File Names
     * @param sizes File sizes in bytes. May be null
     * @param checkSums <checksum type>:<checksum>. May be null
   * @throws Exception 
   */
  public boolean addLFNsToDataset(String vuid, String[] lfns, String[] guids,
        String[] sizes, String [] checkSums) throws Exception {
    Debug.debug("Checking proxy", 3);
    checkProxy();
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
    String keys[]={"files", "vuid", "update", "API", "vuids", "operation"};
    String values[]={data.toString(), vuid,  "yes", ATLASDatabase.DQ2_API_VERSION, "['"+vuid+"']", "addFilesToDataset"};
        
    wsSecure.post(addFilesToDatasetURL, keys, values);
    return true;
  }  

  /**
   * deletes a Dataset
   * @param dsn Logical Dataset Name of the Dataset to erase
   * @param vuid ID of the Dataset to erase
   */
  public boolean deleteDataset(String dsn, String vuid) throws Exception {
    Debug.debug("Checking proxy", 3);
    checkProxy();
    // Delete from each catalog
    String [] keys;
    String [] values;
    Debug.debug("Deleting "+dsn, 2);
    Debug.debug(" on "+deleteDatasetURL+" : "+wsSecure.protocolname, 2);
    String duid = getDuid(dsn);
    if(duid!=null && !duid.equals("")){
      keys = new String [] {"operation", "API", "uids"};
      values = new String [] {"deleteDatasetSubscriptions", ATLASDatabase.DQ2_API_VERSION, "['"+vuid+"', '"+duid+"']"};
      wsSecure.post(deleteDatasetURL3, keys, values);
    }
    /*keys = new String [] {"operation", "API", "vuids"};
    values = new String [] {"deleteDataset", ATLASDatabase.DQ2_API_VERSION, "['"+vuid+"']"};
    wsSecure.get(deleteDatasetURL1, keys, values);*/
    keys = new String [] {"vuids"};
    values = new String [] {"['"+vuid+"']"};
    wsSecure.post(deleteDatasetURL, keys, values);
    keys = new String [] {"operation", "API", "dsn"};
    values = new String [] {"trashDataset", ATLASDatabase.DQ2_API_VERSION, dsn};
    wsSecure.get(deleteDatasetURL2, keys, values);
    return true;
  }

  private String getDuid(String dsn) throws Exception {
    String response = getDatasetsByName(dsn);
    return parseDuid(response);
  }

  /**
   * registers Dataset in Location
   * @param vuid the vuid of the dataset being registered in a site
   * @param complete set the complete Status to yes (true) or no(false)
   */
  public boolean registerLocation(String vuid, String dsn, boolean complete, String location)
     throws Exception {
    Debug.debug("Checking proxy", 3);
    checkProxy();
    String com;
    if (complete) com = "1";
    else com = "0";
    String keys[]={"complete","vuid","dsn","site"};
    String values[]={com, vuid, dsn, location};
    wsSecure.post(addLocationsURL, keys, values);
    return true;
  }
    
  /**
   * parses one vuid out of a dq2 output
   * @param toParse String output from dq webserice access
   */
  public String parseVuid(String toParse) {
    // We have to parse something like
    // {'version': 1, 'vuid': '709b2ae9-f827-4800-b577-e3a38a763983', 'duid': 'b98adabc-c7f5-4354-a0ac-7d65d95c2f16'}
    Debug.debug("Parsing "+toParse, 3);
    String ret = toParse.replaceFirst(".*'vuid': '([^']+)'.*", "$1");
    Debug.debug("--> "+ret, 3);
    return ret.trim();
  }

  public String parseDuid(String toParse) {
    // We have to parse something like
    // {'version': 1, 'vuid': '709b2ae9-f827-4800-b577-e3a38a763983', 'duid': 'b98adabc-c7f5-4354-a0ac-7d65d95c2f16'}
    Debug.debug("Parsing "+toParse, 3);
    String ret = toParse.replaceFirst(".*'duid': '([^']+)'.*", "$1");
    if(ret.equals(toParse)){
      ret = "";
    }
    Debug.debug("--> "+ret, 3);
    return ret.trim();
  }

  /**
   * Deletes a site registered with a dataset identified by vuid.
   * If site is "ALL_SITES", all sites are deleted.
   * 
   * @param vuid    Dataset identifier
   * @param site    Site name
   */
  public void deleteFromSite(String vuid, String site) throws Exception {
    Debug.debug("Checking proxy", 3);
    checkProxy();
    String [] keys= null;
    String [] values = null;
    if(site.equals("ALL_SITES")){
      keys = new String [] {"operation", "API", "vuids"};
      values = new String [] {"deleteDataset", ATLASDatabase.DQ2_API_VERSION, "['"+vuid+"']"};
      wsSecure.get(deleteDatasetURL1, keys, values);

    }
    else{
      keys = new String [] {"vuid", "sites"};
      values = new String [] {vuid, "[\"" + site + "\"]"};
      wsSecure.post(deleteLocationsURL, keys, values);
    }
  }
  
  /**
   * Deletes a site registered with a dataset identified by vuid.
   * If site is "ALL_SITES", all sites are deleted.
   * 
   * @param vuid    Dataset identifier
   * @param site    Site name
   */
  public void deleteFiles(String vuid, String [] guids) throws Exception {
    Debug.debug("Checking proxy", 3);
    checkProxy();
    String [] keys = new String [] {"vuid", "vuids", "guids"};
    String [] values = new String [] {vuid, "['"+vuid+"']", "['" + MyUtil.arrayToString(guids, "', '") + "']"};
    wsSecure.post(deleteFilesURL, keys, values);
  }


}
