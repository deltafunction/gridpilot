package gridpilot.dbplugins.atlas;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import gridpilot.Debug;
import gridpilot.GridPilot;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;

/**
 * Methods for manipulating DQ2 database records.
 * @author  Cyril Topfel
 */
public class DQ2Access {

	//private WebServiceConnection wsPlain;
	private SecureWebServiceConnection wsSecure;
	private final String baseUrl="dq2/";

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
			wsSecure = new SecureWebServiceConnection(httpsServer, httpsPort, baseUrl);

			GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
			GlobusCredential globusCred = null;
			if(credential instanceof GlobusGSSCredentialImpl){
				globusCred =
					((GlobusGSSCredentialImpl)credential).getGlobusCredential();
			}

			wsSecure.trustWrongHostName();
			wsSecure.loadGlobusCredentialCertificate(globusCred);
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
	 * @param DataSetName The Name of the DataSet to be created
	 * returns the vuid of the created Dataset   
	 */
	public String createDataset(String dsn) throws IOException
	{
		String keys[]={"dsn"};
		String values[]={dsn};
		String response=wsSecure.post(createDatasetURL, keys, values);		
		return parseVuid(response);
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
		String keys[]= {"lfn","delete"};
		String values[] = {dsn,"yes"};
		wsSecure.get(deleteDatasetURL, keys, values);
		return true;
	}
	
	/**
	 * registers Dataset in Location
	 * @param vuid the vuid of the dataset being registered in a site
	 * @param complete set the complete Status to yes (true) or no(false)
	 */
	public boolean registerVuidInLocation(String vuid, boolean complete, String location) throws IOException
	{
		String com;
		if (complete) com = "yes";
		else com = "no";
		String keys[]={"complete","vuid","site"};
		String values[]={com,vuid,location};
		wsSecure.post(locationDatasetURL, keys, values);
		return true;
	}
	
  /**
   * updates Dataset
   * @param vuid String            the vuid of the dataset
   * @param dsn String             the dataset name
   * @param incomplete String []   list of sites with incomplete replica
   * @param complete String []     list of sites with complete replica
   * this method is INSECURE. If complete/incomplete is given (not null),
   * the dataset is deleted before a new one is created (with the same vuid).
   * TODO: improve this
   */
  public boolean updateDataset(String vuid, String dsn, String [] incomplete, String [] complete)
     throws IOException
  {
    if(dsn==null || dsn.length()==0){
      throw new IOException("ERROR: empty dataset name");
    }
    if(vuid==null || vuid.length()==0){
      throw new IOException("ERROR: empty vuid");
    }
    if(incomplete!=null && complete!=null){
      if(!deleteDataset(vuid)){
        throw new IOException("ERROR: could not update dataset "+dsn);
      }
    }
    String keys[]={"vuid","dsn"};
    String values[]={vuid, dsn};
    // TODO: does this create a new dataset with this vuid? Check!
    wsSecure.post(locationDatasetURL, keys, values);
    if(incomplete!=null && complete!=null){
      for(int i=0; i<incomplete.length; ++i){
        registerVuidInLocation(vuid, false, incomplete[i]);
      }
      for(int i=0; i<complete.length; ++i){
        registerVuidInLocation(vuid, true, complete[i]);
      }
    }
    return true;
  }
  
	/**
	 * parses one vuid out of a dq2 output
	 * @param toParse String output from dq webserice access
	 */
	private String parseVuid(String toParse)
	{
		toParse=toParse.replaceAll(" ", "");
		Pattern regex=
		  Pattern.compile("vuid:'([A-Fa-f0-9]{8}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{12})'");
		Matcher thematcher= regex.matcher(toParse);
		thematcher.find();
		return thematcher.group(1);
	}


	/**
	 * parses multiple vuids out of a dq2 output
	 * @param toParse String output from dq webservice access
	 */
	private String[] parseVuids(String toParse)
	{
		
		toParse=toParse.replaceAll(" ", "");
		Debug.debug(toParse, 3);
		Pattern regex=Pattern.compile("vuids:\\[(('[A-Fa-f0-9]{8}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{12}'[,]*)+)\\]");
		Matcher thematcher = regex.matcher(toParse);
		thematcher.find();
		String[] res= thematcher.group(1).split(",");
		for (int q=0; q<res.length;q++)
		{
			res[q] = res[q].replaceAll("'","");
		}
		return res;
  }
  /*
   DQ2Access myacc=new DQ2Access(null,0,null,0);
    Debug.debug(myacc.parseVuid(
        "dsjhfagsdkjhf vuid: '45348fe3-4564-ffee-34ef-aef455678efe' hghkjgj"), 2);
    String [] fud=myacc.parseVuids("{'acsadfwedwed.wefwefwef-wefwrf3234r4':{'duid':'754389ef-bb55-7654-98ef-76549870fe43','vuids':['754389ef-bb55-7654-98ef-76549870fe43','754389ef-bb55-7654-98ef-76549870fe43']},{'csadfwedwed.wefwefwef-wefwrf3234r4':{'duid':'754389ef-bb55-7654-98ef-76549870fe43','vuids':['754389ef-bb55-7654-98ef-76549870fe43','754389ef-bb55-7654-98ef-76549870fe43']},{'csadfwedwed.wefwefwef-wefwrf3234r4':{'duid':'754389ef-bb55-7654-98ef-76549870fe43','vuids':['754389ef-bb55-7654-98ef-76549870fe43','754389ef-bb55-7654-98ef-76549870fe43']},{'csadfwedwed.wefwefwef-wefwrf3234r4':{'duid':'754389ef-bb55-7654-98ef-76549870fe43','vuids':['754389ef-bb55-7654-98ef-76549870fe43','754389ef-bb55-7654-98ef-76549870fe43']},}");
    Debug.debug(""+fud.length, 2);
    for (int q=0; q<fud.length; q++)
    {
      Debug.debug(fud[q], 2);
    }
  */
	
}
