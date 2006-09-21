package gridpilot.dbplugins.atlas;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import gridpilot.Debug;
import gridpilot.GridPilot;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;

public class DQ2Access {

	private WebServiceConnection WSplain;
	private SecureWebServiceConnection WSsecure;
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
	public DQ2Access(String httpServer, int httpPort, 
			String httpsServer, int httpsPort)
	{
		try 
		{
			WSplain = new WebServiceConnection(httpServer, httpPort, baseUrl);
			WSsecure = new SecureWebServiceConnection(httpsServer, httpsPort, baseUrl);

			GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
			GlobusCredential globusCred = null;
			if(credential instanceof GlobusGSSCredentialImpl){
				globusCred =
					((GlobusGSSCredentialImpl)credential).getGlobusCredential();
			}

			WSsecure.trustWrongHostName();
			WSsecure.loadGlobusCredentialCertificate(globusCred);
			WSsecure.trustAllCerts();
			WSsecure.init();
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
	public String createDataset(String DatasetName) throws IOException
	{
		String keys[]={"dsn"};
		String values[]={DatasetName};
		String response=WSsecure.post(createDatasetURL, keys, values);		
		return parseVuid(response);
	}

	/**
	 * adds Files to Dataset
	 * @param lfns Logical File Names
	 * @param guids Grid Unique Identfiers
	 * @param vuid Version Unique Identifier of the Dataset to add the Files
	 */
	public boolean addLFNtoDataset(String[] lfns, String[] guids, String vuid) throws IOException
	{	
		if (lfns.length!=guids.length) 
			throw new IOException("Number of LFN's must be the same as Number of GUID's. " +
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
		
		WSsecure.post(addFilesToDatasetURL, keys, values);
		return true;
	}	

	/**
	 * deletes a Dataset
	 * @param lfn Logical File Name of the Dataset to erase
	 */
	public boolean deleteDataset(String lfn) throws IOException
	{
		String keys[]= {"lfn","delete"};
		String values[] = {lfn,"yes"};
		WSsecure.get(deleteDatasetURL, keys, values);
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
		WSsecure.post(locationDatasetURL, keys, values);
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
	 * @param toParse String output from dq webserice access
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
	
	public static void main (String[] args)
	{
		DQ2Access myacc=new DQ2Access(null,0,null,0);
    Debug.debug(myacc.parseVuid(
        "dsjhfagsdkjhf vuid: '45348fe3-4564-ffee-34ef-aef455678efe' hghkjgj"), 2);
		String [] fud=myacc.parseVuids("{'acsadfwedwed.wefwefwef-wefwrf3234r4':{'duid':'754389ef-bb55-7654-98ef-76549870fe43','vuids':['754389ef-bb55-7654-98ef-76549870fe43','754389ef-bb55-7654-98ef-76549870fe43']},{'csadfwedwed.wefwefwef-wefwrf3234r4':{'duid':'754389ef-bb55-7654-98ef-76549870fe43','vuids':['754389ef-bb55-7654-98ef-76549870fe43','754389ef-bb55-7654-98ef-76549870fe43']},{'csadfwedwed.wefwefwef-wefwrf3234r4':{'duid':'754389ef-bb55-7654-98ef-76549870fe43','vuids':['754389ef-bb55-7654-98ef-76549870fe43','754389ef-bb55-7654-98ef-76549870fe43']},{'csadfwedwed.wefwefwef-wefwrf3234r4':{'duid':'754389ef-bb55-7654-98ef-76549870fe43','vuids':['754389ef-bb55-7654-98ef-76549870fe43','754389ef-bb55-7654-98ef-76549870fe43']},}");
    Debug.debug(""+fud.length, 2);
		for (int q=0; q<fud.length; q++)
		{
      Debug.debug(fud[q], 2);
		}
	}
}
