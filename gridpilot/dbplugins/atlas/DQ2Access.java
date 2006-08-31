package gridpilot.dbplugins.atlas;

import java.io.IOException;

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
	

	private String parseVuid(String toParse)
	{
		return "dfds";
	}

	private String[] parseVuids(String toParse)
	{
		String[] a=null;
		return a;
	}
}
