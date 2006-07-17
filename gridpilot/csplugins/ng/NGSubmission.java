package gridpilot.csplugins.ng;

import java.io.*;
import java.net.URL;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;
import org.nordugrid.gridftp.ARCGridFTPJob;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.is.ARCDiscovery;
import org.nordugrid.is.ARCDiscoveryException;

import gridpilot.ConfigFile;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LogFile;
import gridpilot.GridPilot;
import gridpilot.Util;

/**
 * Submission class for the NorduGrid plugin.
 * <p><a href="NGSubmission.java.html">see sources</a>
 */

public class NGSubmission{
 
  private ConfigFile configFile;
  private LogFile logFile;
  private String csName;
  private String submissionHosts;
  //private String workingDir;
  ARCDiscovery arcDiscover;
  NGScriptGenerator scriptGenerator;

  public NGSubmission(String _csName){
    Debug.debug("Loading class NGSubmission", 3);
    configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();
    csName = _csName;
    submissionHosts = configFile.getValue(csName, "Submission hosts");
    Debug.debug("hosts : " + submissionHosts, 3);
    scriptGenerator =  new NGScriptGenerator(csName);
  }

  public boolean submit(JobInfo job, String scriptName, String xrslName) throws ARCDiscoveryException{

    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    
    String finalStdErr = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdOut = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    // The default is now to create both stderr if either
    // final destination is specified for stderr or final destination is speficied for
    // neither stdout nor stderr
    boolean withStdErr = finalStdErr!=null && finalStdErr.trim().length()>0 ||
       (finalStdErr==null || finalStdErr.equals("")) &&
       (finalStdOut==null || finalStdOut.equals(""));
    Debug.debug("stdout/err: "+finalStdOut+":"+finalStdErr, 3);

    if(!scriptGenerator.createXRSL(job, scriptName, xrslName, !withStdErr)){
      logFile.addMessage("Cannot create scripts for job " + job.getName() +
                              " on " + csName);
      return false;
    }
    String ngJobId = submit(job, xrslName);
    if(ngJobId==null){
      job.setJobStatus(NGComputingSystem.NG_STATUS_ERROR);
      return false;
    }
    else{
      job.setJobId(ngJobId);
      return true;
    }
  }

  private String submit(final JobInfo job, final String xrslFileName) throws ARCDiscoveryException{
    String ngJobId = null;
    String xrsl = "";
    BufferedReader in = null;
    try{
      Debug.debug("Reading file "+xrslFileName, 3);
      in = new BufferedReader(
        new InputStreamReader((new URL("file:"+xrslFileName)).openStream()));
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during submission of " + xrslFileName + ":\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
      return null;
    }
    try{
      String line;
      int lineNumber = 0;
      while((line=in.readLine())!=null){
        ++lineNumber;
        line = line.replaceAll("\\r", "");
        line = line.replaceAll("\\n", "");
        xrsl += line;
        Debug.debug("XRSL: "+xrsl, 3);
      }
      in.close();
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during submission of " + xrslFileName + ":\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
      return null;
    }     
    if(submissionHosts==null || submissionHosts.equals("")){
      arcDiscover = new ARCDiscovery("ldap://index4.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
      arcDiscover.addGIIS("ldap://index1.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
      arcDiscover.addGIIS("ldap://index2.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
      arcDiscover.addGIIS("ldap://index3.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
      arcDiscover.discoverAll();
      if(arcDiscover.getClusters().size()==0){
        Debug.debug("ERROR: No clusters found!", 1);
        throw new ARCDiscoveryException("ERROR: No clusters found!");
      }
      if(arcDiscover.getSEs().size()==0){
        Debug.debug("WARNING: Discovery has found no storage elements!", 1);
      }
      Object [] hosts = arcDiscover.getClusters().toArray();
      submissionHosts = Util.arrayToString(hosts);
    }
    // TODO: brokering: use all hosts
    String submissionHost = Util.split(submissionHosts)[0];
    // add gsiftp:// if not there
    if(!submissionHost.startsWith("gsiftp://")){
      submissionHost = "gsiftp://"+submissionHost+":2811/jobs";
    }
    try{
      ARCGridFTPJob gridJob = new ARCGridFTPJob(submissionHost);
      GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
      GlobusCredential globusCred = null;
      if(credential instanceof GlobusGSSCredentialImpl){
        globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
      }
      gridJob.addProxy(globusCred);
      gridJob.connect();
      
      String testXrsl = "&(executable=/bin/echo)(jobName=\"Jarclib test submission \")" +
      "(action=request)(arguments=\"/bin/echo\" \"Test\")"+
      "(join=yes)(stdout=out.txt)(outputfiles=(\"test\" \"\"))(queue=\"" +
       "short" + "\")";
      
      gridJob.submit(xrsl);
      ngJobId = gridJob.getGlobalId();
      Debug.debug("NGJobId: " + ngJobId, 3);
    }
    catch(ARCGridFTPJobException ae){
      logFile.addMessage("ARCGridFTPJobException during submission of " + xrslFileName + ":\n" +
                         "\tException\t: " + ae.getMessage(), ae);
      ae.printStackTrace();
      return null;
    }
    return ngJobId;
  }
}