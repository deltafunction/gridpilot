package gridpilot.csplugins.ng;

import java.io.*;
import java.net.URL;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;
import org.nordugrid.gridftp.ARCGridFTPJob;
import org.nordugrid.gridftp.ARCGridFTPJobException;

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
  private String workingDir;

  public NGSubmission(String _csName, String _workingDir){
    Debug.debug("Loading class NGSubmission", 3);
    workingDir = _workingDir;
    configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();
    csName = _csName;
    submissionHosts = configFile.getValue(csName, "Submission hosts");
    Debug.debug("hosts : " + submissionHosts, 3);
  }

  public boolean submit(JobInfo job){

    NGScriptGenerator scriptGenerator =  new NGScriptGenerator(csName);
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

    Debug.debug("Submitting in "+ workingDir, 3);

    String xrslName = workingDir + "/" + job.getName() + ".xrsl";
    String scriptName = workingDir + "/" + job.getName() + ".job";

    if(!scriptGenerator.createXRSL(job, scriptName, xrslName, !withStdErr)){
      logFile.addMessage("Cannot create scripts for job " + job.getName() +
                              " on " + csName);
      return false;
    }

    String NGJobId = submit(xrslName);
    if(NGJobId==null){
      job.setJobStatus(NGComputingSystem.NG_STATUS_ERROR);
      return false;
    }
    else{
      job.setJobId(NGJobId);
      return true;
    }

  }

  private String submit(String xrslFileName){

   String NGJobId;
   try{
     Debug.debug("Reading file "+xrslFileName, 3);
     BufferedReader in = new BufferedReader(
       new InputStreamReader((new URL("file:"+xrslFileName)).openStream()));
     String xrsl = "";
     String line;
     int lineNumber = 0;
     while((line=in.readLine())!=null){
       ++lineNumber;
       xrsl += line+"\n";
     }
     in.close();
     
     // TODO: use all hosts from submissionHosts
     String submissionHost = Util.split(submissionHosts)[0];
     ARCGridFTPJob gridJob = new ARCGridFTPJob(submissionHost);
     GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
     GlobusCredential globusCred = null;
     if(credential instanceof GlobusGSSCredentialImpl){
       globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
     }
     gridJob.addProxy(globusCred);
     gridJob.submit(xrsl);
     NGJobId = gridJob.getGlobalId();
     Debug.debug("NGJobId : " + NGJobId, 3);
    }
    catch(ARCGridFTPJobException ae){
      logFile.addMessage("ARCGridFTPJobException during submission of " + xrslFileName + ":\n" +
                         "\tException\t: " + ae.getMessage(), ae);
      ae.printStackTrace();
      return null;
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during submission of " + xrslFileName + ":\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
      return null;
    }
    return NGJobId;
  }
}