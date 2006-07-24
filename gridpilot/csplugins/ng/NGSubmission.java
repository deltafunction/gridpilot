package gridpilot.csplugins.ng;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;
import org.nordugrid.gridftp.ARCGridFTPJob;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.is.ARCDiscoveryException;
import org.nordugrid.matcher.Matcher;
import org.nordugrid.matcher.SimpleMatcher;
import org.nordugrid.model.ARCResource;

import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LogFile;
import gridpilot.GridPilot;

/**
 * Submission class for the NorduGrid plugin.
 * <p><a href="NGSubmission.java.html">see sources</a>
 */

public class NGSubmission{
 
  private LogFile logFile;
  private String csName;
  private String [] clusters = null;
  private int clusterIndex = 0;
  private ARCResource [] resources = null;
  private int resourceIndex = 0;
  NGScriptGenerator scriptGenerator;

  public NGSubmission(String _csName, String [] _clusters){
    Debug.debug("Loading class NGSubmission", 3);
    logFile = GridPilot.getClassMgr().getLogFile();
    csName = _csName;
    clusters = _clusters;
    scriptGenerator =  new NGScriptGenerator(csName);
  }

  public NGSubmission(String _csName, ARCResource [] _resources){
    Debug.debug("Loading class NGSubmission", 3);
    logFile = GridPilot.getClassMgr().getLogFile();
    csName = _csName;
    resources = _resources;
    scriptGenerator =  new NGScriptGenerator(csName);
  }

  public boolean submit(JobInfo job, String scriptName, String xrslName) throws ARCDiscoveryException,
  MalformedURLException{

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

  private String submit(final JobInfo job, final String xrslFileName) throws ARCDiscoveryException,
  MalformedURLException{
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
    String submissionHost = null;
    if(resources==null){
      // No information system.
      // extremely simple brokering, but without the information system
      // and only a list of hosts we cannot do much else.
      submissionHost = clusters[clusterIndex];
      if(clusterIndex>clusters.length-1){
        clusterIndex = 0;
      }
      else{
        ++clusterIndex;
      }
      // add gsiftp:// if not there
      if(submissionHost.startsWith("ldap://")){
        try{
          submissionHost =(new GlobusURL(submissionHost)).getHost();
        }
        catch(MalformedURLException e){
          throw new MalformedURLException("ERROR: host could not be parsed from "+submissionHost);
        }
      }
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
        
        /*String testXrsl = "&(executable=/bin/echo)(jobName=\"Jarclib test submission \")" +
        "(action=request)(arguments=\"/bin/echo\" \"Test\")"+
        "(join=yes)(stdout=out.txt)(outputfiles=(\"test\" \"\"))(queue=\"" +
         "short" + "\")";*/
        
        gridJob.submit(xrsl);
        ngJobId = gridJob.getGlobalId();
        Debug.debug("NGJobId: " + ngJobId, 3);
      }
      catch(ARCGridFTPJobException ae){
        logFile.addMessage("ARCGridFTPJobException during submission of " + xrslFileName + ":\n" +
                           "\tException\t: " + ae.getMessage(), ae);
        ae.printStackTrace();
      }
    }
    else{
      // Use information system
      Matcher matcher = new SimpleMatcher();
      try{
        if(matcher.isResourceSuitable(xrsl, resources[resourceIndex])){
          submissionHost = resources[resourceIndex].getClusterName();
          if(resourceIndex>resources.length-1){
            resourceIndex = 0;
          }
          else{
            ++resourceIndex;
          }
          Debug.debug("Submitting to "+ submissionHost, 2);
          ARCGridFTPJob gridJob = new ARCGridFTPJob(submissionHost);
          GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
          GlobusCredential globusCred = null;
          if(credential instanceof GlobusGSSCredentialImpl){
            globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
          }
          gridJob.addProxy(globusCred);
          gridJob.connect();
          gridJob.submit(xrsl);
          ngJobId = gridJob.getGlobalId();
          Debug.debug("NGJobId: " + ngJobId, 3);
        }
      }
      catch(ARCGridFTPJobException ae){
        logFile.addMessage("ARCGridFTPJobException during submission of " + xrslFileName + ":\n" +
            "\tException\t: " + ae.getMessage(), ae);
        ae.printStackTrace();
      }
    }
    return ngJobId;
  }
}