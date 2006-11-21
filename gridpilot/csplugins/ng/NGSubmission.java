package gridpilot.csplugins.ng;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

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
import gridpilot.Util;

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
  private NGScriptGenerator scriptGenerator;
  
  private static int MAX_SUBMIT_RETRIES = 7;

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
  MalformedURLException, IOException{

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

    List files = scriptGenerator.createXRSL(job, scriptName, xrslName, !withStdErr);
    // Now the short file names
    List fileNames = new Vector();
    String file;
    for(Iterator it=files.iterator(); it.hasNext();){
      file = (new File(it.next().toString())).getName();
      fileNames.add(file);
    }
    
    if(files==null){
      logFile.addMessage("Cannot create scripts for job " + job.getName() +
                              " on " + csName);
      return false;
    }
    String ngJobId = null;
    try{
      ngJobId = submit(job, xrslName, files, fileNames);
    }
    catch(Exception e){
      e.printStackTrace();
      logFile.addMessage("Cannot submit job " + job.getName() +
          " to " + csName, e);
    }
    if(ngJobId==null){
      job.setJobStatus(NGComputingSystem.NG_STATUS_ERROR);
      return false;
    }
    else{
      job.setJobId(ngJobId);
      return true;
    }
  }

  private String submit(final JobInfo job, final String xrslFileName,
      final List files, final List fileNames) throws ARCDiscoveryException,
  MalformedURLException, ARCGridFTPJobException, IOException{
    
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
      throw ioe;
    }
    try{
      String line;
      int lineNumber = 0;
      while((line=in.readLine())!=null){
        ++lineNumber;
        line = line.replaceAll("\\r", "");
        line = line.replaceAll("\\n", "");
        xrsl += line;
        Debug.debug("XRSL: "+line, 3);
      }
      in.close();
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during submission of " + xrslFileName + ":\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
      throw ioe;
    }
    String submissionHost = null;
    String queue = null;
    if(resources==null){
      // No information system.
      // Extremely simple brokering, but without the information system
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
      // TODO: don't know if this will work
      queue = "";
    }
    else{
      // Use information system
      Matcher matcher = new SimpleMatcher();
      for(int i=0; i<resources.length; ++i){
        try{
          // Very simple brokering.
          // If resource is suitable and has free slots, the job
          // is submitted, otherwise try next resource.
          // If no free slots are found, submit to the resource
          // with highest number of CPUs
          if(matcher.isResourceSuitable(xrsl, resources[resourceIndex]) &&
              resources[resourceIndex].getFreejobs()>0){
            submissionHost = resources[resourceIndex].getClusterName();
            queue = resources[resourceIndex].getQueueName();
            break;
          }
          else{
            if(resourceIndex==resources.length-1){
              resourceIndex = 0;
            }
            else{
              ++resourceIndex;
            }
          }
        }
        catch(ARCGridFTPJobException ae){
          logFile.addMessage("ARCGridFTPJobException during submission of " + xrslFileName + ":\n" +
              "\tException\t: " + ae.getMessage(), ae);
          throw ae;
        }
      }
      if(submissionHost==null){
        // no free slots found, take best bet
        ARCResource resource = null;
        for(int i=0; i<resources.length; ++i){
          Debug.debug("Checking resource "+resources[i].getClusterName()+
              " : "+Util.arrayToString(resources[i].getRuntimeenvironment().toArray()), 2);
          if(matcher.isResourceSuitable(xrsl, resources[i]) &&
             (resource==null ||
             resources[i].getMaxjobs()>resource.getMaxjobs() &&
             resources[i].getTotalQueueCPUs()>resource.getTotalQueueCPUs())){
            resource = resources[i];
          }
        }
        if(resource!=null){
          submissionHost = resource.getClusterName();
        }
      }
      if(submissionHost==null){
        throw new ARCDiscoveryException("No suitable clusters found.");
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
      Debug.debug("Submitting to "+ submissionHost, 2);

      /*String testXrsl = "&(executable=/bin/echo)(jobName=\"Jarclib test submission \")" +
      "(action=request)(arguments=\"/bin/echo\" \"Test\")"+
      "(join=yes)(stdout=out.txt)(outputfiles=(\"test\" \"\"))(queue=\"" +
       "short" + "\")";*/
      
      xrsl = xrsl.replaceFirst("\\(\\*(?i)queue=\"_submitqueue_\"\\*\\)",
          "(queue=\""+queue+"\")");
      xrsl = xrsl.replaceFirst("\\(\\*(?i)action=\"request\"\\*\\)",
          "(action=\"request\")");
      // Since cpuTime has been used to find queue, we no longer need it
      xrsl = xrsl.replaceFirst("\\((?i)cputime=.*\\)\\(\\*endCpu\\*\\)", "");
      
      Debug.debug("Submittig with input files: "+Util.arrayToString(files.toArray()), 2);
      Debug.debug("Submittig with input files: "+Util.arrayToString(fileNames.toArray()), 2);

      int i = 0;
      ARCGridFTPJob gridJob = null;
      while(true){
        try{
          gridJob = new ARCGridFTPJob(submissionHost);
          GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
          GlobusCredential globusCred = null;
          if(credential instanceof GlobusGSSCredentialImpl){
            globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
          }
          gridJob.addProxy(globusCred);
          gridJob.connect();             
          gridJob.submit(xrsl, files, fileNames);       
          ngJobId = gridJob.getGlobalId();
          Debug.debug("NG Job Id: " + ngJobId, 3);
          gridJob.disconnect();
          break;
        }
        catch(ARCGridFTPJobException ae){
          gridJob.disconnect();
          logFile.addMessage("ARCGridFTPJobException during submission of " + xrslFileName + ":\n" +
              "\tException\t: " + ae.getMessage(), ae);
          if(i>MAX_SUBMIT_RETRIES){
            Debug.debug("WARNING: could not submit job", 1);
            break;
          }
          else{
            Debug.debug("WARNING: problem submitting, retrying", 2);
            ++i;
            try{
              Thread.sleep(5000);
            }
            catch(Exception ee){
              break;
            }
            continue;
          }
        }
      }
    }
    return ngJobId;
  }
}