package gridpilot.csplugins.ng;

import java.io.*;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.Vector;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;
import org.nordugrid.gridftp.ARCGridFTPJob;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.is.ARCDiscovery;
import org.nordugrid.model.ARCJob;
import org.nordugrid.model.ARCResource;
import org.nordugrid.multithread.TaskResult;

import gridpilot.ComputingSystem;
import gridpilot.ConfigFile;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LocalStaticShellMgr;
import gridpilot.LogFile;
import gridpilot.GridPilot;
import gridpilot.StatusBar;
import gridpilot.Util;
import gridpilot.ftplugins.gsiftp.GSIFTPFileTransfer;

/**
 * Main class for the NorduGrid plugin. <br>
 * <p><a href="NGComputingSystem.java.html">see sources</a>
 */

public class NGComputingSystem implements ComputingSystem{

  public static final String NG_STATUS_ACCEPTED =  "ACCEPTED";
  public static final String NG_STATUS_PREPARING = "PREPARING" ;
  public static final String NG_STATUS_FINISHING = "FINISHING" ;
  public static final String NG_STATUS_FINISHED = "FINISHED" ;
  public static final String NG_STATUS_DELETED = "DELETED" ;
  public static final String NG_STATUS_CANCELLING = "CANCELLING";
  public static final String NG_STATUS_SUBMITTING = "SUBMITTING";
  public static final String NG_STATUS_INLRMSQ = "INLRMS: Q";
  public static final String NG_STATUS_INLRMSR = "INLRMS: R";
  public static final String NG_STATUS_INLRMSE = "INLRMS: E";

  public static final String NG_STATUS_FAILURE = "FAILURE";
  public static final String NG_STATUS_FAILED = "FAILED";
  public static final String NG_STATUS_ERROR = "ERROR";

  private NGSubmission ngSubmission;
  private Boolean gridProxyInitialized = Boolean.FALSE;
  private static String csName;
  private static LogFile logFile;
  private String workingDir;
  private ARCDiscovery arcDiscovery;
  private String defaultUser;
  private String error = "";
  private boolean useInfoSystem = false;
  private String [] clusters;
  private String [] giises;
  private ARCResource [] resources;
  private String runtimeDB = null;
  private HashSet finalRuntimes = null;
  
  public NGComputingSystem(String _csName){
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    csName = _csName;
    workingDir = configFile.getValue(csName, "working directory");
    if(workingDir==null || workingDir.equals("")){
      workingDir = "~";
    }
    else if(!workingDir.toLowerCase().startsWith("c:") &&
        !workingDir.startsWith("/") && !workingDir.startsWith("~")){
      workingDir = "~"+File.separator+workingDir;
    }
    if(workingDir.startsWith("~")){
      workingDir = System.getProperty("user.home")+workingDir.substring(1);
    }
    if(workingDir.endsWith("/") || workingDir.endsWith("\\")){
      workingDir = workingDir.substring(0, workingDir.length()-1);
    }
    Debug.debug("Working dir: "+workingDir, 2);

    logFile = GridPilot.getClassMgr().getLogFile();  
    
    defaultUser = configFile.getValue("GridPilot", "user");
    String useInfoSys = configFile.getValue(csName, "use information system");
    useInfoSystem = useInfoSys.equalsIgnoreCase("true") || useInfoSys.equalsIgnoreCase("yes");
    clusters = configFile.getValues(csName, "clusters");
    giises = configFile.getValues(csName, "giises");
       
    arcDiscovery = new ARCDiscovery();

    // restrict to these clusters
    if(clusters!=null && clusters.length!=0){
      String cluster = null;
      Set clusterSet = new HashSet();
      for(int i=0; i<clusters.length; ++i){
        if(!clusters[i].startsWith("ldap://")){
          cluster = "ldap://"+clusters[i]+
          ":2135/nordugrid-cluster-name="+clusters[i]+",Mds-Vo-name=local,o=grid";
        }
        else{
          cluster = clusters[i];
        }
        clusterSet.add(cluster);
        Debug.debug("Adding cluster "+cluster, 3);
      }
      arcDiscovery.setClusters(clusterSet);
    }
    else{
      if(giises!=null && giises.length!=0){
        // If GIISes given, use them
        String giis = null;
        for(int i=0; i<giises.length; ++i){
          if(!giises[i].startsWith("ldap://")){
            giis = "ldap://"+giises[i]+":2135/Mds-Vo-Name=NorduGrid,O=Grid";
          }
          else{
            giis = giises[i];
          }
          Debug.debug("Adding GIIS "+giis, 3);
          arcDiscovery.addGIIS(giis);
        }
      }
      else{
        // TODO: implement true hierarchy: query these top-level GIISes and
        // find all lower-level ones. Try to submit using lowest one.
        // If no free slots, try higher up, etc.
        // Use default set of GIISes
        arcDiscovery.addGIIS("ldap://index4.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
        //arcDiscovery.addGIIS("ldap://index1.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
        //arcDiscovery.addGIIS("ldap://index2.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
        //arcDiscovery.addGIIS("ldap://index3.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
      }
    }
    
    if(useInfoSystem){
      // Use information system
      GridPilot.splashShow("Discovering NG ARC resources...");
      arcDiscovery.discoverAll();
      Set clusterSet = arcDiscovery.getClusters();
      clusters = new String[clusterSet.size()];
      for(int i=0; i<clusterSet.size(); ++i){
        try{
          clusters[i]= (new GlobusURL(clusterSet.toArray()[i].toString())).getHost();
          Debug.debug("Added cluster "+clusters[i], 2);
        }
        catch(MalformedURLException mue){
          mue.printStackTrace();
        }
      }
      GridPilot.splashShow("Finding authorized NG ARC resources...");
      resources = findAuthorizedResourcesFromIS();
      ngSubmission = new NGSubmission(csName, resources);
    }
    else{
      ngSubmission = new NGSubmission(csName, clusters);
    }
    
    Debug.debug("Clusters: "+Util.arrayToString(clusters), 2);
    
    try{
      runtimeDB = GridPilot.getClassMgr().getConfigFile().getValue(
          csName, "runtime database");
    }
    catch(Exception e){
      Debug.debug("ERROR getting runtime database: "+e.getMessage(), 1);
    }
    if(runtimeDB!=null && !runtimeDB.equals("")){
      setupRuntimeEnvironments(csName);
    }    
  }
  
  /**
   * The runtime environments are simply found from the
   * information system.
   */
  public void setupRuntimeEnvironments(String csName){
    Set runtimes = null;
    DBPluginMgr dbPluginMgr = null;      
    try{
      dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(
          runtimeDB);
    }
    catch(Exception e){
      Debug.debug("WARNING: could not load runtime DB "+runtimeDB, 1);
      return;
    }
    finalRuntimes = new HashSet();
    if(useInfoSystem){
      Object rte = null;
      for(int i=0; i<resources.length; ++i){
        try{
          runtimes = resources[i].getRuntimeenvironment();
          for(Iterator it=runtimes.iterator(); it.hasNext();){
            rte = it.next();
            Debug.debug("Adding runtime environment: "+rte.toString()+":"+rte.getClass(), 3);
            finalRuntimes.add(rte);
          }
        }
        catch(Exception ae){
          ae.printStackTrace();
        }
      }
      String [] runtimeEnvironmentFields =
        dbPluginMgr.getFieldNames("runtimeEnvironment");
      String [] rtVals = new String [runtimeEnvironmentFields.length];
      String name = null;
      for(Iterator it=finalRuntimes.iterator(); it.hasNext();){       
        name = (String) it.next();
        for(int i=0; i<runtimeEnvironmentFields.length; ++i){
          if(runtimeEnvironmentFields[i].equalsIgnoreCase("name")){
            rtVals[i] = name;
          }
          else if(runtimeEnvironmentFields[i].equalsIgnoreCase("computingSystem")){
            rtVals[i] = csName;
          }
          else{
            rtVals[i] = "";
          }
        }
        try{
          // only create if there is not already a record with this name
          // and csName
          if(dbPluginMgr.getRuntimeEnvironmentID(name, csName).equals("-1")){
            if(!dbPluginMgr.createRuntimeEnvironment(rtVals)){
              finalRuntimes.remove(name);
            }
          }
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
    }
    else{
      // Can't do anything. The user will have to set up runtime environments by hand.
    }
  }

    /*
   * Local directory to keep xrsl, shell script and temporary copies of stdin/stdout
   */
  protected String runDir(JobInfo job){
    return workingDir+File.separator+job.getName();
  }

  public boolean submit(JobInfo job) {
    Debug.debug("submitting..."+gridProxyInitialized, 3);
    String scriptName = runDir(job) + File.separator + job.getName() + ".job";
    String xrslName = runDir(job) + File.separator + job.getName() + ".xrsl";
    try{
      return ngSubmission.submit(job, scriptName, xrslName);
    }
    catch(Exception e){
      error = e.getMessage();
      e.printStackTrace();
      logFile.addMessage("Exception during submission of " + job.getName() + ":\n" +
          "\tException\t: " + e.getMessage(), e);
      return false;
    }
  }


  public void updateStatus(Vector jobs){
    for(int i=0; i<jobs.size(); ++i){
      updateStatus((JobInfo) jobs.get(i));
    }
  }

  private int updateStatus(JobInfo job){
    
    boolean doUpdate = false;
    String jobId = job.getJobId();
    if(jobId!=null){ // job already submitted
      String statusLine;
      statusLine = getFullStatus(job);
      if(statusLine!=null){
        doUpdate = extractStatus(job, statusLine);
      }
      else{
        doUpdate = false;//true;
      }
    }

    if(doUpdate){
      Debug.debug("Updating status of job "+job.getName(), 2);
      if(job.getJobStatus()==null){
        Debug.debug("No status found for job "+job.getName(), 2);
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      else if(job.getJobStatus().equals(NG_STATUS_FINISHED)){
        try{
          if(getOutput(job)){
            job.setInternalStatus(ComputingSystem.STATUS_DONE);
          }
          else{
            job.setInternalStatus(ComputingSystem.STATUS_ERROR);
          }
        }
        catch(Exception e){
          job.setInternalStatus(ComputingSystem.STATUS_ERROR);
        }
      }
      else if(job.getJobStatus().equals(NG_STATUS_FAILURE)){
        //getOutput(job);
        job.setInternalStatus(ComputingSystem.STATUS_FAILED);
      }
      else if(job.getJobStatus().equals(NG_STATUS_ERROR)){
        // try to clean up, just in case...
        //getOutput(job);
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      else if(job.getJobStatus().equals(NG_STATUS_DELETED)){
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      else if(job.getJobStatus().equals(NG_STATUS_FAILED)){
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      else if(job.getJobStatus().equals(NG_STATUS_INLRMSR)){
        job.setInternalStatus(ComputingSystem.STATUS_RUNNING);
      }
      //job.setInternalStatus(ComputingSystem.STATUS_WAIT);
      else{
        Debug.debug("WARNING: unknown status: "+job.getJobStatus(), 1);
        job.setInternalStatus(ComputingSystem.STATUS_WAIT);
      }
    }
    return job.getInternalStatus();
  }
  
  private ARCGridFTPJob getGridJob(JobInfo job) throws ARCGridFTPJobException{
    
    String jobID = job.getJobId().substring(job.getJobId().lastIndexOf("/"));
    int lastSlash = job.getJobId().lastIndexOf("/");
    if(lastSlash>-1){
      jobID = job.getJobId().substring(lastSlash + 1);
    }
    String submissionHost;
    try{
      submissionHost = "gsiftp://"+(new GlobusURL(job.getJobId())).getHost()+":2811/jobs";
    }
    catch(MalformedURLException e){
      error = "ERROR: host could not be parsed from "+job.getJobId();
      Debug.debug(error, 1);
      e.printStackTrace();
      throw new ARCGridFTPJobException(error);
    }
    Debug.debug("Getting job "+submissionHost +" : "+ jobID, 3);
    ARCGridFTPJob gridJob = new ARCGridFTPJob(submissionHost, jobID);
    GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
    GlobusCredential globusCred = null;
    if(credential instanceof GlobusGSSCredentialImpl){
      globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
    }
    gridJob.addProxy(globusCred);
    gridJob.connect();
    return gridJob;
  }
  
  public boolean killJobs(Vector jobsToKill){
    JobInfo job = null;
    Vector errors = new Vector();
    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      try{
        job = (JobInfo) en.nextElement();
        Debug.debug("Cleaning : " + job.getName() + ":" + job.getJobId(), 3);
        ARCGridFTPJob gridJob = getGridJob(job);
        gridJob.cancel();
        gridJob.clean();
      }
      catch(Exception ae){
        errors.add(ae.getMessage());
        logFile.addMessage("Exception during killing of " + job.getName() + ":" + job.getJobId() + ":\n" +
                           "\tException\t: " + ae.getMessage(), ae);
        ae.printStackTrace();
      }
    }    
    if(errors.size()!=0){
      error = Util.arrayToString(errors.toArray());
      return false;
    }
    else{
      return true;
    }
  }

  public void clearOutputMapping(JobInfo job){
    
    GSIFTPFileTransfer gsiftpFileTransfer = GridPilot.getClassMgr().getGSIFTPFileTransfer();
    
    // Clean job off grid. - just in case...
    try{
      ARCGridFTPJob gridJob = getGridJob(job);
      gridJob.cancel();
    }
    catch(Exception e){
      Debug.debug("Could not cancel job. Probably finished. "+
          job.getName()+". "+e.getMessage(), 3);
      e.printStackTrace();
    }
    try{
      ARCGridFTPJob gridJob = getGridJob(job);
      gridJob.clean();
    }
    catch(Exception e){
      Debug.debug("Could not clean job. Probably already deleted. "+
          job.getName()+". "+e.getMessage(), 3);
      e.printStackTrace();
    }
    
    // Delete files that may have been copied to storage elements
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String[] outputMapping = dbPluginMgr.getOutputMapping(job.getJobDefId());
    String fileName;
    for(int i=0; i<outputMapping.length/2-1; ++i){
      try{
        fileName = Util.addFile(outputMapping[2*i+1]);
        gsiftpFileTransfer.deleteFile(new GlobusURL(fileName));
      }
      catch(Exception e){
        error = "WARNING: could not delete output file. "+e.getMessage();
        Debug.debug(error, 3);
      }
    }
    // Delete stdout/stderr that may have been copied to final destination
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      try{
        gsiftpFileTransfer.deleteFile(new GlobusURL(finalStdOut));
      }
      catch(Exception e){
        error = "WARNING: could not delete "+finalStdOut+". "+e.getMessage();
        Debug.debug(error, 2);
      }
      catch(Throwable e){
        error = "WARNING: could not delete "+finalStdOut+". "+e.getMessage();
        Debug.debug(error, 2);
      }
    }
    if(finalStdErr!=null && finalStdErr.trim().length()>0){
      try{
        gsiftpFileTransfer.deleteFile(new GlobusURL(finalStdErr));
      }
      catch(Exception e){
        error = "WARNING: could not delete "+finalStdErr+". "+e.getMessage();
        Debug.debug(error, 2);
      }
      catch(Throwable e){
        error = "WARNING: could not delete "+finalStdErr+". "+e.getMessage();
        Debug.debug(error, 2);
      }
    }
    
    // Delete the local run directory
    String runDir = runDir(job);
    try{
      Debug.debug("Deleting runtime directory "+runDir, 2);
      LocalStaticShellMgr.deleteDir(new File(runDir));
    }
    catch(Exception e){
      error = "WARNING: could not delete "+runDir+". "+e.getMessage();
      Debug.debug(error, 2);
    }

  }

  public void exit(){
    String runtimeName = null;
    String initText = null;
    String id = "-1";
    boolean ok = true;
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(
        runtimeDB);
    for(Iterator it=finalRuntimes.iterator(); it.hasNext();){
      ok = true;
      runtimeName = (String )it.next();
      // Don't delete records with a non-empty initText.
      // These can only have been created by hand.
      initText = dbPluginMgr.getRuntimeInitText(runtimeName, csName);
      if(initText!=null && !initText.equals("")){
        continue;
      }
      id = dbPluginMgr.getRuntimeEnvironmentID(runtimeName, csName);
      if(!id.equals("-1")){
        ok = dbPluginMgr.deleteRuntimeEnvironment(id);
      }
      else{
        ok = false;
      }
      if(!ok){
        Debug.debug("WARNING: could not delete runtime environment " +
            runtimeName+
            " from database "+
            dbPluginMgr.getDBName(), 1);
      }
    }
  }
  
  private boolean getOutput(JobInfo job){
    
    String jobID = null;
    int lastSlash = job.getJobId().lastIndexOf("/");
    if(lastSlash>-1){
      jobID = job.getJobId().substring(lastSlash + 1);
    }
    String dirName = runDir(job)+File.separator+jobID;
    
    // After a crash some unfinished downloads may be around.
    // Move away before downloading.
    try{
      // current date and time
      SimpleDateFormat dateFormat = new SimpleDateFormat(GridPilot.dateFormatString);
      dateFormat.setTimeZone(TimeZone.getDefault());
      String dateString = dateFormat.format(new Date());
      LocalStaticShellMgr.moveFile(dirName, dirName+"."+dateString);
    }
    catch(Exception ioe){
      error = "Exception during job " + job.getName() + " get output :\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
      return false;
    }
    
    // Get the outputs
    try{
      Debug.debug("Getting : " + job.getName() + ":" + job.getJobId(), 3);
      ARCGridFTPJob gridJob = getGridJob(job);
      gridJob.get(dirName);
     }
     catch(Exception ae){
       error = "Exception during get outputs of " + job.getName() + ":" + job.getJobId() + ":\n" +
       "\tException\t: " + ae.getMessage();
       logFile.addMessage(error, ae);
       ae.printStackTrace();
     }
    
    // Rename stdout and stderr to the name specified in the job description,
    // and move them one level up
    if(job.getStdOut()!=null && !job.getStdOut().equals("")){      
      try{
        LocalStaticShellMgr.copyFile(dirName+File.separator+"stdout", job.getStdOut());
      } 
      catch(Exception ioe){
        error = "Exception during job " + job.getName() + " getFullStatus :\n" +
        "\tException\t: " + ioe.getMessage();
        logFile.addMessage(error, ioe);
        return false;
      }
    }
    if(job.getStdErr() != null && !job.getStdErr().equals("")){
      try{
        LocalStaticShellMgr.copyFile(dirName+File.separator, job.getStdErr());
      }
      catch(Exception ioe){
        error = "Exception during job " + job.getName() + " getOutput :\n" +
        "\tException\t: " + ioe.getMessage();
        logFile.addMessage(error, ioe);
        return false;
      }
    }
    return true;
  }

  public String getFullStatus(JobInfo job){
    
    String comment = "";
    String queue = "";
    String queueRank = "";
    String submissionTime = "";
    String completionTime = "";
    String proxyExpirationTime = "";
    
    String status = "";
    String input = "";
    String output = "";
    String errors = "";
    String lrmsStatus = "";
    
    if(useInfoSystem){
      Debug.debug("Using information system", 3);
      try{
        ARCJob arcJob = getJobFromIS(job);
        status = arcJob.getStatus();
        queue = arcJob.getQueue();
        queueRank = Integer.toString(arcJob.getQueueRank());
        comment = arcJob.getComment();
        errors = arcJob.getErrors();
        submissionTime = arcJob.getSubmissionTime();
        completionTime = arcJob.getCompletionTime();
        proxyExpirationTime = arcJob.getProxyExpirationTime();
      }
      catch(ARCGridFTPJobException e){
        //e.printStackTrace();
        return e.getMessage();
      }
    }
    else{
      ARCGridFTPJob gridJob;
      try{
        Debug.debug("Getting " + job.getJobId(), 3);
        gridJob = getGridJob(job);

      }
      catch(Exception ioe){
        error = "Exception during job " + job.getName() + " getFullStatus :\n" +
        "\tException\t: " + ioe.getMessage();
        logFile.addMessage(error, ioe);
        return ioe.getMessage();
      }
      
      try{
        //status = gridJob.state();
        status = gridJob.getOutputFile("log/status");
        /* remove leading whitespace */
        status = status.replaceAll("^\\s+", "");
        /* remove trailing whitespace */
        status = status.replaceAll("\\s+$", "");      

      }
      catch(Exception ioe){
        error = "Exception during job " + job.getName() + " getFullStatus :\n" +
        "\tException\t: " + ioe.getMessage();
        Debug.debug(error, 2);
        status = "";
      }
      
      try{
        input = gridJob.getOutputFile("log/input");
        input = input.replaceAll("^\\s+", "");
        input = input.replaceAll("\\s+$", "");      
        input = input.replaceAll("\\n", " ");
      }
      catch(Exception ioe){
        error = "Exception during job " + job.getName() + " getFullStatus :\n" +
        "\tException\t: " + ioe.getMessage();
        Debug.debug(error, 2);
        input = "";
      }
      
      try{
        output = gridJob.getOutputFile("log/output");
        output = output.replaceAll("^\\s+", "");
        output = output.replaceAll("\\s+$", "");      
        output = output.replaceAll("\\n", " ");
      }
      catch(Exception ioe){
        error = "Exception during job " + job.getName() + " getFullStatus :\n" +
        "\tException\t: " + ioe.getMessage();
        Debug.debug(error, 2);
        output = "";
      }
      
      try{
        errors = gridJob.getOutputFile("log/failed");
      }
      catch(Exception ioe){
        error = "Exception during job " + job.getName() + " getFullStatus :\n" +
        "\tException\t: " + ioe.getMessage();
        Debug.debug(error, 2);
        errors = "";
      }
      
      try{
        lrmsStatus = gridJob.getOutputFile("log/local");
        lrmsStatus = lrmsStatus.replaceAll("=", ": ");
      }
      catch(Exception ioe){
        error = "Exception during job " + job.getName() + " getFullStatus :\n" +
        "\tException\t: " + ioe.getMessage();
        Debug.debug(error, 3);
        lrmsStatus = "";
      }
    }
    
    String result = "";
    
    if(status!=null && !status.equals("")){
      result += "Status: "+status+"\n";
    }
    if(input!=null && !input.equals("")){
      result += "Input: "+input+"\n";
    }
    if(output!=null && !output.equals("")){
      result += "Output: "+output+"\n";
    }
    if(errors!=null && !errors.equals("")){
      result += "Error: "+errors+"\n";
    }
    if(lrmsStatus!=null && !lrmsStatus.equals("")){
      result += lrmsStatus;
    }
    
    if(comment!=null && !comment.equals("")){
      result += "Comment: "+comment+"\n";
    }
    if(queue!=null && !queue.equals("")){
      result += "Queue: "+queue+"\n";
    }
    if(queueRank!=null && !queueRank.equals("")){
      result += "Queue rank: "+queueRank+"\n";
    }
    if(submissionTime!=null && !submissionTime.equals("")){
      result += "Submission time: "+submissionTime+"\n";
    }
    if(completionTime!=null && !completionTime.equals("")){
      result += "Completion time: "+completionTime+"\n";
    }
    if(proxyExpirationTime!=null && !proxyExpirationTime.equals("")){
      result += "Proxy expiration time: "+proxyExpirationTime+"\n";
    }

    return result;
  }

  public String [] getCurrentOutputs(JobInfo job) throws IOException{
    
    String stdOutFile = job.getStdOut();
    String stdErrFile = job.getStdErr();

    // move existing files out of the way
    if(stdOutFile!=null && !stdOutFile.equals("") &&
        LocalStaticShellMgr.existsFile(stdOutFile)){
      LocalStaticShellMgr.moveFile(stdOutFile, stdOutFile+".bk");
    }
    if(stdErrFile!=null && !stdErrFile.equals("") &&
        LocalStaticShellMgr.existsFile(stdErrFile)){
      LocalStaticShellMgr.moveFile(stdErrFile, stdErrFile+".bk");
    }
    
    // if retrieval of files fails, move old files back in place
    if(!syncCurrentOutputs(job)){
      try{
        LocalStaticShellMgr.deleteFile(stdOutFile);
      }
      catch(Exception e){
      }
      try{
        LocalStaticShellMgr.deleteFile(stdErrFile);
      }
      catch(Exception e){
      }
      try{
        LocalStaticShellMgr.moveFile(stdOutFile+".bk", stdOutFile);
      }
      catch(Exception e){
      }
      try{
        LocalStaticShellMgr.moveFile(stdErrFile+".bk", stdErrFile);
      }
      catch(Exception e){
      }
    }
    // delete backup files
    else{
      try{
        LocalStaticShellMgr.deleteFile(stdOutFile+".bk");
        LocalStaticShellMgr.deleteFile(stdErrFile+".bk");
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    
    String [] res = new String[2];

    if(stdOutFile!=null && !stdOutFile.equals("")){
      // read stdout
      try{
        res[0] = LocalStaticShellMgr.readFile(stdOutFile);
       }
       catch(IOException ae){
         error = "Exception during getCurrentOutputs (stdout) for " + job.getName() + ":" + job.getJobId() + ":\n" +
         "\nException: " + ae.getMessage();
         res[0] = error;
         GridPilot.getClassMgr().getStatusBar().setLabel("ERROR: "+ae.getMessage());
         logFile.addMessage(error, ae);
         //throw ae;
       }
    }
    if(stdErrFile!=null && !stdErrFile.equals("")){
      // read stderr
      try{
        res[1] = LocalStaticShellMgr.readFile(stdErrFile);
       }
       catch(Exception ae){
         error = "Exception during getCurrentOutputs (stderr) for " + job.getName() + ":" + job.getJobId() + ":\n" +
         "\nException: " + ae.getMessage();
         //logFile.addMessage(error, ae);
         //ae.printStackTrace();
         res[1] = error;
       }
    }
    return res;
  }
  
  public ARCJob getJobFromIS(JobInfo job) throws ARCGridFTPJobException{
    Set tmpClusters = arcDiscovery.getClusters();
    Set jobClusters = new HashSet();
    String submissionHost;
    try{
      submissionHost = (new GlobusURL(job.getJobId())).getHost();
    }
    catch(MalformedURLException e){
      error = "ERROR: host could not be parsed from "+job.getJobId();
      Debug.debug(error, 1);
      e.printStackTrace();
      throw new ARCGridFTPJobException(error);
    }
    if(!submissionHost.startsWith("ldap://")){
      jobClusters.add("ldap://"+submissionHost+
          ":2135/nordugrid-cluster-name="+submissionHost+",Mds-Vo-name=local,o=grid");
    }
    else{
      jobClusters.add(submissionHost);
    }
    // restrict to the one cluster holding the job
    arcDiscovery.setClusters(jobClusters);
    ARCJob [] allJobs = findCurrentJobsFromIS();
    HashSet allJobIds = new HashSet();
    // return to including all clusters
    arcDiscovery.setClusters(tmpClusters);
    for(int i=0; i<allJobs.length; ++i){
      Debug.debug("found job "+allJobs[i].getGlobalId(), 3);
      if(allJobs[i].getGlobalId().equalsIgnoreCase(job.getJobId())){
        return allJobs[i];
      }
      allJobIds.add(allJobs[i].getGlobalId());
    }
    Debug.debug("No job found matching "+job.getJobId()+
        " The following jobs found: "+Util.arrayToString(allJobIds.toArray()), 1);
    throw new ARCGridFTPJobException("No job found matching "+job.getJobId());
  }
  
  public ARCJob [] findCurrentJobsFromIS(){
    StatusBar statusBar = GridPilot.getClassMgr().getStatusBar();

    long start = System.currentTimeMillis();
    long limit = 10000;
    long offset = 2000; // some +- coefficient
    Collection foundJobs = null;
    HashSet foundJobIDs = new HashSet();
    try{
      statusBar.setLabel("Finding jobs for "+getUserInfo(csName)+ ", please wait...");
      Debug.debug("Finding jobs for "+getUserInfo(csName)+ ", please wait...", 3);
      foundJobs = arcDiscovery.findUserJobs(getUserInfo(csName), 20, limit);
      HashSet result = null;
      Object itObj = null;
      for(Iterator it=foundJobs.iterator(); it.hasNext(); ){
        itObj = it.next();
        if(((TaskResult) itObj).getResult()==null){
          continue;
        }
        if(((TaskResult) itObj).getResult().getClass()==String.class){
          // Failed to connect to server message. Just ignore.
          continue;
        }
        result = (HashSet) ((TaskResult) itObj).getResult();
        foundJobIDs.addAll(result);
      }
      statusBar.setLabel("Finding jobs done");
    }
    catch(InterruptedException e){
      Debug.debug("User interrupt of job checking!", 2);
    }
    long end = System.currentTimeMillis();
    if((end - start) < limit + offset){
      Debug.debug("WARNING: failed to stay within time limit of "+limit/1000+" seconds. "+
          (end - start)/1000, 1);
    }
    if(foundJobs.size()==0){
      error = "WARNING: failed to find authorized queues.";
      statusBar.setLabel(error);
      Debug.debug(error, 1);
      logFile.addMessage(error + "\n\tDN\t: " + getUserInfo(csName));
    }
    ARCJob [] returnArray = new ARCJob [foundJobIDs.size()];
    int i = 0;
    Object itObj;
    for(Iterator it=foundJobIDs.iterator(); it.hasNext(); ){
      itObj = it.next();
      returnArray[i] = ((ARCJob) itObj);
      ++i;
    }
    return returnArray;
  }
  
  public ARCResource [] findAuthorizedResourcesFromIS(){
    StatusBar statusBar = GridPilot.getClassMgr().getStatusBar();
    long start = System.currentTimeMillis();
    long limit = 10000;
    long offset = 2000; // some +- coefficient
    Collection foundResources = null;
    ARCResource [] resourcesArray = null;
    try{
      statusBar.setLabel("Finding resources, please wait...");
      // a Collection of HashSets
      foundResources = arcDiscovery.findAuthorizedResources(
          getUserInfo(csName), 20, limit);
      Object itObj = null;
      HashSet tmpRes = null;
      HashSet resourcesSet = new HashSet();
      boolean clusterOK = false;
      for(Iterator it=foundResources.iterator(); it.hasNext(); ){
        itObj = it.next();
        if((((TaskResult) itObj).getResult())!=null &&
            (((TaskResult) itObj).getResult()).getClass().equals(HashSet.class)){
          tmpRes = (HashSet) ((TaskResult) itObj).getResult();
          if(tmpRes.size()>0){
            Debug.debug("Found resource : " +
                ((TaskResult) itObj).getWorkDescription() + " : " +
                ((TaskResult) itObj).getComment(), 3);
            // tmpRes is a set of ARCResources
            ARCResource res = null;
            for(Iterator ita=tmpRes.iterator(); ita.hasNext();){
              res = (ARCResource) ita.next();
              Debug.debug("resource: "+res, 3);
              clusterOK = false;
              for(int cl=0; cl<clusters.length; ++cl){
                if(res.getClusterName().equalsIgnoreCase(clusters[cl])){
                  clusterOK = true;
                  break;
                }
              }
              if(clusterOK){
                resourcesSet.add(res);
                for(Iterator ite=res.getRuntimeenvironment().iterator(); ite.hasNext();){
                  Debug.debug("runtime environment: "+ite.next(), 3);
                }
              }
            }
          }
        }
      }
      resourcesArray = new ARCResource[resourcesSet.size()];
      int i = 0;
      for(Iterator it=resourcesSet.iterator(); it.hasNext(); ){
        resourcesArray[i] = (ARCResource) it.next();
        ++i;
      }
      statusBar.setLabel("Finding resources done");
    }
    catch(InterruptedException e){
      Debug.debug("User interrupt of resource checking!", 2);
    }
    long end = System.currentTimeMillis();
    if((end - start) < limit + offset){
      Debug.debug("WARNING: failed to stay within time limit of "+limit/1000+" seconds.", 1);
    }
    if(foundResources.size()==0){
      error = "WARNING: failed to find authorized queues.";
      statusBar.setLabel(error);
      Debug.debug("WARNING: failed to find authorized queues.", 1);
      logFile.addMessage(error + "\n\tDN\t: "+getUserInfo(csName));
    }
    return resourcesArray;
  }

  // Copy stdout+stderr to local files
  public boolean syncCurrentOutputs(JobInfo job){
        
    try{
      Debug.debug("Getting : " + job.getName() + ":" + job.getJobId(), 3);
      ARCGridFTPJob gridJob = getGridJob(job);
      String dirName = runDir(job);
      Debug.debug("Downloading stdout/err of: " + job.getName() + ":" + job.getJobId()+
          " to " + dirName, 3);
      gridJob.getOutputFile("stdout", dirName);
      gridJob.getOutputFile("stderr", dirName);
      LocalStaticShellMgr.moveFile((new File(dirName, "stdout")).getAbsolutePath(),
          job.getStdOut());
      LocalStaticShellMgr.moveFile((new File(dirName, "stderr")).getAbsolutePath(),
          job.getStdErr());
    }
    catch(Exception ae){
      error = "Exception during get stdout of " + job.getName() + ":" + job.getJobId() + ":\n" +
      "\tException\t: " + ae.getMessage();
      //logFile.addMessage(error, ae);
      //ae.printStackTrace();
      return false;
    }
    return true;
  }

  public String getUserInfo(String csName){
    String user = null;
    try{
      Debug.debug("getting credential", 3);
      GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
      GlobusCredential globusCred = null;
      if(credential instanceof GlobusGSSCredentialImpl){
        globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
      }
      Debug.debug("getting identity", 3);
      user = globusCred.getIdentity();
      /* remove leading whitespace */
      user = user.replaceAll("^\\s+", "");
      /* remove trailing whitespace */
      user = user.replaceAll("\\s+$", "");      
    }
    catch(Exception ioe){
      error = "Exception during getUserInfo\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
    if(user==null && defaultUser!=null){
      Debug.debug("Job user null, getting from config file", 3);
      user = defaultUser;
    }
    return user;
  }

  public String[] getScripts(JobInfo job){
    String scriptName = runDir(job) + File.separator + job.getName() + ".job";
    String xrslName = runDir(job) + File.separator + job.getName() + ".xrsl";
    return new String [] {xrslName, scriptName};
  }

  public boolean postProcess(JobInfo job){
    Debug.debug("PostProcessing for job " + job.getName(), 2);
    if(copyToFinalDest(job)){
      try{
        // Delete the local run directory
        String runDir = runDir(job);
        LocalStaticShellMgr.deleteDir(new File(runDir));
      }
      catch(Exception e){
        error = e.getMessage();
        return false;
      }
      return true;
    }
    else{
      return false;
    }
  }

  public boolean preProcess(JobInfo job){
    final String stdoutFile = runDir(job) + File.separator + job.getName() + ".stdout";
    final String stderrFile = runDir(job) + File.separator + job.getName() + ".stderr";
    job.setOutputs(stdoutFile, stderrFile);
    // input files are already there
    return true;
  }
  
  /**
   * Moves job.StdOut and job.StdErr to final destination specified in the DB. <p>
   * job.StdOut and job.StdErr are then set to these final values. <p>
   * @return <code>true</code> if the move went ok, <code>false</code> otherwise.
   * (from AtCom1)
   */
  private boolean copyToFinalDest(JobInfo job){
    // Will only run if there is a shell available for the computing system
    // in question - and if the destination is accessible from this shell.
    // For grids, stdout and stderr should be taken care of by the xrsl or jdsl
    // (*ScriptGenerator)
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    
    GSIFTPFileTransfer gridftpFileSystem = GridPilot.getClassMgr().getGSIFTPFileTransfer();

    /**
     * move temp StdOut -> finalStdOut
     */
    if(finalStdOut!=null && finalStdOut.trim().length()!=0){
      try{
        syncCurrentOutputs(job);
        if(!LocalStaticShellMgr.existsFile(job.getStdOut())){
          logFile.addMessage("Post processing : File " + job.getStdOut() + " doesn't exist");
          return false;
        }
      }
      catch(Throwable e){
        error = "ERROR checking for stdout: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error, e);
        //throw e;
      }
      Debug.debug("Post processing : Renaming " + job.getStdOut() + " in " + finalStdOut, 2);
      // TODO: use TransferControl
      // if(!shell.moveFile(job.getStdOut(), finalStdOut)){
      try{
        gridftpFileSystem.putFile(new File(job.getStdOut()),
            new GlobusURL(finalStdOut),
            GridPilot.getClassMgr().getStatusBar(),
            null);
      }
      catch(Throwable e){
        error = "ERROR copying stdout: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error, e);
        //throw e;
      }
      job.setStdOut(finalStdOut);
    }

    /**
     * move temp StdErr -> finalStdErr
     */

    if(finalStdErr!=null && finalStdErr.trim().length()!=0){
      try{
        if(!LocalStaticShellMgr.existsFile(job.getStdErr())){
          logFile.addMessage("Post processing : File " + job.getStdErr() + " doesn't exist");
          return false;
        }
      }
      catch(Throwable e){
        error = "ERROR checking for stderr: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error, e);
        //throw e;
      }
      Debug.debug("Post processing : Renaming " + job.getStdErr() + " in " + finalStdErr,2);
      //shell.moveFile(job.getStdOut(), finalStdOutName);
      try{
        gridftpFileSystem.putFile(new File(job.getStdErr()),
            new GlobusURL(finalStdErr),
            GridPilot.getClassMgr().getStatusBar(),
            null);
      }
      catch(Throwable e){
        error = "ERROR copying stderr: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error, e);
        //throw e;
      }
      job.setStdErr(finalStdErr);
    }
    return true;
  }
  
  /** 
   * Extracts the ng status status of the job and updates job status with job.setJobStatus().
   * Returns false if the status has changed, true otherwise.
   */
  private static boolean extractStatus(JobInfo job, String line){

    // host
    if(job.getHost()==null){
      //String host = getValueOf("Cluster", line);
      String host = getValueOf("Execution nodes", line);
      Debug.debug("Job Destination : " + host, 2);
      if(host!=null){
        job.setHost(host);
      }
    }

    // status
    String status = getValueOf("Status", line);
    Debug.debug("Got Status: "+status, 2);
    if(status==null){
      job.setJobStatus(NGComputingSystem.NG_STATUS_ERROR);
      Debug.debug(
          "Status not found for job " + job.getName() +" : \n" + line, 2);
      return true;
    }
    else{
      //if(status.equals(NGComputingSystem.NG_STATUS_FINISHED)){
      if(status.startsWith(NGComputingSystem.NG_STATUS_FINISHED)){
        int errorBegin =line.indexOf("Error:");
        if(errorBegin != -1){
          GridPilot.getClassMgr().getLogFile().addMessage("Error at end of job " +
              job.getName() + " :\n" +
              line.substring(errorBegin, line.indexOf("\n", errorBegin)));
          job.setJobStatus(NGComputingSystem.NG_STATUS_FAILURE);
          return true;
        }
      }
      if(job.getJobStatus()!=null && job.getJobStatus().equals(status)){
        return false;
      }
      else{
        job.setJobStatus(status);
        return true;
      }
    }
  }

  private static String getValueOf(String attribute, String out){
    Debug.debug("getValueOf : " + attribute + "\n" + out, 1);

    int index = out.indexOf(attribute);
    if(index==-1){
      return null;
    }
    StringTokenizer st = new StringTokenizer(out.substring(index+attribute.length()+1,
        out.length()));

    String res = st.nextToken();
    if(res.endsWith(":")){
      res += " "+st.nextToken();
    }
    return res;
  }
  
  public String getError(String csName){
    return error;
  }

}