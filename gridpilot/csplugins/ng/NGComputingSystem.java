package gridpilot.csplugins.ng;

import java.io.*;
import java.net.MalformedURLException;
import java.security.GeneralSecurityException;
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

import javax.swing.JOptionPane;

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

import gridfactory.common.ConfigFile;
import gridfactory.common.ConfirmBox;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.Shell;
import gridfactory.common.StatusBar;
import gridfactory.common.jobrun.VirtualMachine;

import gridpilot.MyComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.MyJobInfo;
import gridpilot.GridPilot;
import gridpilot.MyLogFile;
import gridpilot.MyTransferControl;
import gridpilot.MyUtil;

/**
 * Main class for the NorduGrid plugin. <br>
 * <p><a href="NGComputingSystem.java.html">see sources</a>
 */

public class NGComputingSystem implements MyComputingSystem{

  public static final String NG_STATUS_ACCEPTED =  "ACCEPTED";
  public static final String NG_STATUS_PREPARING = "PREPARING" ;
  public static final String NG_STATUS_FINISHING = "FINISHING" ;
  public static final String NG_STATUS_FINISHED = "FINISHED" ;
  public static final String NG_STATUS_DELETED = "DELETED" ;
  public static final String NG_STATUS_CANCELLING = "CANCELLING";
  public static final String NG_STATUS_SUBMITTING = "SUBMITTING";
  public static final String NG_STATUS_INLRMSQ = "INLRMS: Q";
  public static final String NG_STATUS_INLRMSQ1 = "INLRMS:Q";
  public static final String NG_STATUS_INLRMSR = "INLRMS: R";
  public static final String NG_STATUS_INLRMSR1 = "INLRMS:R";
  public static final String NG_STATUS_INLRMSE = "INLRMS: E";
  public static final String NG_STATUS_INLRMSE1 = "INLRMS:E";

  public static final String NG_STATUS_FAILURE = "FAILURE";
  public static final String NG_STATUS_FAILED = "FAILED";
  public static final String NG_STATUS_ERROR = "ERROR";

  public static int SUBMIT_RESOURCES_REFRESH_INTERVAL = 5;

  private NGSubmission ngSubmission;
  private Boolean gridProxyInitialized = Boolean.FALSE;
  private String workingDir;
  private String unparsedWorkingDir;
  private ARCDiscovery arcDiscovery;
  private String defaultUser;
  private String error = "";
  private boolean useInfoSystem = false;
  private String [] clusters;
  private String [] giises;
  private ARCResource [] resources;
  private String [] runtimeDBs = null;
  private HashSet finalRuntimes = null;
  private int submissionNumber = 1;
  private MyTransferControl transferControl;
  
  private static String csName;
  private static MyLogFile logFile;
  private static boolean CONFIRM_RUN_DIR_CREATION = false;
  
  // At least for now, we only have Linux resources on NorduGrid
  public static final String OS = "Linux";
  
  public NGComputingSystem(String _csName){
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    try{
      GridPilot.getClassMgr().getSSL().activateProxySSL();
    }
    catch(Exception e){
      e.printStackTrace();
      logFile.addMessage("WARNING: could not initialize GSI security.", e);
    }
    transferControl = GridPilot.getClassMgr().getTransferControl();
    csName = _csName;
    unparsedWorkingDir= configFile.getValue(csName, "working directory");
    if(unparsedWorkingDir==null || unparsedWorkingDir.equals("")){
      unparsedWorkingDir = "~";
    }
    // unqualified names
    else if(!unparsedWorkingDir.toLowerCase().matches("\\w:.*") &&
        !unparsedWorkingDir.startsWith("/") && !unparsedWorkingDir.startsWith("~")){
      unparsedWorkingDir = "~"+"/"+unparsedWorkingDir;
    }
    workingDir = unparsedWorkingDir;
    workingDir = MyUtil.clearTildeLocally(MyUtil.clearFile(workingDir));
    if(workingDir.endsWith("/") || workingDir.endsWith("\\")){
      workingDir = workingDir.substring(0, workingDir.length()-1);
    }
    Debug.debug("Working dir: "+workingDir, 2);

    logFile = GridPilot.getClassMgr().getLogFile();  
    
    defaultUser = configFile.getValue(GridPilot.topConfigSection, "Default user");
    String useInfoSys = configFile.getValue(csName, "Use information system");
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
      Debug.debug("Found "+resources.length+" authorized ARC resources.", 1);
      for(int i=0; i<resources.length; ++i){
        Debug.debug("--> "+resources[i].getClusterName()+"--> "+resources[i].getQueueName(), 1);
      }
      if(resources.length==0){
        MyUtil.showMessage("No authorization", "WARNING: You are not not authorized to run jobs on any defined NorduGrid/ARC clusters.");
      }
      ngSubmission = new NGSubmission(csName, resources);
    }
    else{
      ngSubmission = new NGSubmission(csName, clusters);
    }
    
    Debug.debug("Clusters: "+MyUtil.arrayToString(clusters), 2);
    
    try{
      runtimeDBs = GridPilot.getClassMgr().getConfigFile().getValues(
          csName, "runtime databases");
    }
    catch(Exception e){
      Debug.debug("ERROR getting runtime database: "+e.getMessage(), 1);
    }
  }
  
  /**
   * The runtime environments are simply found from the
   * information system.
   */
  public void setupRuntimeEnvironments(String csName){
    if(runtimeDBs==null || runtimeDBs.length==0){
      return;
    }    
    Set runtimes = null;
    DBPluginMgr dbPluginMgr = null;
    for(int ii=0; ii<runtimeDBs.length; ++ii){
      try{
        dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(
            runtimeDBs[ii]);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not load runtime DB "+runtimeDBs[ii], 1);
        continue;
      }
      finalRuntimes = new HashSet();
      // At least for now, we only have Linux resources on NorduGrid
      finalRuntimes.add(OS);
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
            if(dbPluginMgr.getRuntimeEnvironmentIDs(name, csName)==null){
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
  }

  /*
   * Local directory to keep xrsl, shell script and temporary copies of stdin/stdout
   */
  protected String runDir(JobInfo job){
    return workingDir+"/"+job.getName();
  }

  public boolean submit(JobInfo job) {
    if(submissionNumber==SUBMIT_RESOURCES_REFRESH_INTERVAL){
      GridPilot.getClassMgr().getLogFile().addInfo("Refreshing computing resources...");
      refreshResources();
      submissionNumber = 1;
    }
    Debug.debug("submitting..."+gridProxyInitialized, 3);
    String scriptName = runDir(job) + File.separator + job.getName() + ".job";
    String xrslName = runDir(job) + File.separator + job.getName() + ".xrsl";
    try{
      boolean ret = ngSubmission.submit(submissionNumber, job, scriptName, xrslName);
      ++submissionNumber;
      return ret;
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
      updateStatus((MyJobInfo) jobs.get(i));
    }
  }

  private void updateStatus(MyJobInfo job){
    
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
      if(job.getCSStatus()==null){
        Debug.debug("No status found for job "+job.getName(), 2);
        job.setStatusError();
      }
      else if(job.getCSStatus().equals(NG_STATUS_FINISHED)){
        try{
          // Only sync if CE has copied stdout/stderr to final destination.
          // Otherwise, getOutput will get them (and syncCurrentOutputs will fail
          // because it will try to get them from final destination).
          DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
          String stdoutDest = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
          String stderrDest = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());
          if(!stdoutDest.startsWith("file:") ||
              stderrDest!=null && !stderrDest.equals("") && !stderrDest.startsWith("file:")){
            syncCurrentOutputs(job);
          }
          if(getOutput(job)){
            job.setStatusDone();
          }
          else{
            job.setStatusError();
          }
        }
        catch(Exception e){
          job.setStatusError();
        }
      }
      else if(job.getCSStatus().equals(NG_STATUS_FAILURE) ||
          job.getCSStatus().equals(NG_STATUS_FAILED)){
        //getOutput(job);
        job.setStatusFailed();
      }
      else if(job.getCSStatus().equals(NG_STATUS_ERROR)){
        // try to clean up, just in case...
        //getOutput(job);
        job.setStatusError();
      }
      else if(job.getCSStatus().equals(NG_STATUS_DELETED)){
        job.setStatusError();
      }
      else if(job.getCSStatus().equals(NG_STATUS_INLRMSR) ||
          job.getCSStatus().equals(NG_STATUS_INLRMSR1)){
        job.setStatusRunning();
      }
      else{
        Debug.debug("WARNING: unknown status: "+job.getCSStatus(), 1);
        job.setStatusReady();
      }
    }
  }
  
  private ARCGridFTPJob getGridJob(MyJobInfo job) throws ARCGridFTPJobException, IOException, GeneralSecurityException{
    
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
    GSSCredential credential = GridPilot.getClassMgr().getSSL().getGridCredential();
    GlobusCredential globusCred = null;
    if(credential instanceof GlobusGSSCredentialImpl){
      globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
    }
    gridJob.addProxy(globusCred);
    gridJob.connect();
    return gridJob;
  }
  
  public boolean killJobs(Vector jobsToKill){
    MyJobInfo job = null;
    Vector errors = new Vector();
    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      try{
        job = (MyJobInfo) en.nextElement();
        Debug.debug("Cleaning : " + job.getName() + ":" + job.getJobId(), 3);
        ARCGridFTPJob gridJob = getGridJob(job);
        gridJob.cancel();
        //gridJob.clean();
      }
      catch(Exception ae){
        errors.add(ae.getMessage());
        logFile.addMessage("Exception during killing of " + job.getName() + ":" + job.getJobId() + ":\n" +
                           "\tException\t: " + ae.getMessage(), ae);
        ae.printStackTrace();
      }
    }    
    if(errors.size()!=0){
      error = MyUtil.arrayToString(errors.toArray());
      return false;
    }
    else{
      return true;
    }
  }

  public boolean cleanup(JobInfo job){
    
    boolean ret = true;
    
    // Clean job off grid. - just in case...
    try{
      ARCGridFTPJob gridJob = getGridJob((MyJobInfo) job);
      gridJob.cancel();
    }
    catch(Exception e){
      Debug.debug("Could not cancel job. Probably finished. "+
          job.getName()+". "+e.getMessage(), 3);
      //e.printStackTrace();
    }
    try{
      ARCGridFTPJob gridJob = getGridJob((MyJobInfo) job);
      gridJob.clean();
    }
    catch(Exception e){
      Debug.debug("Could not clean job. Probably already deleted. "+
          job.getName()+". "+e.getMessage(), 3);
      e.printStackTrace();
    }
    
    // Delete files that may have been copied to storage elements
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getIdentifier());
    try{
      for(int i=0; i<outputFileNames.length; ++i){
        outputFileNames[i] = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFileNames[i]);
      }
      transferControl.deleteFiles(outputFileNames);
    }
    catch(Exception e){
      error = "WARNING: could not delete output file. "+e.getMessage();
      Debug.debug(error, 3);
    }
    // Delete stdout/stderr that may have been copied to final destination
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      try{
        transferControl.deleteFiles(new String [] {finalStdOut});
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
        transferControl.deleteFiles(new String [] {finalStdErr});
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
      Debug.debug("Deleting run directory "+runDir, 2);
      LocalStaticShell.deleteDir(runDir);
    }
    catch(Exception e){
      error = "WARNING: could not delete "+runDir+". "+e.getMessage();
      Debug.debug(error, 2);
      ret = false;
    }
    
    return ret;

  }

  public void exit(){
  }
  
  public void cleanupRuntimeEnvironments(String csName){
    String runtimeName = null;
    String initText = null;
    String[] ids = null;
    boolean ok = true;
    for(int ii=0; ii<runtimeDBs.length; ++ii){
      try{
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(
            runtimeDBs[ii]);
        for(Iterator it=finalRuntimes.iterator(); it.hasNext();){
          ok = true;
          runtimeName = (String )it.next();
          // Don't delete records with a non-empty initText.
          // These can only have been created by hand.
          initText = dbPluginMgr.getRuntimeInitText(runtimeName, csName);
          if(initText!=null && !initText.equals("")){
            continue;
          }
          ids = dbPluginMgr.getRuntimeEnvironmentIDs(runtimeName, csName);
          if(ids!=null){
            for(int i=0; i<ids.length; ++i){
              ok = ok && dbPluginMgr.deleteRuntimeEnvironment(ids[i]);
            }
          }
          else{
            ok = false;
          }
          if(!ok){
            Debug.debug("WARNING: could not delete runtime environment(s) " +
                runtimeName+" from database "+dbPluginMgr.getDBName(), 1);
          }
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
  }
  
  private boolean getOutput(MyJobInfo job){
    
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
      LocalStaticShell.moveFile(dirName, dirName+"."+dateString);
    }
    catch(Exception ioe){
      error = "Exception during job " + job.getName() + " get output :\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
      return false;
    }
    
    // Get the outputs
    try{
      Debug.debug("Getting : " + job.getName() + " : " + job.getJobId() +
          " -> "+dirName, 3);
      ARCGridFTPJob gridJob = getGridJob(job);
      gridJob.get(runDir(job)/*dirName*/);
     }
     catch(Exception ae){
       error = "Exception during get outputs of " + job.getName() + " : " + job.getJobId() + ":\n" +
       "\tException\t: " + ae.getMessage();
       logFile.addMessage(error, ae);
       ae.printStackTrace();
     }
    
    // Rename stdout and stderr to the name specified in the job description,
    // and move them one level up
    if(job.getOutTmp()!=null && !job.getOutTmp().equals("")){      
      try{
        LocalStaticShell.copyFile(dirName+File.separator+"stdout", job.getOutTmp());
      } 
      catch(Exception ioe){
        error = "Exception during job " + job.getName() + " getFullStatus :\n" +
        "\tException\t: " + ioe.getMessage();
        logFile.addMessage(error, ioe);
        return false;
      }
    }
    if(job.getErrTmp()!=null && !job.getErrTmp().equals("")){
      try{
        LocalStaticShell.copyFile(dirName+File.separator+"stderr", job.getErrTmp());
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
    String node = "";
    
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
        gridJob = getGridJob((MyJobInfo) job);

      }
      catch(Exception ioe){
        error = "Exception during job " + job.getName() + " getFullStatus :\n" +
        "\tException\t: " + ioe.getMessage();
        logFile.addMessage(error, ioe);
        return ioe.getMessage();
      }
      
      try{
        //status = gridJob.state();
        status = gridJob.getOutputFile("log/status").trim();
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

      try{
        String diag = gridJob.getOutputFile("log/diag");
        InputStream is = new ByteArrayInputStream(diag.getBytes());
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while((line = in.readLine())!=null){
          if(line.startsWith("nodename=")){
            node = line.replaceFirst("nodename=(\\S+)", "$1");
          }
        }
        in.close();        
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
    if(node!=null && !node.equals("")){
      result += "Node: "+node+"\n";
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

  public String [] getCurrentOutput(JobInfo job) throws IOException{
    
    String stdOutFile = job.getOutTmp();
    String stdErrFile = job.getErrTmp();
    
    boolean resyncFirst = true;

    if(resyncFirst){

      // move existing files out of the way.
      // - do it only if job.getOutTmp is not the final destination, that is,
      // if syncCurrentOutputs will not get
      // stdout/stderr from the final destination
      boolean isValidated = false;
      String dirName = runDir(job);
      if(!LocalStaticShell.existsFile(dirName) || job.getDBStatus()==DBPluginMgr.VALIDATED){
        isValidated = true;
      }
      if(!isValidated && stdOutFile!=null && !stdOutFile.equals("") &&
          LocalStaticShell.existsFile(stdOutFile)){
        LocalStaticShell.moveFile(stdOutFile, stdOutFile+".bk");
      }
      if(!isValidated && stdErrFile!=null && !stdErrFile.equals("") &&
          LocalStaticShell.existsFile(stdErrFile)){
        LocalStaticShell.moveFile(stdErrFile, stdErrFile+".bk");
      }
      
      // if retrieval of files fails, move old files back in place
      if(!syncCurrentOutputs((MyJobInfo) job)){
        if(!isValidated){
          try{
            if(LocalStaticShell.existsFile(stdOutFile+".bk")){
              LocalStaticShell.deleteFile(stdOutFile);
              LocalStaticShell.moveFile(stdOutFile+".bk", stdOutFile);
            }
          }
          catch(Exception e){
            e.printStackTrace();
          }
          try{
            if(LocalStaticShell.existsFile(stdErrFile+".bk")){
              LocalStaticShell.deleteFile(stdErrFile);
              LocalStaticShell.moveFile(stdErrFile+".bk", stdErrFile);
            }
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
      // delete backup files
      else{
        Debug.debug("WARNING: could not update stdout/stderr for job "+
            job.getName(), 2);
        if(!isValidated){
          try{
            LocalStaticShell.deleteFile(stdOutFile+".bk");
            LocalStaticShell.deleteFile(stdErrFile+".bk");
          }
          catch(Exception e){
            e.printStackTrace();
          }    
        }
      }
    }
        
    String [] res = new String[2];

    if(stdOutFile!=null && !stdOutFile.equals("")){
      // read stdout
      try{
        res[0] = LocalStaticShell.readFile(stdOutFile);
       }
       catch(IOException ae){
         error = "Exception during getCurrentOutputs (stdout) for " + job.getName() + " --> " + job.getJobId() + "\n" +
         "\nException: " + ae.getMessage();
         res[0] = "*** Could not read stdout ***\n Probably the job has not started yet, " +
                "did never start or got deleted.";
         GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar.setLabel("ERROR: "+ae.getMessage());
         logFile.addMessage(error, ae);
         //throw ae;
       }
    }
    if(stdErrFile!=null && !stdErrFile.equals("")){
      // read stderr
      try{
        res[1] = LocalStaticShell.readFile(stdErrFile);
       }
       catch(Exception ae){
         error = "Exception during getCurrentOutputs (stderr) for " + job.getName() + ":" + job.getJobId() + ":\n" +
         "\nException: " + ae.getMessage();
         //logFile.addMessage(error, ae);
         //ae.printStackTrace();
         res[1] = "*** Could not read stderr ***\n Probably the job has not started yet, " +
         "did never start or got deleted.";
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
        " The following jobs found: "+MyUtil.arrayToString(allJobIds.toArray()), 1);
    throw new ARCGridFTPJobException("No job found matching "+job.getJobId());
  }
  
  public ARCJob [] findCurrentJobsFromIS(){
    StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;

    long start = System.currentTimeMillis();
    long limit = 30000;//10000;
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
    if((end - start) > limit + offset){
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
    long start = System.currentTimeMillis();
    long limit = 30000;//10000;
    long offset = 2000; // some +- coefficient
    Collection foundResources = null;
    ARCResource [] resourcesArray = null;
    try{
      logFile.addInfo("Finding resources, please wait...");
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
      logFile.addInfo("Finding resources done");
    }
    catch(InterruptedException e){
      Debug.debug("User interrupt of resource checking!", 2);
    }
    long end = System.currentTimeMillis();
    if((end - start)<limit + offset){
      Debug.debug("WARNING: failed to stay within time limit of "+limit/1000+" seconds.", 1);
    }
    if(foundResources.size()==0){
      error = "WARNING: failed to find authorized queues.";
      Debug.debug("WARNING: failed to find authorized queues.", 1);
      logFile.addMessage(error + "\n\tDN\t: "+getUserInfo(csName));
    }
    return resourcesArray;
  }

  /**
   * Checks if runDir(job) exists. If not, attempts to create it. 
   * Returns true if the directory didn't exist and has been successfully
   * created.
   */
  private boolean createMissingWorkingDir(MyJobInfo job){
    // First check if working directory is there. If not, we may be
    // checking from another machine than the one we submitted from.
    // We just create it...
    boolean getFromfinalDest = false;
    try{
      String dirName = runDir(job);
      if(!LocalStaticShell.existsFile(dirName)){
        int choice = -1;
        if(CONFIRM_RUN_DIR_CREATION){
          choice = (new ConfirmBox(JOptionPane.getRootFrame())).getConfirm(
              "Confirm create directory",
              "The working directory, "+dirName+",  of this job was not found. \n" +
              "The job was probably submitted from another machine or has already been validated. \n" +
              "Click OK to create the directory " +
              "(stdout/stder will be synchronized, scripts will not).", new Object[] {"OK",  "Skip"});
        }
        else{
          choice = 0;
        }
        if(choice==0){
          LocalStaticShell.mkdirs(dirName);
          final String stdoutFile = unparsedWorkingDir+"/"+job.getName() + "/" + job.getName() + ".stdout";
          final String stderrFile = unparsedWorkingDir+"/"+job.getName() + "/" + job.getName() + ".stderr";
          job.setOutputs(stdoutFile, stderrFile);
          DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
          if(!dbPluginMgr.updateJobDefinition(
              job.getIdentifier(),
              new String []{job.getUserInfo(), job.getJobId(), job.getName(),
              job.getOutTmp(), job.getErrTmp()})){
            logFile.addMessage("DB update(" + job.getIdentifier() + ", " +
                           job.getJobId() + ", " + job.getName() + ", " +
                           job.getOutTmp() + ", " + job.getErrTmp() +
                           ") failed", job);    
          }
          getFromfinalDest = true;
        }
        else{
          logFile.addMessage("WARNING: Directory "+dirName+" does not exist. Cannot proceed.");
          getFromfinalDest = false;
        }
      }
    }
    catch(Exception ae){
      error = "Exception during get stdout of " + job.getName() + ":" + job.getJobId() + ":\n" +
      "\tException\t: " + ae.getMessage();
      getFromfinalDest = false;
    }
    return getFromfinalDest;
  }
  
  // Copy stdout+stderr to local files
  private boolean syncCurrentOutputs(MyJobInfo job){
    try{
      Debug.debug("Syncing " + job.getName() + " --> " + job.getJobId(), 3);
      ARCGridFTPJob gridJob = getGridJob(job);
      
      String dirName = runDir(job);

      DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
      String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
      String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());

      boolean getFromfinalDest = createMissingWorkingDir(job);
      if(!(getFromfinalDest || job.getCSStatus().equals(NG_STATUS_FINISHED) ||
          job.getDBStatus()==DBPluginMgr.UNDECIDED)){
        Debug.debug("Downloading stdout/err of running job: " + job.getName() + " : " + job.getJobId() +
            " : " + job.getCSStatus()+" to " + dirName, 3);
        try{
          gridJob.getOutputFile("stdout", dirName);
          LocalStaticShell.moveFile((new File(dirName, "stdout")).getAbsolutePath(),
              job.getOutTmp());
          gridJob.getOutputFile("stderr", dirName);
          LocalStaticShell.moveFile((new File(dirName, "stderr")).getAbsolutePath(),
              job.getErrTmp());
        }
        catch(Exception e){
          // if this fails, give it a try to get from final destination;
          // it could be that the job is in NG_STATUS_FINISHED on the CE,
          // but GridPilot does not know, because the job has not been
          // refreshed yet
          getFromfinalDest = true;
          e.printStackTrace();
        }
      }
      
      if(getFromfinalDest || job.getCSStatus().equals(NG_STATUS_FINISHED) ||
          job.getDBStatus()==DBPluginMgr.UNDECIDED){
        //if(getFromfinalDest || !finalStdOut.startsWith("file:")){
          Debug.debug("Downloading stdout of: " + job.getName() + ":" + job.getJobId()+
              " from final destination "+finalStdOut+" to " +
              MyUtil.clearTildeLocally(MyUtil.clearFile(job.getOutTmp())), 3);
          transferControl.download(finalStdOut, new File(MyUtil.clearTildeLocally(MyUtil.clearFile(job.getOutTmp()))));
        //}
        //if(getFromfinalDest || !finalStdErr.startsWith("file:")){
          Debug.debug("Downloading stderr of: " + job.getName() + ":" + job.getJobId()+
              " from final destination "+finalStdErr+" to " +
              MyUtil.clearTildeLocally(MyUtil.clearFile(job.getErrTmp())), 3);
          transferControl.download(finalStdErr, new File(MyUtil.clearTildeLocally(MyUtil.clearFile(job.getErrTmp()))));
        //}
      }
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
      user = GridPilot.getClassMgr().getSSL().getGridSubject();
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
    if(copyToFinalDest((MyJobInfo) job)){
      try{
        // Delete the local run directory
        String runDir = runDir(job);
        LocalStaticShell.deleteDir(runDir);
      }
      catch(Exception e){
        error = e.getMessage();
        return false;
      }
      // Clean the job off the grid
      try{
        ARCGridFTPJob gridJob = getGridJob((MyJobInfo) job);
        gridJob.clean();
      }
      catch(Exception e){
        Debug.debug("Could not clean job. Probably already deleted. "+
            job.getName()+". "+e.getMessage(), 3);
        e.printStackTrace();
        //return false;
      }
      return true;
    }
    else{
      return false;
    }
  }

  public boolean preProcess(JobInfo job){
    //final String stdoutFile = runDir(job) + File.separator + job.getName() + ".stdout";
    //final String stderrFile = runDir(job) + File.separator + job.getName() + ".stderr";
    // preserve ~ in tmp stdout/stderr, so checking from another machine might work
    final String stdoutFile = unparsedWorkingDir+"/"+job.getName() + "/" + job.getName() + ".stdout";
    final String stderrFile = unparsedWorkingDir+"/"+job.getName() + "/" + job.getName() + ".stderr";
    ((MyJobInfo) job).setOutputs(stdoutFile, stderrFile);
    // input files are already there
    return true;
  }
  
  /**
   * Moves job.StdOut and job.StdErr to final destination specified in the DB. <p>
   * job.StdOut and job.StdErr are then set to these final values. <p>
   * @return <code>true</code> if the move went ok, <code>false</code> otherwise.
   * (from AtCom1)
   */
  private boolean copyToFinalDest(MyJobInfo job){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());
    boolean ok = true;
    
    /**
     * move downloaded output files to their final destinations -
     * Iff these destinations have the format file:...
     */
    String jobID = job.getJobId().substring(job.getJobId().lastIndexOf("/"));
    int lastSlash = job.getJobId().lastIndexOf("/");
    if(lastSlash>-1){
      jobID = job.getJobId().substring(lastSlash + 1);
    }
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getIdentifier());
    String localName = null;
    String remoteName = null;
    for(int i=0; i<outputFileNames.length; ++i){
      try{
        localName = dbPluginMgr.getJobDefOutLocalName(job.getIdentifier(), outputFileNames[i]);
        remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFileNames[i]);
        if(remoteName.startsWith("file:")){
          transferControl.upload(
              new File(runDir(job)+File.separator+jobID+File.separator+localName),
              remoteName);
        }
      }
      catch(Exception e){
        error = "ERROR copying file "+localName+" -> "+remoteName+
        " to final destination: "+e.getMessage();
        logFile.addMessage(error, e);
        ok = false;
      }
    }
    
    // Horrible clutch because Globus gass copy fails on empty files...
    boolean emptyFile = false;

    /**
     * move temp StdOut -> finalStdOut
     */
    if(finalStdOut!=null && finalStdOut.trim().length()!=0 &&
        !(finalStdOut.startsWith("gsiftp://") ||
            finalStdOut.startsWith("ftp://") ||
            finalStdOut.startsWith("rls://") ||
            finalStdOut.startsWith("se://") ||
            finalStdOut.startsWith("httpg://") ||
            finalStdOut.startsWith("https://"))){
      try{
        // this has already been done by doValidate --> getCurrentOutputs
        //syncCurrentOutputs(job);
        if(!LocalStaticShell.existsFile(job.getOutTmp())){
          logFile.addMessage("Post-processing : File " + job.getOutTmp() + " doesn't exist");
          ok = false;
        }
      }
      catch(Throwable e){
        error = "ERROR checking for stdout: "+e.getMessage();
        e.printStackTrace();
        Debug.debug(error, 2);
        logFile.addMessage(error, e);
        ok = false;
      }
      Debug.debug("Post-processing : Moving " + job.getOutTmp() + " -> " + finalStdOut, 2);
      try{
        File stdoutSourceFile = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(job.getOutTmp())));
        emptyFile = finalStdOut.startsWith("https") && stdoutSourceFile.length()==0;
        transferControl.upload(stdoutSourceFile, finalStdOut);
        job.setOutTmp(finalStdOut);
      }
      catch(Throwable e){
        error = "ERROR copying stdout: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error, e);
        ok = ok && emptyFile;
      }
    }

    /**
     * move temp StdErr -> finalStdErr
     */

    if(finalStdErr!=null && finalStdErr.trim().length()!=0 &&
        !(finalStdErr.startsWith("gsiftp://") ||
            finalStdErr.startsWith("ftp://") ||
            finalStdErr.startsWith("rls://") ||
            finalStdErr.startsWith("se://") ||
            finalStdErr.startsWith("httpg://") ||
            finalStdOut.startsWith("https://"))){
      try{
        if(!LocalStaticShell.existsFile(job.getErrTmp())){
          logFile.addMessage("Post-processing : File " + job.getErrTmp() + " doesn't exist");
          return false;
        }
      }
      catch(Throwable e){
        error = "ERROR checking for stderr: "+e.getMessage();
        e.printStackTrace();
        Debug.debug(error, 2);
        logFile.addMessage(error, e);
        ok = false;
      }
      Debug.debug("Post processing : Moving " + job.getErrTmp() + " -> " + finalStdErr,2);
      try{
        File stderrSourceFile = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(job.getErrTmp())));
        emptyFile = finalStdOut.startsWith("https") && stderrSourceFile.length()==0;
        transferControl.upload(stderrSourceFile, finalStdErr);
        job.setErrTmp(finalStdErr);
      }
      catch(Throwable e){
        error = "ERROR copying stderr: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error, e);
        ok = ok && emptyFile;
      }
    }
    return ok;
  }
  
  /** 
   * Extracts the ng status status of the job and updates job status with job.setJobStatus().
   * Returns false if the status has changed, true otherwise.
   */
  private static boolean extractStatus(MyJobInfo job, String line){

    // host
    if(job.getHost()==null){
      //String host = getValueOf("Cluster", line);
      String host = getValueOf("Node", line);
      Debug.debug("Job Destination : " + host, 2);
      if(host!=null){
        job.setHost(host);
      }
    }

    // status
    String status = getValueOf("Status", line);
    Debug.debug("Got Status: "+status, 2);
    if(status==null){
      job.setCSStatus(NG_STATUS_ERROR);
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
          job.setCSStatus(NG_STATUS_FAILURE);
          return true;
        }
      }
      if(job.getCSStatus()!=null && job.getCSStatus().equals(status)){
        return false;
      }
      else{
        job.setCSStatus(status);
        return true;
      }
    }
  }
  
  public void refreshResources(){
    resources = findAuthorizedResourcesFromIS();
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
  
  public String getError(){
    return error;
  }
  
  public Shell getShell(JobInfo job){
    return null;
  }

  public long getRunningTime(JobInfo arg0) {
    return -1;
  }

  public VirtualMachine getVM(JobInfo arg0) {
    return null;
  }

  public boolean pauseJobs(Vector<JobInfo> arg0) {
    return false;
  }

  public boolean resumeJobs(Vector<JobInfo> arg0) {
    return false;
  }

}