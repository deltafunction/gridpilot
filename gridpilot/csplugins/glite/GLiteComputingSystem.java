package gridpilot.csplugins.glite;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JOptionPane;

import gridpilot.ComputingSystem;
import gridpilot.ConfigFile;
import gridpilot.ConfirmBox;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LocalStaticShellMgr;
import gridpilot.LogFile;
import gridpilot.GridPilot;
import gridpilot.TransferControl;
import gridpilot.Util;
import gridpilot.csplugins.ng.NGComputingSystem;

import org.glite.wms.wmproxy.JobIdStructType;
import org.glite.wms.wmproxy.WMProxyAPI;
import org.glite.wmsui.apij.*;
import org.globus.gsi.GlobusCredentialException;
import org.globus.mds.MDS;
import org.globus.mds.MDSException;
import org.globus.mds.MDSResult;
import org.safehaus.uuid.UUIDGenerator;

/**
 * Main class for the LSF plugin.
 */

public class GLiteComputingSystem implements ComputingSystem{

  private String csName = null;
  private LogFile logFile = null;
  private ConfigFile configFile = null;
  private String error = "";
  private String workingDir = null;
  private String unparsedWorkingDir = null;
  private String [] runtimeDBs = null;
  private HashSet finalRuntimes = null;
  private String wmUrl = null;
  private WMProxyAPI wmProxyAPI = null;
  private String bdiiHost = null;
  private MDS mds = null;
  private String [] rteClusters = null;
  private String [] rteVos = null;
  private String [] rteTags = null;
  private HashSet rteScriptMappings = null;
  private String defaultUser;
  
  private static boolean CONFIRM_RUN_DIR_CREATION = false;
  private static String BDII_PORT = "2170";
  private static String BDII_BASE_DN = "mds-vo-name=local,o=grid";

  public GLiteComputingSystem(String _csName){
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    defaultUser = configFile.getValue("GridPilot", "Default user");
    unparsedWorkingDir= configFile.getValue(csName, "Working directory");
    if(unparsedWorkingDir==null || unparsedWorkingDir.equals("")){
      unparsedWorkingDir = "~";
    }
    // unqualified names
    else if(!unparsedWorkingDir.toLowerCase().matches("\\w:.*") &&
        !unparsedWorkingDir.startsWith("/") && !unparsedWorkingDir.startsWith("~")){
      unparsedWorkingDir = "~"+"/"+unparsedWorkingDir;
    }
    workingDir = unparsedWorkingDir;
    workingDir = Util.clearTildeLocally(Util.clearFile(workingDir));
    if(workingDir.endsWith("/") || workingDir.endsWith("\\")){
      workingDir = workingDir.substring(0, workingDir.length()-1);
    }
    Debug.debug("Working dir: "+workingDir, 2);
    try{
      rteVos = GridPilot.getClassMgr().getConfigFile().getValues(
          csName, "runtime vos");
    }
    catch(Exception e){
      error = "WARNING: runtime vos for "+csName+" not defined. Showing all RTEs";
      logFile.addMessage(error, e);
    }
    try{
      rteTags = GridPilot.getClassMgr().getConfigFile().getValues(
          csName, "runtime tags");
    }
    catch(Exception e){
      error = "WARNING: runtime tags for "+csName+" not defined. Showing all RTEs";
      logFile.addMessage(error, e);
    }
    try{
      rteClusters = GridPilot.getClassMgr().getConfigFile().getValues(
          csName, "runtime clusters");
    }
    catch(Exception e){
      error = "WARNING: runtime clusters for "+csName+" not defined." +
      " Querying all clusters for RTEs. This may take a LONG time...";
      logFile.addMessage(error, e);
    }
    try{
      wmUrl = GridPilot.getClassMgr().getConfigFile().getValue(
          csName, "WMProxy URL");
      bdiiHost = GridPilot.getClassMgr().getConfigFile().getValue(
          csName, "BDII host");
      
      // setup proxy if not there
      try{
        GridPilot.getClassMgr().getGridCredential();
      }
      catch(Exception ee){
        ee.printStackTrace();
      }

      wmProxyAPI = new WMProxyAPI(wmUrl,
            Util.getProxyFile().getAbsolutePath(),
            GridPilot.getClassMgr().getCaCertsTmpDir());
      
      mds = new MDS(bdiiHost, BDII_PORT, BDII_BASE_DN);
      
      try{
        runtimeDBs = GridPilot.getClassMgr().getConfigFile().getValues(
            csName, "runtime databases");
      }
      catch(Exception e){
        Debug.debug("ERROR getting runtime database: "+e.getMessage(), 1);
        e.printStackTrace();
      }
      if(runtimeDBs!=null && runtimeDBs.length>0){
        setupRuntimeEnvironments(csName);
      }
      
    }
    catch(Exception e){
      Debug.debug("ERROR initializing "+csName+". "+e.getMessage(), 1);
      e.printStackTrace();
    }
  }

  /*
   * Local directory to keep xrsl, shell script and temporary copies of stdin/stdout
   */
  private String runDir(JobInfo job){
    return workingDir+"/"+job.getName();
  }

  /**
   * The runtime environments are simply found from the
   * information system.
   */
  public void setupRuntimeEnvironments(String csName){
    for(int i=0; i<runtimeDBs.length; ++i){
      setupRuntimeEnvironments(csName, runtimeDBs[i]);
    }
  }

    /**
   * The runtime environments are simply found from the
   * information system.
   */
  public void setupRuntimeEnvironments(String csName, String runtimeDB){
    finalRuntimes = new HashSet();
    HashSet runtimes = new HashSet();
    
    GridPilot.splashShow("Discovering gLite runtime environments...");

    try{
      mds.connect();
      Hashtable clusterTable =
        mds.search(BDII_BASE_DN, "(GlueSubClusterName=*)",
            new String [] {"GlueSubClusterName"}, MDS.SUBTREE_SCOPE);
      Enumeration en = clusterTable.elements();
      Enumeration enn = null;
      Hashtable rteTable = null;
      MDSResult hostRes = null;
      MDSResult rteRes = null;
      String host = null;
      String rte = null;
      Debug.debug("rteClusters: "+rteClusters, 2);
      while(en.hasMoreElements()){
        hostRes = (MDSResult) en.nextElement();
        host = hostRes.getFirstValue("GlueSubClusterName").toString();
        // If runtime hosts are defined, ignore non-mathing hosts
        if(rteClusters!=null && !Arrays.asList(rteClusters).contains(host)){
          continue;
        }
        Debug.debug("host -> "+host, 2);
        rteTable = mds.search(BDII_BASE_DN, "(GlueSubClusterName="+host+")",
            new String [] {"GlueHostApplicationSoftwareRunTimeEnvironment"},
            MDS.SUBTREE_SCOPE);
        enn = rteTable.elements();
        while(enn.hasMoreElements()){
          rteRes = (MDSResult) enn.nextElement();
          for(int i=0; i<rteRes.size("GlueHostApplicationSoftwareRunTimeEnvironment"); ++i){
            rte = (String) rteRes.getValueAt("GlueHostApplicationSoftwareRunTimeEnvironment", i);
            // Ignore RTEs that don't belong to one of the defined VOs
            if(rteVos!=null){
              for(int j=0; j<rteVos.length; ++j){
                Debug.debug("checking "+rte.toLowerCase()+" <-> "+"vo-"+rteVos[j].toLowerCase(), 3);
                if(rteVos[j]!=null &&
                    rte.toLowerCase().startsWith("vo-"+rteVos[j].toLowerCase())){
                  runtimes.add(rte);
                  continue;
                }
              }
            }
            else{
              runtimes.add(rte);
            }
            Debug.debug("RTE ---> "+rte, 2);
          }
        }
      }
      mds.disconnect();
    }
    catch(MDSException e){
      error = "WARNING: could not list runtime environments.";
      logFile.addMessage(error, e);
      e.printStackTrace();
    }
    
    if(runtimes!=null && runtimes.size()>0){
      String name = null;
      DBPluginMgr dbPluginMgr = null;      
      try{
        dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(runtimeDB);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not load runtime DB "+runtimeDB, 1);
        return;
      }
      String [] runtimeEnvironmentFields =
        dbPluginMgr.getFieldNames("runtimeEnvironment");
      String [] rtVals = new String [runtimeEnvironmentFields.length];
      for(Iterator it=runtimes.iterator(); it.hasNext();){
        name = null;
        try{
          name = it.next().toString();       
        }
        catch(Exception e){
          e.printStackTrace();
        }
        if(name!=null && name.length()>0){
          // Write the entry in the local DB
          for(int i=0; i<runtimeEnvironmentFields.length; ++i){
            if(runtimeEnvironmentFields[i].equalsIgnoreCase("name")){
              rtVals[i] = name;
            }
            else if(runtimeEnvironmentFields[i].equalsIgnoreCase("computingSystem")){
              rtVals[i] = csName;
            }
            else if(runtimeEnvironmentFields[i].equalsIgnoreCase("initLines")){             
              rtVals[i] = mapRteNameToScriptPaths(name);
            }
            else{
              rtVals[i] = "";
            }
          }
          try{
            if(dbPluginMgr.createRuntimeEnvironment(rtVals)){
              finalRuntimes.add(name);
            }
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    }
    else{
      Debug.debug("WARNING: no runtime environments found", 1);
    }
  }
  
  private String mapRteNameToScriptPaths(String name){
    if(rteScriptMappings==null){
      // Try to find (guess...) the paths to the setup scripts
      rteScriptMappings = new HashSet();
      String [] mappings = null;
      if(rteTags!=null){
        for(int i=0; i<rteVos.length; ++i){
          mappings = null;
          try{
            mappings = GridPilot.getClassMgr().getConfigFile().getValues(
               csName, rteTags[i]);
          }
          catch(Exception e){
          }
          if(mappings!=null){
            rteScriptMappings.add(mappings);
          }
        }
      }
    }

    String [] patternAndReplacements = null;
    String ret = "";
    for(Iterator it=rteScriptMappings.iterator(); it.hasNext();){
      patternAndReplacements = (String []) it.next();
      if(patternAndReplacements!=null && patternAndReplacements.length>1 &&
          name.matches(patternAndReplacements[0])){
        for(int i=1; i<patternAndReplacements.length; ++i){
          if(i>1){
            ret += "\n";
          }
          ret += "source "+name.replaceFirst(patternAndReplacements[0], patternAndReplacements[i]);
        }
      }
    }
    return ret;
  }

  public boolean submit(JobInfo job){
    String delegationId = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
    Debug.debug("using delegation id "+delegationId, 3);
    String proxy;
    try{
      // setup credentials
      proxy=wmProxyAPI.grstGetProxyReq(delegationId);
      wmProxyAPI.grstPutProxy(delegationId, proxy);

      // create script and JDL
      GLiteScriptGenerator scriptGenerator =  new GLiteScriptGenerator(csName);
      String scriptName = runDir(job) + File.separator + job.getName() + ".job";
      String jdlName = runDir(job) + File.separator + job.getName() + ".jdl";
      List uploadFiles = scriptGenerator.createJDL(job, scriptName, jdlName);
      String jdlString = LocalStaticShellMgr.readFile(jdlName);
      Debug.debug("Registering job; "+jdlName+":"+scriptName, 2);
      JobIdStructType jobId = wmProxyAPI.jobRegister(jdlString, delegationId);
      // upload the sandbox
      String protocol = "gsiftp";
      org.glite.wms.wmproxy.StringList list =
        wmProxyAPI.getSandboxDestURI(jobId.getId(), protocol);
      String uri = list.getItem()[0];
      uri = uri+(uri.endsWith("/")?"":"/");
      Debug.debug("Uploading sandbox to "+uri, 2);
      String upFile;
      for(Iterator it=uploadFiles.iterator(); it.hasNext();){
        upFile = (String) it.next();
        TransferControl.upload(new File(upFile), uri,
            GridPilot.getClassMgr().getGlobalFrame().getContentPane());
      }
      // start the job
      wmProxyAPI.jobStart(jobId.getId());
      if(jobId.getId()==null){
        job.setJobStatus(NGComputingSystem.NG_STATUS_ERROR);
        throw new Exception("job id unexpectedly null");
      }
      else{
        job.setJobId(jobId.getId());
      }
    }
    catch(Exception e){
      error = "ERROR: could not run job "+job;
      logFile.addMessage(error, e);
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public void updateStatus(Vector jobs){
    for(int i=0; i<jobs.size(); ++i){
      try{
        updateStatus((JobInfo) jobs.get(i));
      }
      catch(Exception e){
        error = "WARNING: could not update status of job "+jobs.get(i);
        Debug.debug(error, 1);
        e.printStackTrace();
      }
    }
  }
  
  private String statusCodeToString(int statusCode){
    String ret = null;
    switch(statusCode){
    case JobStatus.SUBMITTED:
      ret = "SUBMITTED";
      break;
    case JobStatus.WAITING:
      ret = "WAITING";
      break;
    case JobStatus.READY:
      ret = "READY";
      break;
    case JobStatus.SCHEDULED:
      ret = "SCHEDULED";
      break;
    case JobStatus.RUNNING:
      ret = "RUNNING";
      break;
    case JobStatus.DONE:
      ret = "DONE";
      break;
    case JobStatus.CLEARED:
      ret = "CLEARED";
      break;
    case JobStatus.ABORTED:
      ret = "ABORTED";
      break;
    case JobStatus.CANCELLED:
      ret = "CANCELLED";
      break;
    case JobStatus.UNKNOWN:
      ret = "UNKNOWN";
      break;
    case JobStatus.PURGED:
      ret = "PURGED";
      break;
    default:
      ret = "UNKNOWN";
    }
    return ret;
  }

    private String operationCodeToString(int statusCode){
    String ret = null;
    switch(statusCode){
      case Result.SUCCESS:
        ret = "SUCCESS";
        break;
      case Result.ACCEPTED:
        ret = "ACCEPTED";
        break;
      case Result.CANCEL_FAILURE:
        ret = "CANCEL_FAILURE";
        break;
      case Result.CANCEL_FORBIDDEN:
        ret = "CANCEL_FORBIDDEN";
        break;
      case Result.CONDOR_FAILURE:
        ret = "CONDOR_FAILURE";
        break;
      case Result.FILE_TRANSFER_ERROR:
        ret = "FILE_TRANSFER_ERROR";
        break;
      case Result.GENERIC_FAILURE:
        ret = "GENERIC_FAILURE";
        break;
      case Result.GETOUTPUT_FAILURE:
        ret = "GENERIC_FAILURE";
        break;
      case Result.GETOUTPUT_FORBIDDEN:
        ret = "GETOUTPUT_FORBIDDEN";
        break;
      case Result.GLOBUS_JOBMANAGER_FAILURE:
        ret = "GLOBUS_JOBMANAGER_FAILURE";
        break;
      case Result.JOB_ABORTED:
        ret = "JOB_ABORTED";
        break;
      case Result.JOB_ALREADY_DONE:
        ret = "JOB_ALREADY_DONE";
        break;
      case Result.JOB_CANCELLING:
        ret = "JOB_CANCELLING";
        break;
      case Result.JOB_NOT_FOUND:
        ret = "JOB_NOT_FOUND";
        break;
      case Result.JOB_NOT_OWNER:
        ret = "JOB_NOT_OWNER";
        break;
      case Result.LISTMATCH_FAILURE:
        ret = "LISTMATCH_FAILURE";
        break;
      case Result.LISTMATCH_FORBIDDEN:
        ret = "LISTMATCH_FORBIDDEN";
        break;
      case Result.LOGINFO_FAILURE:
        ret = "LOGINFO_FAILURE";
        break;
      case Result.LOGINFO_FORBIDDEN:
        ret = "LOGINFO_FORBIDDEN";
        break;
      case Result.MARKED_FOR_REMOVAL:
        ret = "MARKED_FOR_REMOVAL";
        break;
      case Result.OUTPUT_NOT_READY:
        ret = "OUTPUT_NOT_READY";
        break;
      case Result.OUTPUT_UNCOMPLETED:
        ret = "OUTPUT_UNCOMPLETED";
        break;
      case Result.STATUS_FAILURE:
        ret = "STATUS_FAILURE";
        break;
      case Result.STATUS_FORBIDDEN:
        ret = "STATUS_FORBIDDEN";
        break;
      case Result.SUBMIT_FAILURE:
        ret = "SUBMIT_FAILURE";
        break;
      case Result.SUBMIT_FORBIDDEN:
        ret = "SUBMIT_FORBIDDEN";
        break;
      case Result.SUBMIT_SKIP:
        ret = "SUBMIT_SKIP";
        break;
      default:
        ret = "UNKNOWN";
    }
    return ret;
  }

  private void updateStatus(JobInfo job) throws
     UnsupportedOperationException, FileNotFoundException, GlobusCredentialException{
    
    Job gliteJob = new Job(job.getJobId());
    try{
      gliteJob.setCredPath(Util.getProxyFile().getAbsoluteFile());
    }
    catch(Exception e){
      error = "Could not set credentials for job "+job;
      logFile.addMessage(error, e);
      e.printStackTrace();
    }
    
    Result result = gliteJob.getStatus() ;
    if(result.getCode()!=Result.SUCCESS){
        throw new UnsupportedOperationException(
            "Unable to retrieve the status of the Job. "+result.getCode()+
            "-->"+operationCodeToString(result.getCode()));
    }
    JobStatus status = (JobStatus) result.getResult();
    
    int statusCode = status.code();
    if(statusCode<0){
      job.setJobStatus(operationCodeToString(JobStatus.UNKNOWN));
      Debug.debug(
          "Status not found for job " + job.getName(), 2);
      return;
    }
    else{
      job.setJobStatus(operationCodeToString(statusCode));
    }

    // Update only if status has changed
    boolean doUpdate = (job.getJobStatus()!=null &&
        operationCodeToString(statusCode).equals(job.getJobStatus()));

    if(doUpdate){
      Debug.debug("Updating status of job "+job.getName(), 2);
      if(job.getJobStatus()==null){
        Debug.debug("No status found for job "+job.getName(), 2);
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      else if(job.getJobStatus().equals(statusCodeToString(JobStatus.DONE))){
        try{
          // get stdout and stderr and any other sandbox files
          gliteJob.getOutput(runDir(job));
          // if this went well we can set the status to done
          if(status.getValInt(JobStatus.DONE_CODE)==0){
            job.setInternalStatus(ComputingSystem.STATUS_DONE);
          }
          else{
            job.setInternalStatus(ComputingSystem.STATUS_FAILED);
          }
        }
        catch(Exception e){
          job.setInternalStatus(ComputingSystem.STATUS_ERROR);
        }
      }
      else if(job.getJobStatus().equals(statusCodeToString(JobStatus.UNKNOWN)) ||
          job.getJobStatus().equals(statusCodeToString(JobStatus.CLEARED)) ||
          job.getJobStatus().equals(statusCodeToString(JobStatus.ABORTED)) ||
          job.getJobStatus().equals(statusCodeToString(JobStatus.CANCELLED)) ||
          job.getJobStatus().equals(statusCodeToString(JobStatus.PURGED))){
        // try to clean up, just in case...
        //getOutput(job);
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      else if(job.getJobStatus().equals(statusCodeToString(JobStatus.RUNNING))){
        job.setInternalStatus(ComputingSystem.STATUS_RUNNING);
      }
      //job.setInternalStatus(ComputingSystem.STATUS_WAIT);
      else{
        Debug.debug("WARNING: unknown status: "+job.getJobStatus(), 1);
        job.setInternalStatus(ComputingSystem.STATUS_WAIT);
      }
    }
  }

  public boolean killJobs(Vector jobsToKill){
    JobInfo job = null;
    Vector errors = new Vector();
    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      try{
        job = (JobInfo) en.nextElement();
        Debug.debug("Cleaning : " + job.getName() + ":" + job.getJobId(), 3);
        wmProxyAPI.jobCancel(job.getJobId());
        wmProxyAPI.jobPurge(job.getJobId());
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
    
    // Clean job off grid. - just in case...
    try{
      wmProxyAPI.jobCancel(job.getJobId());
      wmProxyAPI.jobPurge(job.getJobId());
    }
    catch(Exception e){
      Debug.debug("Could not cancel job. Probably finished. "+
          job.getName()+". "+e.getMessage(), 3);
      //e.printStackTrace();
    }
    
    // Delete files that may have been copied to storage elements
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getJobDefId());
    try{
      for(int i=0; i<outputFileNames.length; ++i){
        outputFileNames[i] = dbPluginMgr.getJobDefOutRemoteName(job.getJobDefId(), outputFileNames[i]);
      }
      TransferControl.deleteFiles(outputFileNames);
    }
    catch(Exception e){
      error = "WARNING: could not delete output file. "+e.getMessage();
      Debug.debug(error, 3);
    }
    // Delete stdout/stderr that may have been copied to final destination
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      try{
        TransferControl.deleteFiles(new String [] {finalStdOut});
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
        TransferControl.deleteFiles(new String [] {finalStdErr});
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
    cleanupRuntimeEnvironments(csName);
  }
  
  public void cleanupRuntimeEnvironments(String csName){
    String runtimeName = null;
    String initText = null;
    String id = "-1";
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
          /*if(initText!=null && !initText.equals("")){
            continue;
          }*/
          id = dbPluginMgr.getRuntimeEnvironmentID(runtimeName, csName);
          if(!id.equals("-1")){
            ok = dbPluginMgr.deleteRuntimeEnvironment(id);
          }
          else{
            ok = false;
          }
          if(!ok){
            Debug.debug("WARNING: could not delete runtime environment " +
                runtimeName+" from database "+dbPluginMgr.getDBName(), 1);
          }
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
  }

  public String getFullStatus(JobInfo job){
    Job gliteJob = new Job(job.getJobId());
    try{
      gliteJob.setCredPath(Util.getProxyFile().getAbsoluteFile());
    }
    catch(Exception e){
      error = "Could not set credentials for job "+job;
      logFile.addMessage(error, e);
      e.printStackTrace();
    }
    return gliteJob.toString();
  }

  /**
   * Checks if runDir(job) exists. If not, attempts to create it. 
   * Returns true if the directory didn't exist and has been successfully
   * created.
   */
  private boolean createMissingWorkingDir(JobInfo job){
    // First check if working directory is there. If not, we may be
    // checking from another machine than the one we submitted from.
    // We just create it...
    boolean getFromfinalDest = false;
    try{
      String dirName = runDir(job);
      if(!LocalStaticShellMgr.existsFile(dirName)){
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
          LocalStaticShellMgr.mkdirs(dirName);
          final String stdoutFile = unparsedWorkingDir+"/"+job.getName() + "/" + job.getName() + ".stdout";
          final String stderrFile = unparsedWorkingDir+"/"+job.getName() + "/" + job.getName() + ".stderr";
          job.setOutputs(stdoutFile, stderrFile);
          DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
          if(!dbPluginMgr.updateJobDefinition(
              job.getJobDefId(),
              new String []{job.getUser(), job.getJobId(), job.getName(),
              job.getStdOut(), job.getStdErr()})){
            logFile.addMessage("DB update(" + job.getJobDefId() + ", " +
                           job.getJobId() + ", " + job.getName() + ", " +
                           job.getStdOut() + ", " + job.getStdErr() +
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
  
  /**
   * This will only work after the job has finished. We just read
   * the stdout/stderr downloaded from the sandbox to the working dir.
   */
  public String[] getCurrentOutputs(JobInfo job) throws IOException{
    // if the job is done, get the files from their final destination
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    String stdOutFile = job.getStdOut();
    String stdErrFile = job.getStdErr();
    String [] res = new String[2];
    boolean getFromfinalDest = createMissingWorkingDir(job);
    // stdout/stderr of running jobs are not accessible
    // Get stdout/stderr of done jobs
    if(getFromfinalDest || job.getDBStatus()==DBPluginMgr.VALIDATED ||
        job.getDBStatus()==DBPluginMgr.UNDECIDED){
      if(!finalStdOut.startsWith("file:")){
        Debug.debug("Downloading stdout of: " + job.getName() + ":" + job.getJobId()+
            " from final destination "+finalStdOut+" to " +
            Util.clearTildeLocally(Util.clearFile(stdOutFile)), 3);
        try {
          TransferControl.download(finalStdOut, new File(Util.clearTildeLocally(Util.clearFile(stdOutFile))),
              GridPilot.getClassMgr().getGlobalFrame().getContentPane());
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
      if(!finalStdErr.startsWith("file:")){
        Debug.debug("Downloading stderr of: " + job.getName() + ":" + job.getJobId()+
            " from final destination "+finalStdErr+" to " +
            Util.clearTildeLocally(Util.clearFile(stdErrFile)), 3);
        try{
          TransferControl.download(finalStdErr, new File(Util.clearTildeLocally(Util.clearFile(stdErrFile))),
              GridPilot.getClassMgr().getGlobalFrame().getContentPane());
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
    }
    if(stdOutFile!=null && !stdOutFile.equals("")){
      // read stdout
      try{
        res[0] = LocalStaticShellMgr.readFile(stdOutFile);
       }
       catch(IOException ae){
         error = "Exception during getCurrentOutputs (stdout) for " + job.getName() + ":" + job.getJobId() + ":\n" +
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
        res[1] = LocalStaticShellMgr.readFile(stdErrFile);
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

  public String[] getScripts(JobInfo job){
    String scriptName = runDir(job) + File.separator + job.getName() + ".job";
    String jdlName = runDir(job) + File.separator + job.getName() + ".jdl";
    return new String [] {jdlName, scriptName};
  }

  public String getUserInfo(String csName){
    String user = null;
    try{
      user = Util.getGridSubject();
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

  /**
   * Moves job.StdOut and job.StdErr to final destination specified in the DB. <p>
   * job.StdOut and job.StdErr are then set to these final values. <p>
   * @return <code>true</code> if the move went ok, <code>false</code> otherwise.
   * (from AtCom1)
   */
  private boolean copyToFinalDest(JobInfo job){
    boolean ok = true;
    /**
     * move downloaded output files to their final destinations -
     * Iff these destinations have the format file:...
     */
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getJobDefId());
    String localName = null;
    String remoteName = null;
    for(int i=0; i<outputFileNames.length; ++i){
      try{
        localName = dbPluginMgr.getJobDefOutLocalName(job.getJobDefId(), outputFileNames[i]);
        remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getJobDefId(), outputFileNames[i]);
        if(remoteName.startsWith("file:")){
          TransferControl.upload(
              new File(runDir(job)+File.separator+localName),
              remoteName,
              GridPilot.getClassMgr().getGlobalFrame().getContentPane());
        }
      }
      catch(Exception e){
        error = "ERROR copying file "+localName+" -> "+remoteName+
        " to final destination: "+e.getMessage();
        logFile.addMessage(error, e);
        ok = false;
      }
    }
    // upload stdout and stderr
    String stdoutDest = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String stderrDest = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    try{
      if(stdoutDest!=null && !stdoutDest.startsWith("/") &&
          !stdoutDest.startsWith("\\\\")){
        TransferControl.upload(
            new File(Util.clearTildeLocally(Util.clearFile(job.getStdOut()))),
            stdoutDest,
            GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.jobMonitor);
        String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
        job.setStdOut(finalStdOut);
      }
      if(stderrDest!=null && !stderrDest.startsWith("/") &&
          !stderrDest.startsWith("\\\\")){
        TransferControl.upload(
            new File(Util.clearTildeLocally(Util.clearFile(job.getStdErr()))),
            stderrDest,
            GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.jobMonitor);
        String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
        job.setStdOut(finalStdErr);
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not upload stdout/stderr of "+job, e);
      e.printStackTrace();
      ok = false;
    }
    return ok;
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
      // Clean the job off the grid
      try{
        try{
          Debug.debug("Cleaning : " + job.getName() + ":" + job.getJobId(), 3);
          wmProxyAPI.jobPurge(job.getJobId());
        }
        catch(Exception ae){
          logFile.addMessage("Exception during purging of " + job.getName() + ":" + job.getJobId() + ":\n" +
                             "\tException\t: " + ae.getMessage(), ae);
          ae.printStackTrace();
        }
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
    // preserve ~ in tmp stdout/stderr, so checking from another machine might work
    final String stdoutFile = unparsedWorkingDir+"/"+job.getName() + "/" + "stdout";
    final String stderrFile = unparsedWorkingDir+"/"+job.getName() + "/" + "stderr";
    job.setOutputs(stdoutFile, stderrFile);
    return true;
  }

  public String getError(String csName){
    return error;
  }

}