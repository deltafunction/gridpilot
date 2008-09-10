package gridpilot.csplugins.glite;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.JOptionPane;
import javax.xml.rpc.holders.LongHolder;

import gridfactory.common.ConfigFile;
import gridfactory.common.ConfirmBox;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.Shell;
import gridfactory.common.VirtualMachine;

import gridpilot.MyComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.MyJobInfo;
import gridpilot.MyLogFile;
import gridpilot.MySSL;
import gridpilot.GridPilot;
import gridpilot.TransferControl;
import gridpilot.MyUtil;

import org.glite.jdl.JobAd;
import org.glite.wms.wmproxy.JobIdStructType;
import org.glite.wms.wmproxy.StringAndLongList;
import org.glite.wms.wmproxy.StringAndLongType;
import org.glite.wms.wmproxy.WMProxyAPI;
import org.globus.gsi.GlobusCredentialException;
import org.globus.mds.MDS;
import org.globus.mds.MDSException;
import org.globus.mds.MDSResult;
import org.safehaus.uuid.UUIDGenerator;

/**
 * Main class for the LSF plugin.
 */

public class GLiteComputingSystem implements MyComputingSystem{

  private String csName = null;
  private MyLogFile logFile = null;
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
  private String delegationId = null;
  
  private static boolean CONFIRM_RUN_DIR_CREATION = false;
  private static String BDII_PORT = "2170";
  private static String BDII_BASE_DN = "mds-vo-name=local,o=grid";
  //private static String SANDBOX_PROTOCOL = "gsiftp";
  private static String SANDBOX_PROTOCOL = "all";
  
  private static String GLITE_STATUS_UNKNOWN = "Unknown";
  private static String GLITE_STATUS_DONE = "Done";
  private static String GLITE_STATUS_WAIT = "Wait";
  private static String GLITE_STATUS_ERROR = "Error";
  private static String GLITE_STATUS_FAILED = "Failed";
  private static String GLITE_STATUS_RUNNING = "Running";

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
    workingDir = MyUtil.clearTildeLocally(MyUtil.clearFile(workingDir));
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
        GridPilot.getClassMgr().getSSL().getGridCredential();
      }
      catch(Exception ee){
        ee.printStackTrace();
      }

      Debug.debug("Creating new WMProxyAPI; "+MySSL.getProxyFile().getAbsolutePath()+
          " : "+GridPilot.getClassMgr().getSSL().getCaCertsTmpDir(), 2);
      wmProxyAPI = new WMProxyAPI(wmUrl,
          MySSL.getProxyFile().getAbsolutePath(),
            GridPilot.getClassMgr().getSSL().getCaCertsTmpDir());
      
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
    return workingDir+File.separator+job.getName();
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
                if(!rte.toLowerCase().startsWith("vo-") || rteVos[j]!=null &&
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
    
    // At least for now, we only have Linux resources on NorduGrid
    runtimes.add("Linux");
    
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
    try{
      if(delegationId==null){
        delegationId = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
        Debug.debug("using delegation id "+delegationId, 3);
        String vmProxyVersion = wmProxyAPI.getVersion();
        Debug.debug("wmProxyAPI version: "+vmProxyVersion, 3);
        // setup credentials
        Debug.debug("putting proxy", 3);
        String proxyReq = wmProxyAPI.grstGetProxyReq(delegationId);
        Debug.debug("proxy req "+proxyReq, 3);
        wmProxyAPI.grstPutProxy(delegationId, proxyReq);
      }
      // create script and JDL
      String scriptName = runDir(job) + File.separator + job.getName() + ".job";
      String jdlName = runDir(job) + File.separator + job.getName() + ".jdl";
      GLiteScriptGenerator scriptGenerator =  new GLiteScriptGenerator(csName, (MyJobInfo) job,
          scriptName, jdlName);
      scriptGenerator.createJDL();
      scriptGenerator.createScript();
      JobAd jad = new JobAd();
      jad.fromFile(jdlName);
      String jdlString = jad.toString();
      // check if any resources match
      StringAndLongList result = null;
      try{
        result = wmProxyAPI.jobListMatch(jdlString, delegationId);
      }
      catch(Exception e){
        e.printStackTrace();
      }    
      if(result==null){
        logFile.addMessage("No Computing Element matching your job requirements has been found!");
        //return false;
      }
      else{
        // list of CE's+their ranks
        StringAndLongType [] list = (StringAndLongType[ ]) result.getFile ();
        if (list != null) {
          int size = list.length ;
          for(int i=0; i<size ; i++){
            String ce = list[i].getName();
            Debug.debug( "- " + ce + list[i].getSize(), 2);
          }
        }
      }
      // register the job
      Debug.debug("Registering job; "+scriptName+":"+jdlString, 2);
      JobIdStructType jobId = wmProxyAPI.jobRegister(jdlString, delegationId);
      // upload the sandbox
      org.glite.wms.wmproxy.StringList list =
        wmProxyAPI.getSandboxDestURI(jobId.getId(), SANDBOX_PROTOCOL);
      String uri = list.getItem()[0];
      uri = uri+(uri.endsWith("/")?"":"/");
      Debug.debug("Uploading sandbox to "+uri, 2);
      String upFile;
      for(Iterator it=scriptGenerator.localInputFilesList.iterator(); it.hasNext();){
        upFile = (String) it.next();
        TransferControl.upload(
            new File(MyUtil.clearTildeLocally(MyUtil.clearFile(upFile))),
            uri,
            GridPilot.getClassMgr().getGlobalFrame().getContentPane());
      }
      // start the job
      wmProxyAPI.jobStart(jobId.getId());
      if(jobId.getId()==null){
        job.setStatusError();
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
      //delegationId = null;
      return false;
    }
    return true;
  }

  public void updateStatus(Vector<JobInfo> jobs){
    for(int i=0; i<jobs.size(); ++i){
      try{
        updateStatus((MyJobInfo) jobs.get(i));
      }
      catch(Exception e){
        error = "WARNING: could not update status of job "+jobs.get(i);
        Debug.debug(error, 1);
        e.printStackTrace();
      }
    }
  }
  
  /*private String statusCodeToString(int statusCode){
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
  }*/

    /*private String operationCodeToString(int statusCode){
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
  }*/

  private void updateStatus(MyJobInfo job) throws
     UnsupportedOperationException, FileNotFoundException, GlobusCredentialException{
    
    String status = getStatus(job);
    
    // Update only if status has changed
    boolean doUpdate = (job.getCSStatus()!=null &&
        !status.equals(job.getCSStatus()));

    if(status==null){
      job.setCSStatus(GLITE_STATUS_UNKNOWN);
      Debug.debug(
          "Status not found for job " + job.getName(), 2);
      return;
    }
    else{
      job.setCSStatus(status);
    }

    if(doUpdate){
      Debug.debug("Updating status of job "+job.getName(), 2);
      if(job.getCSStatus()==null){
        Debug.debug("No status found for job "+job.getName(), 2);
        job.setCSStatus(GLITE_STATUS_ERROR);
      }
      else if(status.equals(GLITE_STATUS_DONE)){
        try{
          // get stdout and stderr and any other sandbox files
          getOutputs(job);
          // if this went well we can set the status to done
          job.setStatusFailed();
        }
        catch(Exception e){
          job.setCSStatus(GLITE_STATUS_FAILED);
        }
      }
      else if(status.equals(GLITE_STATUS_ERROR)){
        // try to clean up, just in case...
        //getOutput(job);
        job.setStatusError();
      }
      else if(status.equals(GLITE_STATUS_RUNNING)){
        job.setStatusRunning();
      }
      //job.setInternalStatus(ComputingSystem.STATUS_WAIT);
      else{
        Debug.debug("WARNING: unknown status: "+status, 1);
        job.setCSStatus(GLITE_STATUS_WAIT);
      }
    }
  }

  public boolean killJobs(Vector jobsToKill){
    MyJobInfo job = null;
    Vector errors = new Vector();
    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      try{
        job = (MyJobInfo) en.nextElement();
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
      wmProxyAPI.jobCancel(job.getJobId());
      wmProxyAPI.jobPurge(job.getJobId());
    }
    catch(Exception e){
      Debug.debug("Could not cancel job. Probably finished. "+
          job.getName()+". "+e.getMessage(), 3);
      //e.printStackTrace();
    }
    
    // Delete files that may have been copied to storage elements
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getIdentifier());
    try{
      for(int i=0; i<outputFileNames.length; ++i){
        outputFileNames[i] = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFileNames[i]);
      }
      TransferControl.deleteFiles(outputFileNames);
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

  // Download all sandbox output files to the local run directory.
  public void getOutputs(MyJobInfo job) throws Exception{
    String url = null;
    StringAndLongList outList = wmProxyAPI.getOutputFileList(job.getJobId(), SANDBOX_PROTOCOL);
    StringAndLongType [] outs = outList.getFile();
    for(int i=0; i<outs.length; ++i){
      url = outs[i].getName();
      if(url!=null){
        if(url.endsWith("stdout")){
          TransferControl.download(url, new File(MyUtil.clearTildeLocally(MyUtil.clearFile(job.getOutTmp()))), null);
        }
        else if(url.endsWith("stderr")){
          TransferControl.download(url, new File(MyUtil.clearTildeLocally(MyUtil.clearFile(job.getErrTmp()))), null);
        }
        else{
          TransferControl.download(url, new File(MyUtil.clearTildeLocally(MyUtil.clearFile(runDir(job)))), null);
        }
      }
    }
  }
  
  // Copy stdout+stderr to local files
  public boolean syncCurrentOutputs(MyJobInfo job){
    try{
      Debug.debug("Syncing " + job.getName() + ":" + job.getJobId(), 3);
      
      String dirName = runDir(job);

      DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
      String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
      String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());

      boolean getFromfinalDest = createMissingWorkingDir(job);
      if(!getFromfinalDest && !job.getCSStatus().equals(GLITE_STATUS_DONE) &&
          job.getDBStatus()!=DBPluginMgr.UNDECIDED){
        Debug.debug("Downloading stdout/err of running job: " + job.getName() + " : " + job.getJobId() +
            " : " + job.getCSStatus()+" to " + dirName, 3);
        try{
          String stdoutUrl = null;
          String stderrUrl = null;
          StringAndLongList outList = wmProxyAPI.getOutputFileList(job.getJobId(), SANDBOX_PROTOCOL);
          StringAndLongType [] outs = outList.getFile();
          if(outs!=null){
            for(int i=0; i<outs.length; ++i){
              if(outs[i].getName().endsWith("stdout")){
                stdoutUrl = outs[i].getName();
              }
              else if(outs[i].getName().endsWith("stderr")){
                stderrUrl = outs[i].getName();
              }
            }
          }
          if(stdoutUrl!=null){
            TransferControl.download(stdoutUrl, new File(MyUtil.clearTildeLocally(MyUtil.clearFile(job.getOutTmp()))), null);
          }
          if(stderrUrl!=null){
            TransferControl.download(stderrUrl, new File(MyUtil.clearTildeLocally(MyUtil.clearFile(job.getErrTmp()))), null);
          }
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
      else{
        if(getFromfinalDest || !finalStdOut.startsWith("file:")){
          Debug.debug("Downloading stdout of: " + job.getName() + ":" + job.getJobId()+
              " from final destination "+finalStdOut+" to " +
              MyUtil.clearTildeLocally(MyUtil.clearFile(job.getOutTmp())), 3);
          TransferControl.download(finalStdOut, new File(MyUtil.clearTildeLocally(MyUtil.clearFile(job.getOutTmp()))),
              GridPilot.getClassMgr().getGlobalFrame().getContentPane());
        }
        if(getFromfinalDest || !finalStdErr.startsWith("file:")){
          Debug.debug("Downloading stderr of: " + job.getName() + ":" + job.getJobId()+
              " from final destination "+finalStdErr+" to " +
              MyUtil.clearTildeLocally(MyUtil.clearFile(job.getErrTmp())), 3);
          TransferControl.download(finalStdErr, new File(MyUtil.clearTildeLocally(MyUtil.clearFile(job.getErrTmp()))),
              GridPilot.getClassMgr().getGlobalFrame().getContentPane());
        }
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

  public String getStatus(MyJobInfo job){
    syncCurrentOutputs(job);
    String stdoutFileName = job.getOutTmp();
    File stdoutFile = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(stdoutFileName)));
    if(stdoutFile.exists()){
      boolean stdoutOK = false;
      try{
        RandomAccessFile raf = new RandomAccessFile(stdoutFile, "r");
        String line = "";
        while(line!=null){
           line = raf.readLine();
           if(line.equals("job "+job.getIdentifier()+" done")){
             stdoutOK = true;
             break;
           }
        }
        raf.close();
      }
      catch(Exception e){
        logFile.addMessage("Could not get status of job "+job, e);
        return GLITE_STATUS_ERROR;
      }
      if(stdoutOK){
        return GLITE_STATUS_DONE;
      }
    }
    return GLITE_STATUS_UNKNOWN;
  }
  
  public String getFullStatus(JobInfo job){
    String ret = "";
    //ret += "Job ID: "+job.getJobId()+"\n";
    try{
      ret += "Status: "+getStatus((MyJobInfo) job)+"\n";
    }
    catch(Exception e){
      e.printStackTrace();
    }
    /*try{
      ret += "Stdout: "+job.getOutTmp()+"\n";
      ret += "Stderr: "+job.getErrTmp()+"\n";
    }
    catch(Exception e){
      e.printStackTrace();
    }*/
    try{
      ret += "Proxy info: "+wmProxyAPI.getJobProxyInfo(job.getJobId())+"\n";
    }
    catch(Exception e){
      e.printStackTrace();
    }
    try{
      StringAndLongList outList = wmProxyAPI.getOutputFileList(job.getJobId(), SANDBOX_PROTOCOL);
      StringAndLongType [] outs = outList.getFile();
      String fileList = "";
      for(int i=0; i<outs.length; ++i){
        fileList += "    "+outs[i].getName() + outs[i].getSize() + "\n";
      }
      ret += "Sandbox output files:\n"+fileList;
    }
    catch(Exception e){
      e.printStackTrace();
    }
    try{
      ret += "Input sandbox: "+
      MyUtil.arrayToString(
          wmProxyAPI.getSandboxDestURI(job.getJobId(), SANDBOX_PROTOCOL).getItem())+"\n";
    }
    catch(Exception e){
      e.printStackTrace();
    }
    try{
      ret += "Transfer protocols: "+wmProxyAPI.getTransferProtocols()+"\n";
    }
    catch(Exception e){
      e.printStackTrace();
    }
    try{
      LongHolder softLimit = new LongHolder();
      LongHolder hardLimit = new LongHolder();
      wmProxyAPI.getFreeQuota(softLimit, hardLimit);
      ret += "Quota limits: "+softLimit.value+":"+hardLimit.value+"\n";
    }
    catch(Exception e){
      e.printStackTrace();
    }
    try{
      ret += "WMProxy version: "+wmProxyAPI.getVersion()+"\n";
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return ret;
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
  
  /**
   * This will only work after the job has finished. We just read
   * the stdout/stderr downloaded from the sandbox to the working dir.
   */
  public String[] getCurrentOutput(JobInfo job) throws IOException{
    // if the job is done, get the files from their final destination
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());
    String stdOutFile = job.getOutTmp();
    String stdErrFile = job.getErrTmp();
    String [] res = new String[2];
    boolean getFromfinalDest = createMissingWorkingDir((MyJobInfo) job);
    // stdout/stderr of running jobs are not accessible
    // Get stdout/stderr of done jobs
    if(getFromfinalDest || job.getDBStatus()==DBPluginMgr.VALIDATED ||
        job.getDBStatus()==DBPluginMgr.UNDECIDED){
      if(!finalStdOut.startsWith("file:")){
        Debug.debug("Downloading stdout of: " + job.getName() + ":" + job.getJobId()+
            " from final destination "+finalStdOut+" to " +
            MyUtil.clearTildeLocally(MyUtil.clearFile(stdOutFile)), 3);
        try {
          TransferControl.download(finalStdOut, new File(MyUtil.clearTildeLocally(MyUtil.clearFile(stdOutFile))),
              GridPilot.getClassMgr().getGlobalFrame().getContentPane());
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
      if(!finalStdErr.startsWith("file:")){
        Debug.debug("Downloading stderr of: " + job.getName() + ":" + job.getJobId()+
            " from final destination "+finalStdErr+" to " +
            MyUtil.clearTildeLocally(MyUtil.clearFile(stdErrFile)), 3);
        try{
          TransferControl.download(finalStdErr, new File(MyUtil.clearTildeLocally(MyUtil.clearFile(stdErrFile))),
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
        res[0] = LocalStaticShell.readFile(stdOutFile);
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

  public String[] getScripts(JobInfo job){
    String scriptName = runDir(job) + File.separator + job.getName() + ".job";
    String jdlName = runDir(job) + File.separator + job.getName() + ".jdl";
    return new String [] {jdlName, scriptName};
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

  /**
   * Moves job.StdOut and job.StdErr to final destination specified in the DB. <p>
   * job.StdOut and job.StdErr are then set to these final values. <p>
   * @return <code>true</code> if the move went ok, <code>false</code> otherwise.
   * (from AtCom1)
   */
  private boolean copyToFinalDest(MyJobInfo job){
    boolean ok = true;
    /**
     * move downloaded output files to their final destinations -
     * Iff these destinations have the format file:...
     */
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getIdentifier());
    String localName = null;
    String remoteName = null;
    for(int i=0; i<outputFileNames.length; ++i){
      try{
        localName = dbPluginMgr.getJobDefOutLocalName(job.getIdentifier(), outputFileNames[i]);
        remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFileNames[i]);
        if(remoteName.startsWith("file:")){
          TransferControl.upload(
              new File(new File(MyUtil.clearTildeLocally(MyUtil.clearFile(runDir(job))))+File.separator+localName),
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
    String stdoutDest = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
    String stderrDest = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());
    // Horrible clutch because Globus gass copy fails on empty files...
    boolean emptyFile = false;
    try{
      if(stdoutDest!=null && !stdoutDest.startsWith("/") &&
          !stdoutDest.startsWith("\\\\")){
        File stdoutSourceFile = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(job.getOutTmp())));
        emptyFile = stdoutDest.startsWith("https") && stdoutSourceFile.length()==0;
        TransferControl.upload(
            stdoutSourceFile,
            stdoutDest,
            GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.jobMonitor);
        String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
        job.setOutTmp(finalStdOut);
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not upload stdout of "+job+". Probably empty.", e);
      e.printStackTrace();
      ok = ok && emptyFile;
    }
    try{
      if(stderrDest!=null && !stderrDest.startsWith("/") &&
          !stderrDest.startsWith("\\\\")){
        File stderrSourceFile = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(job.getErrTmp())));
        emptyFile = stdoutDest.startsWith("https") && stderrSourceFile.length()==0;
        TransferControl.upload(
            stderrSourceFile,
            stderrDest,
            GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.jobMonitor);
        String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());
        job.setOutTmp(finalStdErr);
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not upload stderr of "+job+". Probably empty.", e);
      e.printStackTrace();
      ok = ok && emptyFile;
    }
    return ok;
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
    ((MyJobInfo) job).setOutputs(stdoutFile, stderrFile);
    return true;
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