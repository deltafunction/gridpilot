package gridpilot.csplugins.ng;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.Vector;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;
import org.nordugrid.gridftp.ARCGridFTPJob;
import org.nordugrid.gridftp.ARCGridFTPJobException;
import org.nordugrid.is.ARCDiscovery;
import org.nordugrid.multithread.TaskResult;

import gridpilot.ComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LocalShellMgr;
import gridpilot.LogFile;
import gridpilot.GridPilot;
import gridpilot.StatusBar;
import gridpilot.Util;
import gridpilot.fsplugins.gridftp.GridFTPFileSystem;

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
  private ARCDiscovery arcDiscover;
  private String defaultUser;

  public NGComputingSystem(String _csName){
    csName = _csName;
    workingDir = GridPilot.getClassMgr().getConfigFile().getValue(csName, "working directory");
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
    ngSubmission = new NGSubmission(csName);
    logFile = GridPilot.getClassMgr().getLogFile();
       
    // Information system
    arcDiscover = new ARCDiscovery("ldap://index4.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
    arcDiscover.addGIIS("ldap://index1.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
    arcDiscover.addGIIS("ldap://index2.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");
    arcDiscover.addGIIS("ldap://index3.nordugrid.org:2135/Mds-Vo-Name=NorduGrid,O=Grid");    
    defaultUser = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "user");
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
      e.printStackTrace();
      return false;
    }
  }


  public void updateStatus(Vector jobs){
    for(int i=0; i<jobs.size(); ++i){
      updateStatus((JobInfo) jobs.get(i));
    }
  }

  private int updateStatus(JobInfo job){
    
    boolean ok = false;
    String jobId = job.getJobId();
    if(jobId!=null){ // job already submitted
      String statusLine;
      statusLine = getFullStatus(job);
      if(statusLine!=null){
        ok = extractStatus(job, statusLine);
      }
      else{
        ok = true;
      }
    }

    if(ok){
      Debug.debug("Updating status of job "+job.getName(), 2);
      if(job.getJobStatus()==null){
        Debug.debug("No status found for job "+job.getName(), 2);
        job.setInternalStatus(ComputingSystem.STATUS_WAIT);
      }
      else if(job.getJobStatus().equals(NG_STATUS_FINISHED)){
        if(getOutput(job)){
          job.setInternalStatus(ComputingSystem.STATUS_DONE);
        }
        else{
          job.setInternalStatus(ComputingSystem.STATUS_ERROR);
        }
      }
      if(job.getJobStatus().equals(NG_STATUS_FAILURE)){
        //getOutput(job);
        job.setInternalStatus(ComputingSystem.STATUS_FAILED);
      }
      if(job.getJobStatus().equals(NG_STATUS_ERROR)){
        getOutput(job);
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      if(job.getJobStatus().equals(NG_STATUS_DELETED)){
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      if(job.getJobStatus().equals(NG_STATUS_FAILED)){
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      if(job.getJobStatus().equals(NG_STATUS_INLRMSR)){
        job.setInternalStatus(ComputingSystem.STATUS_RUNNING);
      }
      job.setInternalStatus(ComputingSystem.STATUS_WAIT);
    }
    return job.getInternalStatus();
  }
  
  private ARCGridFTPJob getGridJob(JobInfo job) throws ARCGridFTPJobException{
    
    String submissionHost = "gsiftp://"+job.getHost();
    String jobID = job.getJobId().substring(job.getJobId().lastIndexOf("/"));
    int lastSlash = job.getJobId().lastIndexOf("/");
    if(lastSlash>-1){
      jobID = job.getJobId().substring(lastSlash + 1);
    }
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
        Debug.debug("Killing : " + job.getName() + ":" + job.getJobId(), 3);
        ARCGridFTPJob gridJob = getGridJob(job);
        gridJob.cancel();
       }
       catch(Exception ae){
         errors.add(ae.getMessage());
         logFile.addMessage("Exception during killing of " + job.getName() + ":" + job.getJobId() + ":\n" +
                            "\tException\t: " + ae.getMessage(), ae);
         ae.printStackTrace();
       }
    }    
    if(errors.size()!=0){
      return false;
    }
    else{
      return true;
    }
  }

  public void clearOutputMapping(JobInfo job){
    
    GridFTPFileSystem gridftpFileSystem = GridPilot.getClassMgr().getGridFTPFileSystem();
    
    // Delete files that may have been copied to storage elements
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String[] outputMapping = dbPluginMgr.getOutputMapping(job.getJobDefId());
    String fileName;
    for(int i=0; i<outputMapping.length; ++i){
      fileName = Util.addFile(outputMapping[2*i+1]);
      try{
        gridftpFileSystem.delete(fileName);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not delete "+fileName+". "+e.getMessage(), 2);
      }
    }

    // Delete stdout/err that may have been copied to final destination
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      try{
        gridftpFileSystem.delete(finalStdOut);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not delete "+finalStdOut+". "+e.getMessage(), 2);
      }
    }
    if(finalStdErr!=null && finalStdErr.trim().length()>0){
      try{
        gridftpFileSystem.delete(finalStdErr);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not delete "+finalStdErr+". "+e.getMessage(), 2);
      }
    }
    
    // Delete the local run directory
    String runDir = runDir(job);
    try{
      Debug.debug("Clearing "+runDir, 2);
      LocalShellMgr.deleteDir(new File(runDir));
    }
    catch(Exception ioe){
      logFile.addMessage("Exception during clearOutputMapping of job " + job.getName()+ "\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
    }

    // Kill the job and clean up
    try{
      ARCGridFTPJob gridJob = getGridJob(job);
      gridJob.cancel();
      gridJob.clean();
    }
    catch(Exception ioe){
      Debug.debug("Exception during clearOutputMapping of job " + job.getName() + " :\n" +
                                  "\tException\t: " + ioe.getMessage()+ioe, 2);
    }
  }

  public void exit(){
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
      LocalShellMgr.moveFile(dirName, dirName+"."+dateString);
    }
    catch(Exception ioe){
      logFile.addMessage("IOException during job " + job.getName() + " get output :\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
      return false;
    }
    
    // Get the outputs
    try{
      Debug.debug("Getting : " + job.getName() + ":" + job.getJobId(), 3);
      ARCGridFTPJob gridJob = getGridJob(job);
      gridJob.get(dirName);
     }
     catch(Exception ae){
       logFile.addMessage("Exception during get outputs of " + job.getName() + ":" + job.getJobId() + ":\n" +
                          "\tException\t: " + ae.getMessage(), ae);
       ae.printStackTrace();
     }
    
    // Rename stdout and stderr to the name specified in the job description,
    // and move them one level up
    if(job.getStdOut()!=null && !job.getStdOut().equals("")){      
      try{
        LocalShellMgr.copyFile(dirName+File.separator+"stdout", job.getStdOut());
      } 
      catch(Exception ioe){
        logFile.addMessage("IOException during job " + job.getName() + " getFullStatus :\n" +
                                    "\tException\t: " + ioe.getMessage(), ioe);
        return false;
      }
    }
    if(job.getStdErr() != null && !job.getStdErr().equals("")){
      try{
        LocalShellMgr.copyFile(dirName+File.separator, job.getStdErr());
      }
      catch(Exception ioe){
        logFile.addMessage("IOException during job " + job.getName() + " getOutput :\n" +
                                    "\tException\t: " + ioe.getMessage(), ioe);
        return false;
      }
    }
    return true;
  }

  public String getFullStatus(JobInfo job){
    String state = "";
    try{
      Debug.debug("Getting " + job.getJobId(), 3);
      ARCGridFTPJob gridJob = getGridJob(job);
      // TODO: use information system
      state = gridJob.state();
    }
    catch(Exception ioe){
      logFile.addMessage("IOException during job " + job.getName() + " getFullStatus :\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
      return ioe.getMessage();
    }
    return state;
  }

  public String [] getCurrentOutputs(JobInfo job){
    
    String stdOutFile = job.getStdOut();
    String stdErrFile = job.getStdErr();

    if(stdOutFile!=null && !stdOutFile.equals("") &&
        LocalShellMgr.existsFile(stdOutFile)){
      LocalShellMgr.moveFile(stdOutFile, stdOutFile+".bk");
    }
    if(stdErrFile!=null && !stdErrFile.equals("") &&
        LocalShellMgr.existsFile(stdErrFile)){
      LocalShellMgr.moveFile(stdErrFile, stdErrFile+".bk");
    }
    
    if(!syncCurrentOutputs(job)){
      try{
        LocalShellMgr.deleteFile(stdOutFile);
      }
      catch(Exception e){
      }
      try{
        LocalShellMgr.deleteFile(stdErrFile);
      }
      catch(Exception e){
      }
      try{
        LocalShellMgr.moveFile(stdOutFile+".bk", stdOutFile);
      }
      catch(Exception e){
      }
      try{
        LocalShellMgr.moveFile(stdErrFile+".bk", stdErrFile);
      }
      catch(Exception e){
      }
    }
    
    String [] res = new String[2];

    if(stdOutFile!=null && !stdOutFile.equals("")){
      // get stdout
      try{
        res[0] = LocalShellMgr.readFile(stdOutFile);
       }
       catch(Exception ae){
         logFile.addMessage("Exception during get stdout of " + job.getName() + ":" + job.getJobId() + ":\n" +
                            "\tException\t: " + ae.getMessage(), ae);
         ae.printStackTrace();
         res[0] = "";
       }
    }
    if(stdErrFile!=null && !stdErrFile.equals("")){
      // get stderr
      try{
        res[1] = LocalShellMgr.readFile(stdErrFile);
       }
       catch(Exception ae){
         logFile.addMessage("Exception during get stderr of " + job.getName() + ":" + job.getJobId() + ":\n" +
                            "\tException\t: " + ae.getMessage(), ae);
         ae.printStackTrace();
         res[1] = "";
       }
    }
    return res;
  }
  
  // Copy stdout+stderr to local files
  public boolean syncCurrentOutputs(JobInfo job){
    
    StatusBar statusBar = GridPilot.getClassMgr().getStatusBar();

    if(arcDiscover.getClusters()==null || arcDiscover.getClusters().size()==0){
      statusBar.setLabel("Discovering resources, please wait...");
      arcDiscover.discoverAll();
      statusBar.setLabel("Discovering resources done");
    }
    long start = System.currentTimeMillis();
    long limit = 10000;
    long offset = 2000; // some +- coefficient
    Collection foundResources = null;
    try{
      statusBar.setLabel("Finding jobs, please wait...");
      foundResources = arcDiscover.findUserJobs(
          job.getUser(), 20, limit);
      statusBar.setLabel("Finding jobs done");
    }
    catch(InterruptedException e){
      Debug.debug("User interrupt of job checking!", 2);
      return false;
    }
    long end = System.currentTimeMillis();
    if((end - start) < limit + offset){
      Debug.debug("WARNING: failed to stay within time limit of "+limit/1000+" seconds.", 1);
    }
    if(foundResources.size()==0){
      statusBar.setLabel("Failed to find authorized queues!");
      Debug.debug("WARNING: failed to find authorized queues.", 1);
      logFile.addMessage("WARNING: failed to find authorized queues. :\n" +
          "\tDN\t: " + job.getUser());
      return true;
    }
    
    String dirName = runDir(job)+File.separator+job.getJobId();
    try{
      Debug.debug("Getting : " + job.getName() + ":" + job.getJobId(), 3);
      Debug.debug("Description : " + ((TaskResult) foundResources.toArray()[0]).getWorkDescription(), 3);
      ARCGridFTPJob gridJob = getGridJob(job);
      Debug.debug("Getting stdout of: " + job.getName() + ":" + job.getJobId(), 3);
      gridJob.getOutputFile("stdout", dirName);
      Debug.debug("Getting stderr of: " + job.getName() + ":" + job.getJobId(), 3);
      gridJob.getOutputFile("stderr", dirName);
    }
    catch(Exception ae){
      logFile.addMessage("Exception during get stdout of " + job.getName() + ":" + job.getJobId() + ":\n" +
                         "\tException\t: " + ae.getMessage(), ae);
      ae.printStackTrace();
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
      logFile.addMessage("Exception during getUserInfo\n" +
                                 "\tException\t: " + ioe.getMessage(), ioe);
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
    return copyToFinalDest(job);
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
    
    GridFTPFileSystem gridftpFileSystem = GridPilot.getClassMgr().getGridFTPFileSystem();

    /**
     * move temp StdOut -> finalStdOut
     */
    if(finalStdOut!=null && finalStdOut.trim().length()!=0){
      try{
        syncCurrentOutputs(job);
        if(!LocalShellMgr.existsFile(job.getStdOut())){
          logFile.addMessage("Post processing : File " + job.getStdOut() + " doesn't exist");
          return false;
        }
      }
      catch(Throwable e){
        Debug.debug("ERROR checking for stdout: "+e.getMessage(), 2);
        logFile.addMessage("ERROR checking for stdout: "+e.getMessage());
        //throw e;
      }
      Debug.debug("Post processing : Renaming " + job.getStdOut() + " in " + finalStdOut, 2);
      // if(!shell.moveFile(job.getStdOut(), finalStdOut)){
      try{
        gridftpFileSystem.put(new File(job.getStdOut()), finalStdOut);
      }
      catch(Throwable e){
        Debug.debug("ERROR copying stdout: "+e.getMessage(), 2);
        logFile.addMessage("ERROR copying stdout: "+e.getMessage());
        //throw e;
      }
      job.setStdOut(finalStdOut);
    }

    /**
     * move temp StdErr -> finalStdErr
     */

    if(finalStdErr!=null && finalStdErr.trim().length()!=0){
      try{
        if(!LocalShellMgr.existsFile(job.getStdErr())){
          logFile.addMessage("Post processing : File " + job.getStdErr() + " doesn't exist");
          return false;
        }
      }
      catch(Throwable e){
        Debug.debug("ERROR checking for stderr: "+e.getMessage(), 2);
        logFile.addMessage("ERROR checking for stderr: "+e.getMessage());
        //throw e;
      }
      Debug.debug("Post processing : Renaming " + job.getStdErr() + " in " + finalStdErr,2);
      //shell.moveFile(job.getStdOut(), finalStdOutName);
      try{
        gridftpFileSystem.put(new File(job.getStdErr()), finalStdErr);
      }
      catch(Throwable e){
        Debug.debug("ERROR copying stderr: "+e.getMessage(), 2);
        logFile.addMessage("ERROR copying stderr: "+e.getMessage());
        //throw e;
      }
      job.setStdErr(finalStdErr);
    }
    return true;
  }
  
  private static boolean extractStatus(JobInfo job, String line) {

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
    if(status == null){
      job.setJobStatus(NGComputingSystem.NG_STATUS_ERROR);
      GridPilot.getClassMgr().getLogFile().addMessage(
          "Status not found for job " + job.getName() +" : \n" + line);
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
      if(job.getJobStatus() != null && job.getJobStatus().equals(status))
        return false;
      else{
        job.setJobStatus(status);
        return true;
      }
    }
  }

  private static String getValueOf(String attribute, String out){
    Debug.debug("getValueOf : " + attribute + "\n" + out, 1);

    int index = out.indexOf(attribute);
    if(index == -1)
      return null;

    StringTokenizer st = new StringTokenizer(out.substring(index+attribute.length()+1,
        out.length()));

    String res = st.nextToken();
    if(res.endsWith(":"))
      res += " "+st.nextToken();

    return res;
  }

}