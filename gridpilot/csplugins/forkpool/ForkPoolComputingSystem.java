package gridpilot.csplugins.forkpool;

import java.io.File;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import com.jcraft.jsch.JSchException;


import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.LocalShell;
import gridfactory.common.Shell;
import gridpilot.MyComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.GridPilot;
import gridpilot.MyJobInfo;
import gridpilot.MySecureShell;
import gridpilot.MyUtil;
import gridpilot.csplugins.fork.ForkComputingSystem;
import gridpilot.csplugins.fork.ForkScriptGenerator;

public class ForkPoolComputingSystem extends ForkComputingSystem implements MyComputingSystem {

  // One ShellMgr per host
  protected HashMap<String, Shell> remoteShellMgrs = null;
  protected String [] hosts = null;
  protected String [] maxJobs = null;
  // Map of host -> Set of jobs that are being submmited
  protected HashMap<String, HashSet<String>> submittingHostJobs = null;
  protected String [] users = null;
  protected String [] passwords = null;
  
  public ForkPoolComputingSystem(String _csName) throws Exception{
    super(_csName);
    mkLocalOSRTE = false;
    includeVMRTEs = false;
    basicOSRTES = new String [] {"Linux"};
    setupRemoteShellMgrs();
    GridPilot.splashShow("Setting up environment for remote hosts...");
    setupRuntimeEnvironmentsSSH();
    Debug.debug("Using workingDir "+workingDir, 2);
  }
  
  /**
   * Sets up a HashMap of hosts -> ShellMgrs.
   */
  protected void setupRemoteShellMgrs(){
    remoteShellMgrs = new HashMap();
    // The host defined by 'host' is *the* host,
    // i.e. used when scanning for RTEs, etc.
    // The hosts defined by 'hosts' are used for running jobs.
    hosts = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Hosts");
    users = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Users");
    passwords = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Passwords");
    maxJobs = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Max running jobs");
    String sshKeyFile = GridPilot.getClassMgr().getConfigFile().getValue(csName, "Ssh key file");
    String sshKeyPassword = GridPilot.getClassMgr().getConfigFile().getValue(csName, "Ssh key passphrase");
    for(int i=0; i<hosts.length; ++i){
      GridPilot.splashShow("Setting up shell on "+hosts[i]+"...");
      if(hosts[i]!=null &&
          !hosts[i].startsWith("localhost") && !hosts[i].equals("127.0.0.1")){
        try{
          remoteShellMgrs.put(hosts[i],
             new MySecureShell(hosts[i],
                 users!=null&&users.length>i&&users[i]!=null?users[i]:null/*null will cause prompting*/,
                 passwords!=null&&passwords.length>i&&passwords[i]!=null?passwords[i]:null/*null will cause prompting*/,
                     sshKeyFile==null?null:new File(MyUtil.clearTildeLocally(MyUtil.clearFile(sshKeyFile))),
                     sshKeyPassword));
        }
        catch(JSchException e){
          logFile.addMessage("WARNING: could not open shell on host "+hosts[i], e);
          e.printStackTrace();
        }
      }
      else if(hosts[i]!=null &&
          (hosts[i].startsWith("localhost") || hosts[i].equals("127.0.0.1"))){
        remoteShellMgrs.put(hosts[i], new LocalShell());
      }
      else{
        // host null not accepted...
      }
    }
    submittingHostJobs = new HashMap<String, HashSet<String>>();
    for(int i=0; i<hosts.length; ++i){
      submittingHostJobs.put(hosts[i], new HashSet());
    }
  }
  
  /**
   * Finds a Shell for the host/user/password of the job.
   * If the shellMgr is dead it is attempted to start a new one.
   * 
   * @param job
   * @return a Shell
   * @throws JSchException 
   */
  protected Shell getShellMgr(String host) throws JSchException{
    Shell mgr = null;
    if(host!=null &&
        !host.startsWith("localhost") && !host.equals("127.0.0.1")){
      MySecureShell sMgr = (MySecureShell) remoteShellMgrs.get(host);
      if(!sMgr.isConnected()){
        sMgr.reconnect();
      }
      mgr = sMgr;
    }
    else if(host!=null &&
        (host.startsWith("localhost") || host.equals("127.0.0.1"))){
      mgr = (Shell) remoteShellMgrs.get(host);
    }
    return mgr;
  }
  
  /**
   * The brokering algorithm. As simple as possible: FIFO.
   */
  protected synchronized String selectHost(JobInfo job){
    Shell mgr = null;
    String host = null;
    int maxR = 1;
    int submitting = 0;
    for(int i=0; i<hosts.length; ++i){
      host = hosts[i];
      maxR = 1;
      try{
        mgr = getShellMgr(host);
        if(maxJobs!=null && maxJobs.length>i && maxJobs[i]!=null){
          maxR = Integer.parseInt(maxJobs[i]);
        }
        submitting = (host!=null &&
            submittingHostJobs.get(host)!=null?((HashSet)submittingHostJobs.get(host)).size():0);
        if(mgr.getJobsNumber()+submitting<maxR){
          return host;
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    return null;
  }
  
  protected String runDir(MyJobInfo job){
    return MyUtil.clearFile(workingDir +"/"+job.getName());
  }

  public boolean submit(final JobInfo job){
    final String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
    final String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
    final String cmd = runDir(job)+"/"+job.getName()+commandSuffix;
    Debug.debug("Executing "+cmd, 2);
    ((MyJobInfo) job).setOutputs(stdoutFile, stderrFile);
    ForkScriptGenerator scriptGenerator =
      new ForkScriptGenerator(((MyJobInfo) job).getCSName(), runDir(job));
    try{
      Shell mgr = getShellMgr(job.getHost());
      scriptGenerator.createWrapper(mgr, (MyJobInfo) job, job.getName()+commandSuffix);
      String id = mgr.submit(cmd, runDir(job), stdoutFile, stderrFile, logFile);
      job.setJobId(id!=null?id:"");
      return true;
    }
    catch(Exception ioe){
      ioe.printStackTrace();
      error = "Exception during job " + job.getName() + " submission : \n" +
      "\tCommand\t: " + cmd +"\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
      return false;
    }
    finally{
      ((HashSet) submittingHostJobs.get(job.getHost())).remove(job);
    }
  }

  public void updateStatus(Vector jobs){
    for(int i=0; i<jobs.size(); ++i)
      try{
        updateStatus((MyJobInfo) jobs.get(i), getShellMgr(((MyJobInfo) jobs.get(i)).getHost()));
      }
      catch(JSchException e){
        error = "Exception during job " + ((MyJobInfo) jobs.get(i)).getName() + " submission : \n" +
        "\tException\t: " + e.getMessage();
        logFile.addMessage(error, e);
        e.printStackTrace();
      }
  }

  public boolean killJobs(Vector jobsToKill){
    Vector errors = new Vector();
    MyJobInfo job = null;
    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      try{
        job = (MyJobInfo) en.nextElement();
        getShellMgr(job.getHost()).killProcess(job.getJobId(), logFile);
      }
      catch(Exception e){
        errors.add(e.getMessage());
        logFile.addMessage("Exception during job killing :\n" +
                                    "\tJob#\t: " + job.getName() +"\n" +
                                    "\tException\t: " + e.getMessage(), e);
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
    String runDir = runDir(job);
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());

    // Delete files that may have been copied to final destination.
    // Files starting with file: are considered to locally available, accessed
    // with shellMgr
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getIdentifier());
    String fileName;
    Vector remoteFiles = new Vector();
    for(int i=0; i<outputFileNames.length; ++i){
      fileName = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFileNames[i]);
      if(fileName.startsWith("file:")){
        shell.deleteFile(fileName);
      }
      else{
        remoteFiles.add(fileName);
      }
    }
    String [] remoteFilesArr = new String [remoteFiles.size()];
    for(int i=0; i<remoteFilesArr.length; ++i){
      remoteFilesArr[i] = (String) remoteFiles.get(i);
    }
    try{
      transferControl.deleteFiles(remoteFilesArr);
    }
    catch(Exception e){
      error = "WARNING: could not delete output files. "+e.getMessage();
      Debug.debug(error, 3);
    }
    
    // Delete stdout/stderr that may have been copied to final destination
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      try{
        if(finalStdOut.startsWith("file:")){
          shell.deleteFile(finalStdOut);
        }
        else{
          transferControl.deleteFiles(new String [] {finalStdOut});
        }
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
        if(finalStdErr.startsWith("file:")){
          shell.deleteFile(finalStdErr);
        }
        else{
          transferControl.deleteFiles(new String [] {finalStdErr});
        }
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

    try{
      getShellMgr(job.getHost()).deleteDir(runDir);
    }
    catch(Exception ioe){
      error = "Exception during cleanup of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
      ret = false;
    }
    return ret;
  }

  public String getFullStatus(JobInfo job){
    Debug.debug("Checking job "+job.getHost()+":"+job.getJobId(), 2);
    try {
      if(getShellMgr(job.getHost()).isRunning(job.getJobId())){
        return "Job #"+job.getJobId()+" is running.";
      }
      else{
        return "Job #"+job.getJobId()+" is not running.";
      }
    }
    catch(JSchException e){
      error = "Exception during getFullStatus of job " + job.getName()+ "\n" +
      "\tException\t: " + e.getMessage();
      e.printStackTrace();
      return null;
    }
  }

  public String[] getCurrentOutput(JobInfo job) {
    try{
      String stdOutText = getShellMgr(job.getHost()).readFile(job.getOutTmp());
      String stdErrText = "";
      if(getShellMgr(job.getHost()).existsFile(job.getErrTmp())){
        stdErrText = getShellMgr(job.getHost()).readFile(job.getErrTmp());
      }
      return new String [] {stdOutText, stdErrText};
    }
    catch(Exception ioe){
      error = "IOException during getFullStatus of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
      return null;
    }
  }

  public String[] getScripts(JobInfo job) {
    String jobScriptFile = runDir(job)+"/"+job.getName()+commandSuffix;
    // In case this is not a local shell, first get the script to a local tmp file.
    try {
      if(!getShellMgr(job.getHost()).isLocal()){
        File tmpFile = File.createTempFile(/*prefix*/"GridPilot-Fork-", /*suffix*/"");
        // have the file deleted on exit
        GridPilot.addTmpFile(tmpFile.getAbsolutePath(), tmpFile);
        getShellMgr(job.getHost()).download(jobScriptFile, tmpFile.getAbsolutePath());
        jobScriptFile = tmpFile.getAbsolutePath();
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return new String [] {jobScriptFile};
  }

  public boolean postProcess(JobInfo job) {
    Debug.debug("Post processing job " + job.getName(), 2);
    String runDir = runDir(job);
    try{
      if(copyToFinalDest((MyJobInfo) job, getShellMgr(job.getHost()))){
        return getShellMgr(job.getHost()).deleteDir(runDir);
      }
      else{
        return false;
      }
    }
    catch(Exception e){
      e.printStackTrace();
      error = "Exception during postProcess of job " + job.getName();
      logFile.addMessage(error);
      return false;
    }
  }

  public boolean preProcess(JobInfo job) throws Exception{
    // choose the host
    String host = selectHost(job);
    if(host==null){
      logFile.addInfo("No free slots on any hosts.");
      return false;
    }
    ((HashSet) submittingHostJobs.get(host)).add(job);
    
    Debug.debug("Getting ShellMgr for host "+host, 2);
    Shell mgr = getShellMgr(host);
    job.setHost(host);
    job.setUserInfo(mgr.getUserName());
    
    // create the run directory
    try{
      if(!getShellMgr(job.getHost()).existsFile(runDir(job))){
        getShellMgr(job.getHost()).mkdirs(runDir(job));
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not create run directory for job.", e);
      return false;
    }
    if(!getShellMgr(job.getHost()).isLocal()){
      try{
        writeUserProxy(getShellMgr(job.getHost()));
      }
      catch(Exception e){
        logFile.addMessage("WARNING: could not write user proxy.", e);
      }
    }
    return setupJobRTEs((MyJobInfo) job, getShellMgr(job.getHost())) &&
       setRemoteOutputFiles((MyJobInfo) job) && getInputFiles((MyJobInfo) job, getShellMgr(job.getHost()));
  }
  
  /**
   * The same as setupRuntimeEnvironments, but iterated over all remote ShellMgrs.
   */
  protected void setupRuntimeEnvironmentsSSH(){
    for(Iterator it=remoteShellMgrs.values().iterator(); it.hasNext();){
      setupRuntimeEnvironmentsSSH((Shell) it.next());
    }
    // Already done by ForkComputingSystem constructor
    //MyUtil.syncRTEsFromCatalogs(csName, rteCatalogUrls, localRuntimeDBs, toDeleteRTEs);
  }
  
  protected void setupRuntimeEnvironmentsSSH(Shell shellMgr){
    for(int i=0; i<runtimeDBs.length; ++i){
      DBPluginMgr dbMgr = null;
      try{
        dbMgr = GridPilot.getClassMgr().getDBPluginMgr(
            runtimeDBs[i]);
      }
      catch(Exception e){
        Debug.debug("WARNING: Could not load runtime DB "+
            runtimeDBs[i]+". Runtime environments must be defined by hand. "+
            e.getMessage(), 1);
        continue;
      }
      try{
        scanRTEDir(dbMgr, csName, shellMgr);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
  }
  
  public void exit() {
    // Inform that running jobs will not be bookkept and ask for confirmation.
    Shell shellMgr = null;
    boolean anyRunning = false;
    String host = null;
    int jobs = 0;
    String message = "WARNING: You have ";
    boolean oneIterationDone = false;
    for(Iterator it=remoteShellMgrs.values().iterator(); it.hasNext();){
      try{
        shellMgr = (Shell) it.next();
        host = shellMgr.getHostName();
        jobs = shellMgr.getJobsNumber();
        if(oneIterationDone){
          message += ", \n";
        }
        if(jobs>0){
          message += jobs+" jobs running on "+host;
        }
        oneIterationDone = true;
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(anyRunning){
      message += ". The stdout/stderr of these jobs will be lost\n" +
          "and their output files will not be catalogued.";
      try{
        MyUtil.showError(message);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    super.exit();
  }

  public Shell getShell(JobInfo job){
    return (Shell) remoteShellMgrs.get(job.getHost());
  }

}