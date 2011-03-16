package gridpilot.csplugins.forkpool;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
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
  protected String [] maxRunningJobs = null;
  protected String [] maxPreprocessingJobs = null;
  protected HashMap<String, HashSet<JobInfo>> preprocessingHostJobs = new HashMap<String, HashSet<JobInfo>>();
  protected String [] users = null;
  protected String [] passwords = null;
  protected String [] requiredRuntimeEnvs = null;
  private static HashMap<String, String> remoteCopyCommands = null;
  
  public ForkPoolComputingSystem(String _csName) throws Exception{
    super(_csName);
    mkLocalOSRTE = false;
    includeVMRTEs = false;
    basicOSRTES = new String [] {"Linux"};
    setupRemoteShellMgrs();
    GridPilot.splashShow("Setting up runtime environments for "+csName);
    setupRuntimeEnvironmentsSSH();
    Debug.debug("Using workingDir "+workingDir, 2);
    String [] rtCpCmds = GridPilot.getClassMgr().getConfigFile().getValues(
        csName, "Remote copy commands");
    if(rtCpCmds!=null && rtCpCmds.length>1){
      remoteCopyCommands = new HashMap<String, String>();
      for(int i=0; i<rtCpCmds.length/2; ++i){
        remoteCopyCommands.put(rtCpCmds[2*i], rtCpCmds[2*i+1]);
      }
    }
    requiredRuntimeEnvs = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Required runtime environments");
  }
  
  /**
   * Sets up a HashMap of hosts -> ShellMgrs.
   */
  protected void setupRemoteShellMgrs(){
    remoteShellMgrs = new HashMap<String, Shell>();
    // The host defined by 'host' is *the* host,
    // i.e. used when scanning for RTEs, etc.
    // The hosts defined by 'hosts' are used for running jobs.
    hosts = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Hosts");
    users = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Users");
    passwords = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Passwords");
    maxRunningJobs = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Max running jobs per host");
    maxPreprocessingJobs = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Max preprocessing jobs per host");
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
    for(int i=0; i<hosts.length; ++i){
      preprocessingHostJobs.put(hosts[i], new HashSet<JobInfo>());
    }
  }
  
  protected String getCommandSuffix(MyJobInfo job){
    String commandSuffix = ".sh";
    String host = job.getHost();
    if(host!=null){
      try{
        Shell thisShell = getShell(job);
        if(thisShell.getOSName().toLowerCase().startsWith("windows")){
          commandSuffix = ".bat";
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    return commandSuffix;
  }
  
  /**
   * Finds a Shell for the host/user/password of the job.
   * If the shellMgr is dead it is attempted to start a new one.
   * 
   * @param job
   * @return a Shell
   * @throws JSchException 
   */
  protected Shell getShell(String host) throws JSchException{
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
  
  protected String runDir(MyJobInfo job){
    return MyUtil.clearFile(workingDir +"/"+job.getName());
  }

  /**
   * Select a host for a given job. If none is found, null is returned.
   * A host that is already running a job may be selected - if the setting
   * of "max preprocessing jobs" permits it.
   * The brokering algorithm is as simple as possible: FIFO.
   */
  protected synchronized String selectHost(JobInfo job){
    String host = null;
    int maxP = 1;
    int preprocessing = 0;
    //Shell shell = null;
    //int maxR = 1;
    for(int i=0; i<hosts.length; ++i){
      host = hosts[i];
      try{
        if(maxPreprocessingJobs!=null && maxPreprocessingJobs.length>i && maxPreprocessingJobs[i]!=null){
          maxP = Integer.parseInt(maxPreprocessingJobs[i]);
        }
        preprocessing = (host!=null &&
            preprocessingHostJobs.get(host)!=null?preprocessingHostJobs.get(host).size():0);
        if(preprocessing>=maxP){
          continue;
        }
        Shell thisShell = getShell(host);
        int maxR = 1;
        if(maxRunningJobs!=null && maxRunningJobs.length>i && maxRunningJobs[i]!=null){
          maxR = Integer.parseInt(maxRunningJobs[i]);
        }
        if(thisShell!=null && thisShell.getJobsNumber()<maxR){
          Debug.debug("Selecting host "+host+" for job "+job.getName()+" : "+preprocessing+"<"+maxP, 2);
          return host;
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    return null;
  }
  
  /**
   * Check if we can run a job on a given host - by consulting "max running jobs".
   * The job is assumed to have had its host set to the one returned by selectHost.
   * @throws Exception 
   */
  protected boolean checkHost(JobInfo job) throws Exception{
    Shell mgr = null;
    String host = null;
    int maxR = 1;
    for(int i=0; i<hosts.length; ++i){
      host = hosts[i];
      if(!job.getHost().equals(host)){
        continue;
      }
      mgr = getShell(job);
      if(maxRunningJobs!=null && maxRunningJobs.length>i && maxRunningJobs[i]!=null){
        maxR = Integer.parseInt(maxRunningJobs[i]);
      }
      if(mgr.getJobsNumber()<maxR){
        return true;
      }
    }
    return false;
  }
  
  public int run(final JobInfo job){
    try{
      if(checkHost(job)){
        if(submit(job)){
          return MyComputingSystem.RUN_OK;
        }
        else{
          return MyComputingSystem.RUN_FAILED;
        }
      }
      else{
        return MyComputingSystem.RUN_WAIT;
      }
    }
    catch(Exception e){
      e.printStackTrace();
      return MyComputingSystem.RUN_FAILED;
    }
  }
  
  public boolean submit(final JobInfo job){
    final String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
    final String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
    final String cmd = runDir(job)+"/"+job.getName()+getCommandSuffix((MyJobInfo) job);
    Debug.debug("Executing "+cmd, 2);
    ((MyJobInfo) job).setOutputs(stdoutFile, stderrFile);
    ForkScriptGenerator scriptGenerator =
      new ForkScriptGenerator(((MyJobInfo) job).getCSName(), runDir(job), ignoreBaseSystemAndVMRTEs, false);
    try{
      Shell mgr = getShell(job);
      String scriptFile = job.getName()+getCommandSuffix((MyJobInfo) job);
      scriptGenerator.createWrapper(mgr, (MyJobInfo) job, scriptFile);
      StringBuffer stdout = new StringBuffer();
      StringBuffer stderr = new StringBuffer();
      shell.exec("chmod +x "+runDir(job) +"/"+scriptFile, stdout, stderr);
      String id = mgr.submit(cmd, submitEnvironment, runDir(job), stdoutFile, stderrFile, logFile);
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
      Debug.debug("Removing "+job.getName()+" from preprocessingHostJobs", 2);
      HashSet<JobInfo> jobs = preprocessingHostJobs.get(job.getHost());
      jobs.remove(job);
      preprocessingHostJobs.put(job.getHost(), jobs);
    }
  }

  public void updateStatus(Vector<JobInfo> jobs){
    for(int i=0; i<jobs.size(); ++i)
      try{
        updateStatus((MyJobInfo) jobs.get(i), getShell((MyJobInfo) jobs.get(i)));
      }
      catch(Exception e){
        error = "Exception during job " + ((MyJobInfo) jobs.get(i)).getName() + " update : \n" +
        "\tException\t: " + e.getMessage();
        logFile.addMessage(error, e);
        e.printStackTrace();
      }
  }

  public boolean killJobs(Set<JobInfo> jobsToKill){
    Vector<String> errors = new Vector<String>();
    MyJobInfo job = null;
    for(Iterator<JobInfo>it=jobsToKill.iterator(); it.hasNext();){
      try{
        job = (MyJobInfo) it.next();
        getShell(job).killProcess(job.getJobId(), logFile);
      }
      catch(Exception e){
        errors.add(e.getMessage());
        logFile.addMessage("Exception during job killing :\n" +
                                    "\tJob#\t: " + job.getName() +"\n" +
                                    "\tException\t: " + e.getMessage(), e);
      }
      try{
        HashSet<JobInfo> jobs = preprocessingHostJobs.get(job.getHost());
        if(jobs!=null && jobs.contains(job)){
          jobs.remove(job);
          preprocessingHostJobs.put(job.getHost(), jobs);
        }
      }
      catch(Exception e){
        e.printStackTrace();
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
    try{
      getShell(job).deleteDir(runDir);
    }
    catch(Exception ioe){
      error = "Exception during cleanup of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
      ret = false;
    }
    try{
      super.cleanup(job);
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
    Shell shell = getShell(job);
    if(shell!=null && shell.isRunning(job.getJobId())){
      return "Job #"+job.getJobId()+" is running.";
    }
    else{
      return "Job #"+job.getJobId()+" is not running.";
    }
  }

  public String[] getCurrentOutput(JobInfo job) {
    try{
      String stdOutText = getShell(job).readFile(job.getOutTmp());
      String stdErrText = "";
      if(getShell(job).existsFile(job.getErrTmp())){
        stdErrText = getShell(job).readFile(job.getErrTmp());
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
    String jobScriptFile = runDir(job)+"/"+job.getName()+getCommandSuffix((MyJobInfo) job);
    // In case this is not a local shell, first get the script to a local tmp file.
    try {
      if(!getShell(job).isLocal()){
        File tmpFile = File.createTempFile(/*prefix*/"GridPilot-Fork-", /*suffix*/"");
        // have the file deleted on exit
        GridPilot.addTmpFile(tmpFile.getAbsolutePath(), tmpFile);
        getShell(job).download(jobScriptFile, tmpFile.getAbsolutePath());
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
      if(copyToFinalDest((MyJobInfo) job, getShell(job))){
        if(!getShell(job).deleteDir(runDir)){
          logFile.addMessage("WARNING: could not delete run directory "+runDir+" for job "+job.getIdentifier());
        }
        return true;
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
    boolean ret = true;
    Exception retE = null;
    try{
      // choose the host
      String host = selectHost(job);
      if(host==null){
        ret = false;
        Debug.debug("No free slot on any host.", 2);
        return false;
      }
      Debug.debug("Preprocessing job "+job.getName(), 2);
      (preprocessingHostJobs.get(host)).add(job);
      Debug.debug("Getting ShellMgr for host "+host, 2);
      job.setHost(host);
      Shell mgr = getShell(job);
      job.setUserInfo(mgr.getUserName());
      // create the run directory
      if(!getShell(job).existsFile(runDir(job))){
        getShell(job).mkdirs(runDir(job));
      }
      LinkedHashSet<String> deps = new LinkedHashSet<String>();
      Collections.addAll(deps, requiredRuntimeEnvs);
      Collections.addAll(deps, job.getRTEs());
      job.setRTEs(deps.toArray(new String[deps.size()]));
      boolean rtesOK = setupJobRTEs((MyJobInfo) job, getShell(job));
      boolean outputFilesOK = MyUtil.setRemoteOutputFiles((MyJobInfo) job, remoteCopyCommands);
      boolean inputFilesOK = getInputFiles((MyJobInfo) job, getShell(job));
      ret = rtesOK && outputFilesOK && inputFilesOK;
      if(!rtesOK){
        logFile.addMessage("WARNING: could not setup RTEs for job "+job.getName());
      }
      if(!outputFilesOK){
        logFile.addMessage("WARNING: could not set output files for job "+job.getName());
      }
      if(!inputFilesOK){
        logFile.addMessage("WARNING: could not stage input files for job "+job.getName());
      }
      // With this, jobs can queue up indefinitely on a single host
      /*try{
        ((HashSet) preprocessingHostJobs.get(job.getHost())).remove(job);
      }
      catch(Exception ee){
      }*/
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not prepare job.", e);
      //(preprocessingHostJobs.get(job.getHost())).remove(job);
      HashSet<JobInfo> jobs = preprocessingHostJobs.get(job.getHost());
      jobs.remove(job);
      preprocessingHostJobs.put(job.getHost(), jobs);
      retE = e;
    }
    if(retE!=null){
      throw retE;
    }
    return ret;
  }
  
  /**
   * The same as setupRuntimeEnvironments, but iterated over all remote ShellMgrs.
   */
  protected void setupRuntimeEnvironmentsSSH(){
    for(Iterator<Shell> it=remoteShellMgrs.values().iterator(); it.hasNext();){
      setupRuntimeEnvironmentsSSH(it.next());
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
    for(Iterator<Shell> it=remoteShellMgrs.values().iterator(); it.hasNext();){
      try{
        shellMgr = it.next();
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

  public Shell getShell(JobInfo job) {
    Debug.debug("Getting shell for job "+job.getIdentifier()+" on host "+job.getHost(), 2);
    Shell shell;
    try{
      shell = getShell(job.getHost());
    }
    catch(Exception e){
      error = "Exception during getFullStatus of job " + job.getName()+ "\n" +
      "\tException\t: " + e.getMessage();
      e.printStackTrace();
      return null;
    }
    Debug.debug("Returning "+shell, 2);
    return shell;
  }

}