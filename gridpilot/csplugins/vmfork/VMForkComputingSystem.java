package gridpilot.csplugins.vmfork;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.PullMgr;
import gridfactory.common.Shell;
import gridfactory.common.Util;
import gridfactory.common.jobrun.VMMgr;
import gridfactory.common.jobrun.VirtualMachine;
import gridfactory.common.jobrun.RTECatalog.MetaPackage;

import gridpilot.DBPluginMgr;
import gridpilot.GridPilot;
import gridpilot.MyComputingSystem;
import gridpilot.MyJobInfo;
import gridpilot.MyUtil;
import gridpilot.RteRdfParser;
import gridpilot.csplugins.fork.ForkScriptGenerator;

/**
 * This class provides the means to test the virtual machines and runtime environments of the
 * software catalogs ("runtime catalog URLs" in the config file).
 * It extends ForkComputingSystem from GridFactory in order to match as much as possible what
 * happens on a GridFactory worker node.
 * Some methods are copied from ForkComputingSystem and ForkPoolComputingSystem from GridPilot
 * in order to match what GridPilot expects a MyComputingSystem implementation to do.
 */
public class VMForkComputingSystem extends gridfactory.common.jobrun.ForkComputingSystem implements MyComputingSystem {

  private String csName;
  private String [] rteCatalogUrls;
  private String user;
  private String[] localRuntimeDBs;
  private HashMap<String, String> toDeleteRtes = new HashMap<String, String>();
  // Only here to use checkRequirements
  private PullMgr pullMgr;

  public VMForkComputingSystem(String _csName) throws Exception {
    csName = _csName;
    virtEnforce = true;
    // Just some reasonable number
    minVmMB = 256;
    // Just some reasonable number
    defaultJobMB = 256;
    GridPilot.splashShow("Setting up VMFork");
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    // Just some reasonable number (seconds). Notice that the first boot of a VM typically
    // will take a long time, because the image will need to be downloaded and duplicated.
    int bootTimeout = 700;
    String bt = configFile.getValue(csName, "boot timeout");
    if(bt!=null && !bt.equals("")){
      bootTimeout = Integer.parseInt(bt);
    }
    defaultVmMB = minVmMB+defaultJobMB;
    shells = new HashMap<String, Shell>();
    /*
     * We set the max number of jobs allowed by super.reuseHost() to be the allowed
     * number of running jobs of the config file. The number of preprocessing and running jobs
     * will be attempted controlled by SubmissionControl, but enforced by super.reuseHost().
     */
    int maxRunningJobsPerHost = 1;
    String maxRunningJobsPerHostStr = GridPilot.getClassMgr().getConfigFile().getValue(csName, "Max running jobs per host");
    if(maxRunningJobsPerHostStr!=null && !maxRunningJobsPerHostStr.trim().equals("")){
      maxRunningJobsPerHost = Integer.parseInt(maxRunningJobsPerHostStr);
    }
    maxMachines = MyUtil.getTotalMaxSimultaneousRunningJobs(csName) / maxRunningJobsPerHost;
    // Fill maxRunningJobs with a constant number
    maxRunningJobs = new int[maxMachines];
    Arrays.fill(maxRunningJobs, maxRunningJobsPerHost);
    Debug.debug("Jobs per machine: "+MyUtil.arrayToString(maxRunningJobs), 2);
    // Fill hosts with nulls and assign values as jobs are submitted.
    hosts = new String [maxMachines];
    localRteDir = GridPilot.RUNTIME_DIR;
    remoteRteDir = localRteDir;
    workingDir = configFile.getValue(csName, "working directory");
    logFile = GridPilot.getClassMgr().getLogFile();
    rteCatalogUrls = configFile.getValues(GridPilot.TOP_CONFIG_SECTION, "runtime catalog URLs");
    transferControl = GridPilot.getClassMgr().getTransferControl();
    rteMgr = GridPilot.getClassMgr().getRTEMgr(localRteDir, rteCatalogUrls);
    rteMgr.fixLocalCatalog(GridPilot.class);
    transferStatusUpdateControl = GridPilot.getClassMgr().getTransferStatusUpdateControl();
    termVmOnJobEnd = false;
    String tmp = configFile.getValue(csName, "enforce virtualization");
    if(tmp!=null){
      try{
        virtEnforce = tmp.equalsIgnoreCase("yes") || tmp.equalsIgnoreCase("true");
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"enforce virtualization\" is not set in configuration file", nfe);
      }
    }
    tmp = configFile.getValue(csName, "default ram per vm");
    if(tmp!=null){
      try{
        defaultVmMB = Integer.parseInt(tmp);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"default ram per vm\" is not"+
                                    " an integer in configuration file", nfe);
      }
    }
    try{
      user = GridPilot.getClassMgr().getSSL().getGridSubject();
    }
    catch(Exception e){
      user = System.getProperty("user.name").trim();
    }
    vmMgr = new VMMgr(rteMgr, transferStatusUpdateControl, /*Total memory assigned to VMs*/defaultVmMB,
        bootTimeout, localRteDir, logFile);
    
    localRuntimeDBs = configFile.getValues(csName, "runtime databases");
    
    createMonitor();
    
    pullMgr = new MyPullMgr();

  }
  
  private void createMonitor() throws InterruptedException, InvocationTargetException {
    javax.swing.SwingUtilities.invokeAndWait(new Runnable() {
      public void run(){
        try{
          Debug.debug("Adding Local VM monitor", 2);
          VMForkMonitoringPanel panel = new VMForkMonitoringPanel(vmMgr, rteCatalogUrls);
          // This causes the panel to be added to the monitoring window as a tab,
          // right after the transfer monitoring tab and before the log tab.
          GridPilot.EXTRA_MONITOR_TABS.add(panel);
        }
        catch(Exception e){
          e.printStackTrace();
          logFile.addMessage("WARNING: could not create VM monitoring panel.", e);
        }
      }
    });
  }

  protected void setupJobRTEs(JobInfo job, Shell shell) throws Exception{
    MyUtil.setupJobRTEs(job, shell, rteMgr, transferStatusUpdateControl, remoteRteDir, localRteDir);
  }
  
  protected void updateStatus(JobInfo job, Shell shell){
    super.updateStatus(job, shell);
    // TODO: eliminate MyJobInfo
    ((MyJobInfo) job).setCSStatus(JobInfo.getStatusName(job.getStatus()));
  }
  
  public void cleanupRuntimeEnvironments(String csName) {
    MyUtil.cleanupRuntimeEnvironments(csName, localRuntimeDBs, toDeleteRtes);
  }

  public String getUserInfo(String csName) {
    return user;
  }
  
  public boolean preProcess(JobInfo job) throws Exception {
    
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String [] rtes = dbPluginMgr.getRuntimeEnvironments(job.getIdentifier());
    Debug.debug("Job depends on "+MyUtil.arrayToString(rtes), 2);
    String transID = dbPluginMgr.getJobDefExecutableID(job.getIdentifier());
    String [] transInputFiles = dbPluginMgr.getExecutableInputs(transID);
    String [] jobInputFiles = dbPluginMgr.getJobDefInputFiles(job.getIdentifier());
    String [] inputFiles = new String [transInputFiles.length+jobInputFiles.length];
    for(int i=0; i<transInputFiles.length; ++i){
      inputFiles[i] = transInputFiles[i];
    }
    for(int i=0; i<jobInputFiles.length; ++i){
      inputFiles[transInputFiles.length+i] = jobInputFiles[i];
    }
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getIdentifier());
    String [] outputDestinations = new String [outputFileNames.length];
    for(int i=0; i<outputDestinations.length; ++i){
      outputDestinations[i] = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFileNames[i]);
    }
    job.setInputFileUrls(inputFiles);
    job.setOutputFileDestinations(outputDestinations);
    job.setOutputFileNames(outputFileNames);
    Debug.debug("job "+job.getIdentifier()+" has output files "+
        MyUtil.arrayToString(job.getOutputFileNames())+" --> "+
        MyUtil.arrayToString(job.getOutputFileDestinations()), 2);
    setBaseSystemName(rtes, job);
    if(job.getOpSysRTE()!=null){
      String [] reducedRTEList = MyUtil.removeBaseSystemAndVM(rtes, rteMgr.getProvides(job.getOpSysRTE()));
      Debug.debug("Job dependencies --> "+MyUtil.arrayToString(reducedRTEList), 2);
      job.setRTEs(reducedRTEList);
      Debug.debug("Job dependencies --> "+MyUtil.arrayToString(job.getRTEs()), 2);
    }
    else{
      job.setRTEs(rtes);
    }
    job.setMemory(defaultJobMB);
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());
    job.setStdoutDest(finalStdOut);
    job.setStderrDest(finalStdErr);
    
    if(!pullMgr.checkRequirements(job, virtEnforce)){
      throw new Exception("Requirement(s) could not be satisfied. "+job);
    }
    
    // Check if any VMs have been launched from VMForkMonitoringPanel
    includeManuallyBootedVMs();
    
    boolean ok = gridpilot.csplugins.fork.ForkComputingSystem.setRemoteOutputFiles((MyJobInfo) job);
    if(!super.preProcess(job) /* This is what boots up a VM. Notice that super refers to
                                         gridfactory.common.jobrun.ForkComputingSystem. */){
      // There may still be hosts booting or unbooted - just return false - SubmissionControl
      // will check and fail job if necessary.
      Debug.debug("super.preProcess() failed, returning false", 2);
      return false;
    }

    String commandSuffix = ".sh";
    String scriptFile = job.getName()+".gp"+commandSuffix;
    String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
    String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
    ForkScriptGenerator scriptGenerator = new ForkScriptGenerator(((MyJobInfo) job).getCSName(), runDir(job),
        true, false);
    Shell shell = getShell(job);
    if(!scriptGenerator.createWrapper(shell, (MyJobInfo) job, scriptFile)){
      throw new IOException("Could not create wrapper script.");
    }
    setupExecutable(runDir(job) +"/"+scriptFile, shell);
    ((MyJobInfo) job).setOutputs(stdoutFile, stderrFile);
    job.setExecutable(scriptFile);

    return ok;
    
  }
  
  public boolean postProcess(JobInfo job) {
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getIdentifier());
    String [] outputDestinations = new String [outputFileNames.length];
    for(int i=0; i<outputDestinations.length; ++i){
      outputDestinations[i] = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFileNames[i]);
    }
    job.setOutputFileDestinations(outputDestinations);
    job.setOutputFileNames(outputFileNames);
    Debug.debug("job "+job.getIdentifier()+" has output files "+
        MyUtil.arrayToString(job.getOutputFileNames())+" --> "+
        MyUtil.arrayToString(job.getOutputFileDestinations()), 2);
    return transferControl.copyToFinalDest(job, getShell(job), runDir(job));
  }


  
  /**
   * If a VM RTE provides all of the requested RTEs, set this as the
   * opsys and opSysRTE.
   * @param rtes requested RTEs
   * @param job the job in question
   */
  private void setBaseSystemName(String [] rtes, JobInfo job){
    Set<MetaPackage> mps = rteMgr.getRTECatalog().getMetaPackages();
    MetaPackage mp;
    String rte;
    String vmRte = null;
    int providesHits = 0;
    int maxProvidesHits = 0;
    String [] provides;
    int i = 0;
    for(Iterator<MetaPackage> it=mps.iterator(); it.hasNext();){
      mp = it.next();
      rte = mp.name;
      ++i;
      // TODO: consider using RTEMgr.isVM() instead of relying on people starting their
      //       VM RTE names with VM/
      // Prefer VM that provides as many as the requested rtes as possible,
      // - first try just with no basesystem
      if(rte.startsWith(RteRdfParser.VM_PREFIX)){
        providesHits = 0;
        try{
          if(Util.arrayContains(rtes, rte)){
            ++providesHits;
          }
          provides = rteMgr.getProvides(rte);
          Debug.debug("Checking RTE "+rte+" --> provides: "+MyUtil.arrayToString(provides), 2);
          Debug.debug("Against: "+MyUtil.arrayToString(rtes), 2);
          for(int j=0; j<provides.length; ++j){
            if(Util.arrayContains(rtes, provides[j])){
              Debug.debug("OK: "+provides[j]+" provided", 2);
              ++providesHits;
            }
          }
          if(providesHits>maxProvidesHits){
            maxProvidesHits = providesHits;
            vmRte = rte;
          }
        }
        catch(Exception e){
          logFile.addMessage("getProvides failed for "+rtes[i], e);
        }
      }
    }
    if(vmRte!=null && maxProvidesHits==rtes.length){
      Debug.debug("Setting OS of job "+job.getIdentifier()+" to "+vmRte+" : "+providesHits, 2);
      job.setOpSys(vmRte);
      job.setOpSysRTE(vmRte);
    }
  }

  private void includeManuallyBootedVMs() {
    HashMap<String, VirtualMachine> vms = vmMgr.getVMs();
    VirtualMachine vm;
    String vmHost;
    for(int i=0; i<hosts.length; ++i){
      if(hosts[i]==null || hosts[i].equals("")){
        for(Iterator<String> it=vms.keySet().iterator(); it.hasNext();){
          try{
            vm = vms.get(it.next());
            vmHost = vm.getHostName()+":"+vm.getSshPort();
            if(!MyUtil.arrayContains(hosts, vmHost)){
              hosts[i] = vmHost;
            }
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    }
  }
  
  public void setupRuntimeEnvironments(String csName) {
    if(localRuntimeDBs==null || localRuntimeDBs.length==0){
      return;
    }
    MyUtil.syncRTEsFromCatalogs(csName, rteCatalogUrls, localRuntimeDBs, toDeleteRtes, !virtEnforce,
        true, new String [] {"Linux", "Windows"}, true);
  }
  
  /**
   * Check if there is a free slot for running a job on the host 'host'.
   * @param host the host in question
   * @return true if the job can be run, false otherwise
   */
  private boolean checkMaxRunning(MyJobInfo job) {
    Shell shell = shells.get(job.getHost());
    if(shell.equals(null)){
      return false;
    }    String hostAndPort;
    int sJobs;
    int rJobs;
    for(int i=0; i<hosts.length; ++i){
      if(job.getHost().equals(hosts[i])){
        sJobs = getSubmitting(job);
        rJobs = shell.getJobsNumber();
        Debug.debug("Checking if we can run more jobs on "+hosts[i]+"-->"+
            sJobs+"+"+rJobs+"<"+maxRunningJobs[i], 2);
        return sJobs+rJobs<maxRunningJobs[i];
      }
    }
    return true;
  }
  
  private int getSubmitting(MyJobInfo _job){
    int ret = 0;
    MyJobInfo job;
    Vector<MyJobInfo> jobs = GridPilot.getClassMgr().getSubmissionControl().getSubmittingJobs();
    for(Iterator<MyJobInfo> it=jobs.iterator(); it.hasNext();){
      job = it.next();
      if(!job.getIdentifier().equals(_job.getIdentifier()) && _job.getHost().equals(job.getHost())){
        Debug.debug("getSubmitting-->"+job.getName(), 3);
        ++ret;
      }
    }
    return ret;
  }

  public int run(final MyJobInfo job){
    try{
      if(!checkMaxRunning(job)){
        return MyComputingSystem.RUN_WAIT;
      }

      if(submit(job)){
        return MyComputingSystem.RUN_OK;
      }
      else{
        return MyComputingSystem.RUN_FAILED;
      }
    }
    catch(Exception e){
      e.printStackTrace();
      return MyComputingSystem.RUN_FAILED;
    }
  }

}
