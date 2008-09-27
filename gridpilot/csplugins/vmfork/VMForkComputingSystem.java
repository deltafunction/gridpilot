package gridpilot.csplugins.vmfork;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.Shell;
import gridfactory.common.jobrun.ForkComputingSystem;
import gridfactory.common.jobrun.RTEMgr;
import gridfactory.common.jobrun.VMMgr;
import gridpilot.DBPluginMgr;
import gridpilot.GridPilot;
import gridpilot.MyComputingSystem;
import gridpilot.MyJobInfo;
import gridpilot.MyUtil;
import gridpilot.csplugins.fork.ForkScriptGenerator;

/**
 * This class provides the means to test the virtual machines and runtime environments of the
 * software catalogs ("runtime catalog URLs" in the config file).
 * It extends ForkComputingSystem from GridFactory in order to match as much as possible what
 * happens on a GridFactory worker node.
 * Some methods are copied from ForkComputingSystem and ForkPoolComputingSystem from GridPilot
 * in order to match what GridPilot expects a MyComputingSystem implementation to do.
 */
public class VMForkComputingSystem extends ForkComputingSystem implements MyComputingSystem {

  private String csName;
  private String [] rteCatalogUrls;
  private String user;
  private String[] localRuntimeDBs;
  private HashMap toDeleteRtes = new HashMap();

  public VMForkComputingSystem(String _csName) throws Exception {
    csName = _csName;
    virtEnforce = true;
    // Just some reasonable number
    minVmMB = 256;
    // Just some reasonable number
    defaultJobMB = 256;
    // Just some reasonable number (seconds)
    int bootTimeout = 120;
    defaultVmMB = minVmMB+defaultJobMB;
    // No need to run more than one VM - this CS is just for testing a single job before running it on GridFactory.
    maxMachines = 1;
    GridPilot.splashShow("Setting up VMFork");
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    shells = new HashMap<String, Shell>();
    // Fill hosts with nulls and assign values as jobs are submitted.
    hosts = new String [maxMachines];
    // Fill maxJobs with a constant number
    maxJobs = new int[maxMachines];
    Arrays.fill(maxJobs, jobsPerMachine);
    localRteDir = GridPilot.runtimeDir;
    remoteRteDir = localRteDir;
    workingDir = configFile.getValue(csName, "working directory");
    logFile = GridPilot.getClassMgr().getLogFile();
    rteCatalogUrls = configFile.getValues(GridPilot.topConfigSection, "runtime catalog URLs");
    transferControl = GridPilot.getClassMgr().getTransferControl();
    rteMgr = GridPilot.getClassMgr().getRTEMgr(localRteDir, rteCatalogUrls);
    rteMgr.fixLocalCatalog(GridPilot.class);
    transferStatusUpdateControl = GridPilot.getClassMgr().getTransferStatusUpdateControl();
    termVmOnJobEnd = false;
    jobsPerMachine = 1;
    // We just set the max number of jobs to run inside the VM to the max number of jobs run by this CS.
    String tmp = configFile.getValue(csName, "maximum simultaneous running");
    if(tmp!=null){
      try{
        jobsPerMachine = Integer.parseInt(tmp);
      }
      catch(NumberFormatException nfe){
        logFile.addMessage("Value of \"maximum simultaneous running\" is not"+
                                    " an integer in configuration file", nfe);
      }
    }
    tmp = configFile.getValue(csName, "enforce virtualization");
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

    Debug.debug("Adding Local VM monitor", 2);
    VMForkMonitoringPanel panel = new VMForkMonitoringPanel(vmMgr, rteCatalogUrls);
    // This causes the panel to be added to the monitoring window as a tab,
    // right after the transfer monitoring tab and before the log tab.
    GridPilot.extraMonitorTabs.add(panel);
        
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
    
    // From gridpilot.csplugins.fork.ForkScriptComputingSystem
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String [] rtes = dbPluginMgr.getRuntimeEnvironments(job.getIdentifier());
    String transID = dbPluginMgr.getJobDefTransformationID(job.getIdentifier());
    String [] transInputFiles = dbPluginMgr.getTransformationInputs(transID);
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
    job.setRTEs(MyUtil.removeMyOS(rtes));
    
    boolean ok = gridpilot.csplugins.fork.ForkComputingSystem.setRemoteOutputFiles((MyJobInfo) job);
    ok = ok && super.preProcess(job);
    String commandSuffix = ".sh";
    Shell shell = getShell(job);
    if(shell.isLocal() && MyUtil.onWindows()){
      commandSuffix = ".bat";
    }
    String scriptFile = job.getName()+".gp"+commandSuffix;
    String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
    String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
    ForkScriptGenerator scriptGenerator = new ForkScriptGenerator(((MyJobInfo) job).getCSName(), runDir(job));
    if(!scriptGenerator.createWrapper(shell, (MyJobInfo) job, scriptFile)){
      throw new IOException("Could not create wrapper script.");
    }
    ((MyJobInfo) job).setOutputs(stdoutFile, stderrFile);
    job.setExecutable(scriptFile);
    job.setMemory(defaultJobMB);
    
    return ok;
    
  }

  public void setupRuntimeEnvironments(String csName) {
    if(localRuntimeDBs==null || localRuntimeDBs.length==0){
      return;
    }
    MyUtil.syncRTEsFromCatalogs(csName, rteCatalogUrls, localRuntimeDBs, toDeleteRtes);
  }

}
