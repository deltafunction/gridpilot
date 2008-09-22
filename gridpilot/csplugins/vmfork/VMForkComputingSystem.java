package gridpilot.csplugins.vmfork;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.Shell;
import gridfactory.common.Util;
import gridfactory.common.jobrun.ForkComputingSystem;
import gridfactory.common.jobrun.RTEInstaller;
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
    minVmMB = 512;
    // Just some reasonable number
    defaultJobMB = 512;
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
    localRteDir = configFile.getValue(csName, "runtime directory");
    remoteRteDir = localRteDir;
    workingDir = configFile.getValue(csName, "working directory");
    logFile = GridPilot.getClassMgr().getLogFile();
    rteCatalogUrls = configFile.getValues("GridPilot", "runtime catalog URLs");
    transferControl = GridPilot.getClassMgr().getTransferControl();
    rteMgr = new RTEMgr(localRteDir, rteCatalogUrls, logFile, transferControl);
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
    vmMgr = new VMMgr(rteMgr, transferStatusUpdateControl, defaultVmMB, bootTimeout, localRteDir, logFile);
    
    localRuntimeDBs = configFile.getValues(csName, "runtime databases");
  }
  
  protected void setupJobRTEs(JobInfo job, Shell shell) throws Exception{
    String [] rteNames = job.getRTEs();
    Vector<String> rtes = new Vector<String>();
    Collections.addAll(rtes, rteNames);
    Vector<String> deps = rteMgr.getRteDepends(rtes, job.getOpSys());
    RTEInstaller rteInstaller = null;
    String url = null;
    String mountPoint = null;
    String name = null;
    String os = null;
    boolean osEntry = true;
    Debug.debug("Setting up RTEs "+Util.arrayToString(deps.toArray()), 2);
    for(Iterator<String> it=deps.iterator(); it.hasNext();){
      // The first dependency is the OS; skip it. The fact
      // that the job landed on this system means it matches.
      if(osEntry){
        os = it.next();
        osEntry = false;
        if(!it.hasNext()){
          break;
        }
      }
      name = it.next();
      // Check if installation was already done.
      // This we need, because GridPilot's HTTPSFileTransfer does not cache.
      // GridFactory's does.
      if(shell.existsFile(remoteRteDir+"/"+name+"/"+RTEInstaller.INSTALL_OK_FILE)){
        logFile.addInfo("Reusing existing installation of "+name);
        return;
      }
      url = rteMgr.getRteURL(name, os);
      mountPoint = rteMgr.getRteMountPoint(name, os);
      rteInstaller = new RTEInstaller(url, remoteRteDir, localRteDir, mountPoint, name, shell, transferStatusUpdateControl, logFile);
      rteInstaller.install();
    }
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
    
    return ok;
    
  }

  public void setupRuntimeEnvironments(String csName) {
    if(localRuntimeDBs==null || localRuntimeDBs.length==0){
      return;
    }
    for(int i=0; i<localRuntimeDBs.length; ++i){
      try{
        GridPilot.getClassMgr().getDBPluginMgr(localRuntimeDBs[i]).createRuntimeEnv(
            new String [] {"name", "computingSystem"}, new String [] {MyUtil.getMyOS() , csName});
      }
      catch(Exception e){
        e.printStackTrace();
      }
      toDeleteRtes.put(MyUtil.getMyOS(), localRuntimeDBs[i]);
    }
    MyUtil.syncRTEsFromCatalogs(csName, rteCatalogUrls, localRuntimeDBs, toDeleteRtes);
  }

}
