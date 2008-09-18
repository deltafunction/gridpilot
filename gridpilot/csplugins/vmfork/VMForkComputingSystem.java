package gridpilot.csplugins.vmfork;

import java.util.Arrays;
import java.util.HashMap;

import gridfactory.common.ConfigFile;
import gridfactory.common.Shell;
import gridfactory.common.jobrun.ForkComputingSystem;
import gridfactory.common.jobrun.RTEMgr;
import gridfactory.common.jobrun.VMMgr;
import gridpilot.GridPilot;
import gridpilot.MyComputingSystem;



public class VMForkComputingSystem extends ForkComputingSystem implements MyComputingSystem {

  private String csName;
  private String [] rteCatalogUrls;
  private String user;

  public VMForkComputingSystem(String _csName) throws Exception {
    csName = _csName;
    GridPilot.splashShow("Setting up virtual machine computing system...");
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
    rteMgr = new RTEMgr(csName, rteCatalogUrls, logFile, transferControl);
    vmMgr = new VMMgr(rteMgr, transferStatusUpdateControl, defaultVmMB, defaultVmMB, csName, logFile);
    transferStatusUpdateControl = GridPilot.getClassMgr().getTransferStatusUpdateControl();
    termVmOnJobEnd = false;
    // Just some reasonable number
    minVmMB = 512;
    // No need to run more than one VM - this CS is just for testing a single job before running it on GridFactory.
    maxMachines = 1;
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
    virtEnforce = true;
    defaultVmMB = minVmMB;
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

  }

  public void cleanupRuntimeEnvironments(String csName) {
    // Nothing to do.
  }

  public String getUserInfo(String csName) {
    return user;
  }

  public void setupRuntimeEnvironments(String csName) {
    // Nothing to do.
  }

}
