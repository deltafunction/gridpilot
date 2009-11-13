package gridpilot.csplugins.vmfork;

import java.io.File;

import gridfactory.common.JobInfo;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.PullMgr;

import gridpilot.GridPilot;
import gridpilot.MyUtil;

public class MyPullMgr extends PullMgr {

  public MyPullMgr() {
    logFile = GridPilot.getClassMgr().getLogFile();
    String localRteDir = GridPilot.RUNTIME_DIR;
    logFile = GridPilot.getClassMgr().getLogFile();
    String [] rteCatalogUrls = GridPilot.getClassMgr().getConfigFile().getValues(GridPilot.TOP_CONFIG_SECTION, "runtime catalog URLs");
    transferControl = GridPilot.getClassMgr().getTransferControl();
    rteMgr = GridPilot.getClassMgr().getRTEMgr(localRteDir, rteCatalogUrls);
    jobCacheDir = GridPilot.getClassMgr().getConfigFile().getValue("VMFork", "File cache directory");
    if(jobCacheDir==null){
      logFile.addMessage("WARNING: 'File cache directory not' set in configuration file. " +
          "You will not be able to run jobs requiring input files from file servers.");
    }
  }
  
  public void deleteInputs(JobInfo job){
    String subDir = MyUtil.getjobDirName(job);
    File jobDlDir = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(jobCacheDir)), subDir);
    LocalStaticShell.deleteDir(jobDlDir.getAbsolutePath());
  }

}
