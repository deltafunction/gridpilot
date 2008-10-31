package gridpilot.csplugins.vmfork;

import gridfactory.common.PullMgr;
import gridpilot.GridPilot;

public class MyPullMgr extends PullMgr {

  public MyPullMgr() {
    logFile = GridPilot.getClassMgr().getLogFile();
    String localRteDir = GridPilot.runtimeDir;
    logFile = GridPilot.getClassMgr().getLogFile();
    String [] rteCatalogUrls = GridPilot.getClassMgr().getConfigFile().getValues(GridPilot.topConfigSection, "runtime catalog URLs");
    transferControl = GridPilot.getClassMgr().getTransferControl();
    rteMgr = GridPilot.getClassMgr().getRTEMgr(localRteDir, rteCatalogUrls);
  }

}
