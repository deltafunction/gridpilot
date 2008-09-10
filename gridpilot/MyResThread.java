package gridpilot;

import gridfactory.common.ResThread;
import gridfactory.common.Shell;


public class MyResThread extends ResThread{
  
  /**
   * Implement this to be able to get an ShellMgr result from the process running in the thread.
   * @return a ShellMgr
   */
  public Shell getShellMgr(){
    throw new UnsupportedOperationException("getShellMgrRes not implemented!");
  }
  
}