package gridpilot;

import javax.swing.JPanel;

import gridfactory.common.ResThread;
import gridfactory.common.Shell;


public class MyResThread extends ResThread{
  
  /**
   * Implement this to be able to get a ShellMgr result from the process running in the thread.
   * @return a ShellMgr
   */
  public Shell getShellRes(){
    throw new UnsupportedOperationException("getShellRes not implemented!");
  }

  /**
   * Implement this to be able to get a DBPanel result from the process running in the thread.
   * @return a DBPanel
   */
  public DBPanel getDBPanelRes(){
    throw new UnsupportedOperationException("getDBPanelRes not implemented!");
  }

  /**
   * Implement this to be able to get a BrowserPanel result from the process running in the thread.
   * @return a BrowserPanel
   */
  public BrowserPanel getBrowserPanelRes() {
    throw new UnsupportedOperationException("getBrowserPanelRes not implemented!");
  }
  
  /**
   * Implement this to be able to get a JPanel result from the process running in the thread.
   * @return a JPanel
   */
  public JPanel getJPanelRes() {
    throw new UnsupportedOperationException("getJPanelRes not implemented!");
  }
  
}