package gridpilot;

import gridfactory.common.Debug;
import gridfactory.common.LogViewerPanel;
import gridfactory.common.StatusBar;

import javax.swing.*;

import java.awt.*;
import java.util.Iterator;

/**
 * Tabbed panel with job, transfer and log monitors
 *
 * <p><a href="MonitoringPanel.java.html">see sources</a>
 */
public class MonitoringPanel extends CreateEditPanel {

  public static final int TAB_INDEX_JOBS = 0;
  public static final int TAB_INDEX_TRANSFERS = 1;
  private static final long serialVersionUID = 1L;
  
  // Central panel
  private JTabbedPane tpStatLog;
  private StatusBar statusBar = null;
  private JobMonitoringPanel jobMonitor;
  private TransferMonitoringPanel transferMonitor;
  private JScrollPane spLogView;
  private LogViewerPanel logViewerPanel;

  public MonitoringPanel() throws Exception{   
  }
  
  public String getTitle(){
    return "Monitor";
  }
  
  public JobMonitoringPanel getJobMonitoringPanel() {
    return jobMonitor;
  }

  public TransferMonitoringPanel getTransferMonitoringPanel() {
    return transferMonitor;
  }

  public StatusBar getStatusBar() {
    if(statusBar==null){
      // use status bar on main window until a monitoring panel is actually created
      return GridPilot.getClassMgr().getStatusBar();

    }
    return statusBar;
  }
  
  public void activate() throws Exception {
    jobMonitor.activate();
    transferMonitor.activate();
    SwingUtilities.invokeLater(
      new Runnable(){
        public void run(){
          // add any panels added by plugins
          for(Iterator<JPanel> it=GridPilot.EXTRA_MONITOR_TABS.iterator(); it.hasNext();){
            JPanel panel = it.next();
            tpStatLog.addTab(panel.getName(), panel);
          }
          tpStatLog.addTab("Log", spLogView);
        }
      }
    );
  }

  public void initGUI() throws Exception {
    
    tpStatLog = new JTabbedPane();
    jobMonitor = new JobMonitoringPanel();
    transferMonitor = new TransferMonitoringPanel();    
    spLogView = new JScrollPane();
    logViewerPanel = new LogViewerPanel();
    
    GridPilot.getClassMgr().getLogFile().addActionOnMessage(logViewerPanel.new MyActionOnMessage());
    
    this.setLayout(new BorderLayout());
    statusBar = new StatusBar();

    // central panel
    tpStatLog.setTabPlacement(JTabbedPane.BOTTOM);
    jobMonitor.initGUI();
    transferMonitor.initGUI();
    
    spLogView.getViewport().add(logViewerPanel);
    spLogView.setPreferredSize(new Dimension(700, 500));
    tpStatLog.addTab("Jobs", jobMonitor);
    tpStatLog.addTab("Transfers", transferMonitor);
    
    this.getTopLevelAncestor().add(tpStatLog, BorderLayout.CENTER);
    this.setPreferredSize(new Dimension(700, 500));
    this.getTopLevelAncestor().add(statusBar, BorderLayout.SOUTH);
    this.validate();

  }

  public void windowClosing(){
    GridPilot.getClassMgr().getGlobalFrame().getCBMonitor().setSelected(false);
  }

  /**
   * Called when this panel is shown.
   */
  public void panelShown(){
    Debug.debug("panelShown",1);
    statusBar.setLabel(GridPilot.getClassMgr().getMonitoredJobs().size() + " job(s) monitored");
  }

  /**
   * Called when this panel is hidden
   */
  public void panelHidden(){
    Debug.debug("panelHidden", 1);
    statusBar.removeLabel();
  }

  public JTabbedPane getTPStatLog() {
    return tpStatLog;
  }

}

