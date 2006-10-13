package gridpilot;

import javax.swing.*;

import java.awt.*;

/**
 * Tabbed panel with job, transfer and log monitors
 *
 * <p><a href="MonitoringPanel.java.html">see sources</a>
 */
public class MonitoringPanel extends CreateEditPanel {

  private static final long serialVersionUID = 1L;
  // use status bar on main window
  public StatusBar statusBar = null;
  // Central panel
  public JTabbedPane tpStatLog = new JTabbedPane();
  public JobMonitoringPanel jobMonitor = new JobMonitoringPanel();
  public TransferMonitoringPanel transferMonitor = new TransferMonitoringPanel();
  private JScrollPane spLogView = new JScrollPane();
  private LogViewerPanel logViewerPanel = new LogViewerPanel();

  /**
   * Constructor
   */
  public MonitoringPanel() throws Exception{   
    statusBar = GridPilot.getClassMgr().getStatusBar();
  }
  
  public String getTitle(){
    return "Monitor";
  }

  public void initGUI(){
    
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
    tpStatLog.addTab("Logs", spLogView);

    this.getTopLevelAncestor().add(tpStatLog, BorderLayout.CENTER);
    this.setPreferredSize(new Dimension(700, 500));
    this.getTopLevelAncestor().add(statusBar, BorderLayout.SOUTH);
    
    this.validate();

  }

  public void windowClosing(){
    GridPilot.getClassMgr().getGlobalFrame().cbMonitor.setSelected(false);
  }

  /**
   * Called when this panel is shown.
   */
  public void panelShown(){
    Debug.debug("panelShown",1);
    statusBar.setLabel(GridPilot.getClassMgr().getSubmittedJobs().size() + " job(s) monitored");
  }

  /**
   * Called when this panel is hidden
   */
  public void panelHidden(){
    Debug.debug("panelHidden", 1);
    statusBar.removeLabel();
  }

}

