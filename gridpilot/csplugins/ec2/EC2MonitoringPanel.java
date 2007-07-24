package gridpilot.csplugins.ec2;

import gridpilot.CreateEditPanel;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.StatusBar;

import java.awt.*;

import javax.swing.JPanel;

/**
 * Panel showing the status of EC2 and containing buttons for
 * starting and stopping virtual machines.
 * 
 * Available images: <selectable list>  [launch]
 * Running images: <selectable list> [configure access] [stop]
 * 
 */
public class EC2MonitoringPanel extends CreateEditPanel {

  private static final long serialVersionUID = 1L;
  public StatusBar statusBar = null;

  public EC2MonitoringPanel() throws Exception{   
    // use status bar on main window until a monitoring panel is actually created
    statusBar = GridPilot.getClassMgr().getStatusBar();
  }
  
  public String getTitle(){
    return "EC2 Monitor";
  }

  public void initGUI(){
    
    this.setLayout(new BorderLayout());
    statusBar = new StatusBar();
    
    JPanel panel = createControlPanel();

    this.getTopLevelAncestor().add(panel, BorderLayout.CENTER);
    this.setPreferredSize(new Dimension(700, 500));
    this.getTopLevelAncestor().add(statusBar, BorderLayout.SOUTH);
    
    this.validate();
  }
  
  private JPanel createControlPanel(){
    JPanel panel = new JPanel();
    // TODO: implement lists, buttons, etc.
    
    
    
    
    return panel;
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

