package gridpilot;

import java.awt.BorderLayout;
import java.awt.Window;
import java.net.URL;
import javax.swing.*;

import gridfactory.common.Debug;
import gridfactory.common.StatusBar;
import gridpilot.GridPilot;

/**
 * Parent class for GridPilot frames.
 */
public class GPDialog extends JDialog{

  private static final long serialVersionUID=1L;
  
  // This is the status bar on the individual window.
  // To get to the status bar on the main window, use
  // GridPilot.getClassMgr().getStatusBar()
  public StatusBar statusBar;
  
  public GPDialog(Window owner){
    super(owner);
    init();
  }
  
  public GPDialog(){
    super(GridPilot.getClassMgr().getGlobalFrame());
    init();
    setModal(false);
  }
  
  private void init(){
    //JFrame.setDefaultLookAndFeelDecorated(true);
    ImageIcon icon = null;
    URL imgURL = null;
    
    try{
      imgURL = GridPilot.class.getResource(GridPilot.RESOURCES_PATH + "aviateur.png");
      icon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.RESOURCES_PATH + "aviateur.png", 3);
      icon = new ImageIcon();
    }

    setIconImage(icon.getImage());
    statusBar = new StatusBar();
    this.getContentPane().add(statusBar, BorderLayout.SOUTH);
  }

}
