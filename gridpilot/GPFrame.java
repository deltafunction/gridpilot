package gridpilot;

import java.awt.BorderLayout;
import java.net.URL;
import javax.swing.*;

import gridfactory.common.Debug;
import gridpilot.StatusBar;
import gridpilot.GridPilot;

/**
 * Parent class for GridPilot frames.
 */
public class GPFrame extends JFrame{

  private static final long serialVersionUID=1L;
  
  // This is the status bar on the individual window.
  // To get to the status bar on the main window, use
  // GridPilot.getClassMgr().getStatusBar()
  public StatusBar statusBar;
  
  /**
   * Constructor
   */
  public GPFrame(){
    
    //JFrame.setDefaultLookAndFeelDecorated(true);
    ImageIcon icon = null;
    URL imgURL = null;
    
    try{
      imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "aviateur.png");
      icon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.resourcesPath + "aviateur.png", 3);
      icon = new ImageIcon();
    }

    setIconImage(icon.getImage());
    statusBar = new StatusBar();
    this.getContentPane().add(statusBar, BorderLayout.SOUTH);
  }

}
