package gridpilot;

import java.net.URL;
import javax.swing.*;
import gridpilot.StatusBar;
import gridpilot.Debug;
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
  }

}
