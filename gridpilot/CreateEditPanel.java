package gridpilot;

import gridfactory.common.StatusBar;

import javax.swing.JPanel;

/**
 * @author fjob
 *
 */
public abstract class CreateEditPanel extends JPanel{
  
  public StatusBar statusBar;

  public void clearPanel(){
  }

  public void create(final boolean showResults, boolean editing) {
  }

  public void initGUI() throws Exception{
  }

  public void activate() throws Exception{
  }

  public void showDetails(boolean show){
  }

  public void windowClosing(){
  }

  // This is relevant only for JobDefCreationPanel, which has
  // a method for saving the settings in the text fields as metadata
  // in the dataset record.
  public void saveSettings(){
  }

}
