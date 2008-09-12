package gridpilot;

import javax.swing.JFrame;

import gridfactory.common.ConfigFile;
import gridfactory.common.ConfigNode;
import gridfactory.common.LogFile;
import gridfactory.common.PreferencesPanel;

public class MyPreferencesPanel extends PreferencesPanel {

  private static final long serialVersionUID = 1L;

  public MyPreferencesPanel(JFrame arg0, ConfigNode arg1, ConfigFile arg2, LogFile arg3) {
    super(arg0, arg1, arg2, arg3);
  }

  public void savePrefs(){
    super.savePrefs();
    GridPilot.reloadConfigValues();
  }

}
