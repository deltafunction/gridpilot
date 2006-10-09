package gridpilot;

import javax.swing.*;

/**
 * Creates the runtime environment records with data given by RuntimeCreationPanel.
 * This object removes all known constants from the attributes, and evaluates them.
 */
public class RuntimeCreator{

  private boolean showResults;
  private String [] cstAttr;
  private String [] cstAttrNames;
  private boolean editing;
  private DBPluginMgr dbPluginMgr = null;

  public RuntimeCreator(
                          DBPluginMgr _dbPluginMgr,
                          boolean _showResults,
                          String [] _cstAttr,
                          String [] _cstAttrNames,
                          boolean _editing
                          ){

    dbPluginMgr = _dbPluginMgr;
    showResults = _showResults;
    cstAttr = _cstAttr;
    cstAttrNames =  _cstAttrNames;
    editing = _editing;

   Debug.debug("Are we editing? "+editing,3);
   createRuntimeEnvironmentRecord();
  }

  private void createRuntimeEnvironmentRecord(){
    int choice = 0;
    if(showResults){
      //choice = showResult();
      choice = Util.showResult(cstAttrNames, cstAttr, "Runtime environment", 1);
    }
    switch(choice){
      case 0  : break;  // OK
      case 1  : return; // Skip
      default : return;
    }
    if(editing){
      String tranformationIdentifier =
        dbPluginMgr.getIdentifierField("runtimeEnvironment");
      String id = "-1";
      for(int i=0; i<cstAttrNames.length; ++i){
        Debug.debug("Checking name "+tranformationIdentifier+":"+cstAttrNames[i].toString(), 3);
        if(cstAttrNames[i].toString().equalsIgnoreCase(
            tranformationIdentifier)){
          id = cstAttr[i];
          break;
        }
      }
      Debug.debug("Updating...", 3);
      if(!dbPluginMgr.updateRuntimeEnvironment(id, cstAttrNames, cstAttr)){
        JOptionPane.showMessageDialog(JOptionPane.getRootFrame(),
            "Runtime environment cannot be updated.\n"+
          dbPluginMgr.getError(), "", JOptionPane.PLAIN_MESSAGE);
      }
    }
    else{
      if(!dbPluginMgr.createRuntimeEnvironment(cstAttr)){
        JOptionPane.showMessageDialog(JOptionPane.getRootFrame(),
            "Runtime environment cannot be created.\n"+
          dbPluginMgr.getError(), "", JOptionPane.PLAIN_MESSAGE);
      }
    }     
  }
}
