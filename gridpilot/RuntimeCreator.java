package gridpilot;

import java.awt.Window;

import gridfactory.common.Debug;

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
  private Window parent;
  public boolean anyCreated = false;

  public RuntimeCreator(  Window _parent,
                          DBPluginMgr _dbPluginMgr,
                          boolean _showResults,
                          String [] _cstAttr,
                          String [] _cstAttrNames,
                          boolean _editing
                          ){

    parent = _parent;
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
      choice = MyUtil.showResult(cstAttrNames, cstAttr, "Runtime environment", MyUtil.OK_SKIP_OPTION, "Skip");
    }
    switch(choice){
      case 0  : break;  // OK
      case 1  : return; // Skip
      default : return;
    }
    if(editing){
      String tranformationIdentifier =
        MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "runtimeEnvironment");
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
        JOptionPane.showMessageDialog(GridPilot.getClassMgr().getGlobalFrame(),
            "Runtime environment cannot be updated.\n"+
          dbPluginMgr.getError(), "", JOptionPane.PLAIN_MESSAGE);
      }
      else{
        anyCreated = true;
      }
    }
    else{
      if(!dbPluginMgr.createRuntimeEnvironment(cstAttr)){
        JOptionPane.showMessageDialog(parent,
            "Runtime environment cannot be created.\n"+
          dbPluginMgr.getError(), "", JOptionPane.PLAIN_MESSAGE);
      }
      else{
        anyCreated = true;
      }
    }
  }
}
