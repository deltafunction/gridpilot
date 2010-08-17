package gridpilot;

import gridfactory.common.Debug;

import javax.swing.*;

/**
 * Creates the executable records with data given by ExecutableCreationPanel.
 * This object removes all known constants from the attributes, and evaluates them.
 */
public class ExecutableCreator{

  private boolean showResults;
  private String [] cstAttr;
  private String [] cstAttrNames;
  private boolean editing;
  private DBPluginMgr dbPluginMgr = null;
  public boolean anyCreated = false;

  public ExecutableCreator(
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
    createExecutableRecord();
  }

  private void createExecutableRecord(){
    int choice = 0;
    if(showResults){
      choice = MyUtil.showResult(cstAttrNames, cstAttr, "Executable", MyUtil.OK_SKIP_OPTION, "Skip");
    }

    switch(choice){
      case 0  : break;  // OK
      case 1  : return; // Skip
      default : return;
    }

    boolean ok = true;
    String error = "";
    if(editing){
      String executableIdentifier =
        MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "executable");
      String id = "-1";
      for(int i=0; i<cstAttrNames.length; ++i){
        Debug.debug("Checking name "+executableIdentifier+":"+cstAttrNames[i].toString(), 3);
        if(cstAttrNames[i].toString().equalsIgnoreCase(executableIdentifier)){
          id = cstAttr[i];
          break;
        }
      }
      Debug.debug("Updating...", 3);
      if(!dbPluginMgr.updateExecutable(id, cstAttrNames, cstAttr)){
        JOptionPane.showMessageDialog(JOptionPane.getRootFrame(),
            "Executable cannot be updated.\n"+
          dbPluginMgr.getError(), "", JOptionPane.PLAIN_MESSAGE);
      }
      else{
        anyCreated = true;
      }
    }
    else{
      // Check that name and version have been set
      String [] datasetExecutableReference =
        MyUtil.getDatasetExecutableReference(dbPluginMgr.getDBName());
      String [] datasetExecutableVersionReference =
        MyUtil.getDatasetExecutableVersionReference(dbPluginMgr.getDBName());
      for(int i=0; i<cstAttrNames.length; ++i){
        if(cstAttrNames[i].toString().equalsIgnoreCase(datasetExecutableVersionReference[0])){
          if(cstAttr[i]==null || cstAttr[i].equals("")){
            ok = false;
            error = error+"You must fill in "+datasetExecutableVersionReference[0]+". ";
          }
        }
        else if(cstAttrNames[i].toString().equalsIgnoreCase(datasetExecutableReference[0])){
          if(cstAttr[i]==null || cstAttr[i].equals("")){
            ok = false;
            error = error+"You must fill in "+datasetExecutableReference[0]+". ";
          }
        }
      }
      ok = ok && dbPluginMgr.createExecutable(cstAttr);
      if(!ok){
        JOptionPane.showMessageDialog(JOptionPane.getRootFrame(),
            "Executable cannot be created.\n"+
          error+dbPluginMgr.getError(), "", JOptionPane.PLAIN_MESSAGE);
      }
      else{
        anyCreated = true;
      }
    }
  }
}
