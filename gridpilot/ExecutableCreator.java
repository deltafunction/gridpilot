package gridpilot;

import java.awt.Window;

import gridfactory.common.Debug;

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
  private Window parent;
  public boolean anyCreated = false;

  public ExecutableCreator(
                          Window _parent,
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

    StringBuffer error = new StringBuffer();
    boolean ok = checkFields(error);
    if(!ok){
      MyUtil.showError(parent, error.toString());
      return;
    }
    
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
        MyUtil.showError(parent, "Executable cannot be updated.\n"+dbPluginMgr.getError());
      }
      else{
        anyCreated = true;
      }
    }
    else{
      ok = ok && dbPluginMgr.createExecutable(cstAttr);
      if(!ok){
        MyUtil.showError(parent, "Executable cannot be created.\n"+error+dbPluginMgr.getError());
      }
      else{
        anyCreated = true;
      }
    }
  }

  private boolean checkFields(StringBuffer error) {
    boolean ok = true;
    // Check that name and version have been set
    String [] datasetExecutableReference =
      MyUtil.getDatasetExecutableReference(dbPluginMgr.getDBName());
    String [] datasetExecutableVersionReference =
      MyUtil.getDatasetExecutableVersionReference(dbPluginMgr.getDBName());
    String [] executableRuntimeReference = MyUtil.getExecutableRuntimeReference(dbPluginMgr.getDBName());
    for(int i=0; i<cstAttrNames.length; ++i){
      if(cstAttrNames[i].toString().equalsIgnoreCase(datasetExecutableVersionReference[0])){
        if(cstAttr[i]==null || cstAttr[i].equals("")){
          ok = false;
          error.append("You must fill in "+datasetExecutableVersionReference[0]+". ");
        }
      }
      else if(cstAttrNames[i].toString().equalsIgnoreCase(datasetExecutableReference[0])){
        if(cstAttr[i]==null || cstAttr[i].equals("")){
          ok = false;
          error.append("You must fill in "+datasetExecutableReference[0]+". ");
        }
      }
      else if(cstAttrNames[i].toString().equalsIgnoreCase(executableRuntimeReference[1])){
        if(cstAttr[i]==null || cstAttr[i].equals("")){
          ok = false;
          error.append("You must select a "+executableRuntimeReference[1]+". ");
        }
      }
    }
    return ok;
  }
}
