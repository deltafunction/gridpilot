package gridpilot;

import gridfactory.common.Debug;

import javax.swing.*;

/**
 * Creates the transformation records with data given by TransformationCreationPanel.
 * This object removes all known constants from the attributes, and evaluates them.
 */
public class TransformationCreator{

  private boolean showResults;
  private String [] cstAttr;
  private String [] cstAttrNames;
  private boolean editing;
  private DBPluginMgr dbPluginMgr = null;
  public boolean anyCreated = false;

  public TransformationCreator(
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
    createTransformationRecord();
  }

  private void createTransformationRecord(){
    int choice = 0;
    if(showResults){
      //choice = showResult();
      choice = MyUtil.showResult(cstAttrNames, cstAttr, "Transformation", 1);
    }

    switch(choice){
      case 0  : break;  // OK
      case 1  : return; // Skip
      default : return;
    }

    if(editing){
      String transformationIdentifier =
        MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "transformation");
      String id = "-1";
      for(int i=0; i<cstAttrNames.length; ++i){
        Debug.debug("Checking name "+transformationIdentifier+":"+cstAttrNames[i].toString(), 3);
        if(cstAttrNames[i].toString().equalsIgnoreCase(
            transformationIdentifier)){
          id = cstAttr[i];
          break;
        }
      }
      Debug.debug("Updating...", 3);
      if(!dbPluginMgr.updateTransformation(id, cstAttrNames, cstAttr)){
        JOptionPane.showMessageDialog(JOptionPane.getRootFrame(),
            "Transformation cannot be updated.\n"+
          dbPluginMgr.getError(), "", JOptionPane.PLAIN_MESSAGE);
      }
      else{
        anyCreated = true;
      }
    }
    else{
      if(!dbPluginMgr.createTransformation(cstAttr)){
        JOptionPane.showMessageDialog(JOptionPane.getRootFrame(),
            "Transformation cannot be created.\n"+
          dbPluginMgr.getError(), "", JOptionPane.PLAIN_MESSAGE);
      }
      else{
        anyCreated = true;
      }
    }
  }
}
