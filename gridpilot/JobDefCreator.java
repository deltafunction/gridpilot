package gridpilot;

import java.util.*;
import javax.swing.*;

/**
 * Creates the job definitions with datas given by JobDefCreationPanel.
 * This object removes all known constants from the attributes, and evaluates them.
 *
 */
public class JobDefCreator{

  //private DatasetMgr datasetMgr;
  private DBPluginMgr dbPluginMgr;
  private boolean showResults;
  private String [] cstAttr;
  private String [] cstAttrNames;
  private boolean editing;
  private String dbName;
  public boolean anyCreated = false;

  public JobDefCreator(String _dbName,
                       //DatasetMgr _datasetMgr,
                       boolean _showResults,
                       Vector _constants,
                       String [] _cstAttr,
                       String [] _cstAttrNames,
                       boolean _editing
                       ){

    dbName = _dbName;
    dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName); 
    showResults = _showResults;
    cstAttr = _cstAttr;
    cstAttrNames =  _cstAttrNames;
    editing = _editing;
    Debug.debug("Are we editing? "+editing,3);
    createJobDefs();
  }

  private void createJobDefs(){
    Debug.debug("createJobDefs", 1);
    
    boolean skip = false;
    boolean showThis;
    showThis = showResults;

    if(showThis){
      //int choice = showResult(cstAttr);
      int choice = Util.showResult(cstAttrNames, cstAttr, "Job definition", 1);
      switch(choice){
        case 0  : skip = false;  break;  // OK
        case 1  : skip = true;   break; // Skip
        default : skip = true;  // other (closing the dialog). Same action than "Skip all"
      }
    }
    if(!skip){
      Debug.debug("creating jobDefinition", 2);
    }

    if(!skip){
      Debug.debug("going to call createDBJobDef", 2);
      createDBJobDef();
    }
  }

  private void createDBJobDef(){
    
    Debug.debug(this.getClass().getName() + " is calling DB", 2);

    if(editing){
      String jobDefIdentifier = Util.getIdentifierField(dbPluginMgr.getDBName(), "jobDefinition");
      String id = "-1";
      for(int i=0; i<cstAttrNames.length; ++i){
        if(cstAttrNames[i].toString().equalsIgnoreCase(
            jobDefIdentifier)){
          id = cstAttr[i];
          break;
        }
      }
      Debug.debug("Updating..."+cstAttrNames.length+" : "+cstAttr.length, 3);
      if(!dbPluginMgr.updateJobDefinition(id, cstAttrNames, cstAttr)){
        if(JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), "JobDefinition " +
            " cannot be updated", "",
            JOptionPane.OK_CANCEL_OPTION)==JOptionPane.CANCEL_OPTION){
          //cancel updating
        }
      }
      else{
        anyCreated = true;
      }
    }
    else{
      Debug.debug("Creating..."+cstAttrNames.length+" : "+cstAttr.length, 3);
      try{
        dbPluginMgr.createJobDef(cstAttrNames, cstAttr);
        anyCreated = true;
      }
      catch(Exception e){
        Debug.debug(e.getMessage(), 1);
        e.printStackTrace();
        if(JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), "JobDefinition" +
            " cannot be created. "+e.getMessage()+". "+dbPluginMgr.getError(), "",
            JOptionPane.PLAIN_MESSAGE)==JOptionPane.CANCEL_OPTION){
          //cancel creation
        }
      }
    }
  }
}
