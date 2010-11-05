package gridpilot;

import gridfactory.common.Debug;

import java.awt.Window;
import javax.swing.*;

/**
 * Creates the job definitions with datas given by JobDefCreationPanel.
 * This object removes all known constants from the attributes, and evaluates them.
 *
 */
public class JobDefCreator{

  private DBPluginMgr dbPluginMgr;
  private boolean showResults;
  private String [] cstAttr;
  private String [] cstAttrNames;
  private boolean editing;
  private String dbName;
  private Window parent;
  public boolean anyCreated = false;

  public JobDefCreator(Window _parent,
                       String _dbName,
                       boolean _showResults,
                       String [] _cstAttr,
                       String [] _cstAttrNames,
                       boolean _editing
                       ){
    parent = _parent;
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
      int choice = MyUtil.showResult(cstAttrNames, cstAttr, "Job definition", MyUtil.OK_SKIP_OPTION, "Skip");
      switch(choice){
        case 0  : skip = false;  break;  // OK
        case 1  : skip = true;   break; // Skip
        default : skip = true;  // other (closing the dialog). Same action as "Skip all"
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
      String jobDefIdentifier = MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "jobDefinition");
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
        if(JOptionPane.showConfirmDialog(parent, "JobDefinition " +
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
        if(JOptionPane.showConfirmDialog(parent, "JobDefinition" +
            " cannot be created. "+e.getMessage()+". "+dbPluginMgr.getError(), "",
            JOptionPane.PLAIN_MESSAGE)==JOptionPane.CANCEL_OPTION){
          //cancel creation
        }
      }
    }
  }
}
