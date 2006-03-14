package gridpilot;

import javax.swing.*;
import java.awt.*;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Creates the dataset with data given by DatasetCreationPanel.
 */
public class DatasetCreator{

  private boolean showResults;
  private String [] cstAttr;
  private StatusBar statusBar;
  private String [] resCstAttr;
  private String [] cstAttrNames;
  private int [] datasetIDs;
  private String targetDB;
  private static JProgressBar pb = new JProgressBar();
  private static Object semaphoreAMICreation = new Object();
  private DBPluginMgr dbPluginMgr;

  private Object[] showResultsOptions = {"OK", "Skip", "OK for all", "Skip all"};
  private Object[] showResultsOptions1 = {"OK", "Skip"};

  public DatasetCreator(  
                          DBPluginMgr _dbPluginMgr,
                          boolean _showResults,
                          String [] _cstAttr,
                          String [] _cstAttrNames,
                          int [] _datasetIDs,
                          String _targetDB
                          ){

    showResults = _showResults;
    cstAttr = _cstAttr;
    cstAttrNames =  _cstAttrNames;
	  resCstAttr = _cstAttr;
    datasetIDs =  _datasetIDs;
    targetDB = _targetDB;
    
    statusBar = GridPilot.getClassMgr().getStatusBar();
    dbPluginMgr = _dbPluginMgr;

    createDataset();
  }
  
  private void createDataset(){
    
    boolean okAll = false;
    boolean skip = false;
    boolean showThis  = showResults;
    
    //String reconName = "";
    
    if(targetDB!=null){   
      // this set is used to keep track of which fields were set to ""
      HashSet clearAttrs = new HashSet();
      String transformationID = "";
      for(int j=0; j<cstAttrNames.length; ++j){
        if(cstAttrNames[j].equalsIgnoreCase("transformationFK") ||
            cstAttrNames[j].equalsIgnoreCase("transFK")){
          transformationID = cstAttr[j];
        }
      }
      for(int i=0; i<datasetIDs.length; ++i){     
        Debug.debug("creating #"+datasetIDs[i], 2);
        clearAttrs.clear();        
        for(int j=0; j<cstAttrNames.length; ++j){                   
          if(datasetIDs[i]>0){
            // Get values from source dataset in question, excluding
            // transformation, transVersion and any other filled-in values.
            // Construct name for new target dataset.
            if(cstAttrNames[j].equalsIgnoreCase("name") ||
                cstAttrNames[j].equalsIgnoreCase("taskName")){
              resCstAttr[j] = dbPluginMgr.getTargetDatasetName(
                  targetDB,
                  dbPluginMgr.getDatasetName(datasetIDs[i]),
                  transformationID);
            }
            else if(cstAttrNames[j].equalsIgnoreCase("runNumber")){
              resCstAttr[j] = dbPluginMgr.getRunNumber(datasetIDs[i]);
            }
            else if(cstAttrNames[j].equalsIgnoreCase("InputDataset")){
              resCstAttr[j] = dbPluginMgr.getDatasetName(datasetIDs[i]);
            }
            else if(cstAttrNames[j].equalsIgnoreCase("identifier") ||
                cstAttrNames[j].equalsIgnoreCase("percentageValidatedFiles") ||
                cstAttrNames[j].equalsIgnoreCase("percentageFailedFiles ") ||
                //cstAttrNames[j].equalsIgnoreCase("totalFiles") ||
                //cstAttrNames[j].equalsIgnoreCase("totalEvents") ||
                cstAttrNames[j].equalsIgnoreCase("averageEventSize") ||
                cstAttrNames[j].equalsIgnoreCase("totalDataSize") ||
                cstAttrNames[j].equalsIgnoreCase("averageCPUTime") ||
                cstAttrNames[j].equalsIgnoreCase("totalCPUTime") ||
                cstAttrNames[j].equalsIgnoreCase("created") ||
                cstAttrNames[j].equalsIgnoreCase("lastModified") ||
                cstAttrNames[j].equalsIgnoreCase("lastStatusUpdate")){
              resCstAttr[j] = "";
            }
            // See if attribute has not been set. If it hasn't, set it and clear it
            // again after the new dataset has been created.
            else if(resCstAttr[j]==null || resCstAttr[j].equals("")){
              String arg = "select "+cstAttrNames[j]+" from dataset where logicalDatasetName='"+
              dbPluginMgr.getDatasetName(datasetIDs[i])+"'";
              Database.DBRecord res = dbPluginMgr.getDataset(datasetIDs[i]);
              try{
                if(res.values.length==1){
                  resCstAttr[j] = res.getValue(cstAttrNames[j]).toString();
                  clearAttrs.add(new Integer(j));
                }
              }
              catch(Exception e){
                e.printStackTrace();
              }
            }
            if(resCstAttr[j]==null){
              resCstAttr[j] = "";
            }
          }       
        }
        if(showThis && !okAll){
        int choice = showResult(resCstAttr,datasetIDs[i],i+1<datasetIDs.length);  
        switch(choice){
          case 0  : skip = false; break;  // OK
          case 1  : skip = true ; break;  // Skip
          case 2  : skip = false; okAll = true ;break;  // OK for all
          case 3  : skip = true ; return; // Skip all
          default : skip = true; break;   // other (closing the dialog). Same action as "Skip"
          }
        }
        
        if(!skip || okAll){
          if(!createDataset(targetDB+".dataset")){
            return;
          };
          //statusBar.removeLabel();
        }
        // Clear attributes that were set to "" on the panel and are thus
        // to be read from each source dataset.
        for(Iterator it=clearAttrs.iterator(); it.hasNext(); ){
          resCstAttr[((Integer) it.next()).intValue()]="";
        }
      }
    }
    else{
      for(int i=0; i<datasetIDs.length; ++i){     
        
        Debug.debug("creating #"+datasetIDs[i], 2);
        //vDataset.add(new Integer(0));
        for(int j=0; j<resCstAttr.length; ++j){
        }

        if(showThis && !okAll){
        int choice = showResult(resCstAttr,datasetIDs[i],i+1<datasetIDs.length);  
        switch(choice){
          case 0  : skip = false; break;  // OK
          case 1  : skip = true ; break;  // Skip
          case 2  : skip = false; okAll = true ;break;  // OK for all
          case 3  : skip = true ; return; // Skip all
          default : skip = true; break;   // other (closing the dialog). Same action as "Skip"
          }
        }
        
        if(!skip || okAll){
          if(!createDataset("dataset")){
            return;
          };
          //statusBar.removeLabel();
        }
      }
    }
    
  }

  private boolean createDataset(String target_table){
    synchronized(semaphoreAMICreation){
          statusBar.setLabel("Creating dataset ...");
          pb.setValue(pb.getValue()+1);
        
          boolean succes = dbPluginMgr.createDataset(
              target_table, cstAttrNames, resCstAttr);
          if(!succes){
            JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(),
               "ERROR: dataset cannot be created in "+target_table,
               "", JOptionPane.OK_OPTION);
            statusBar.setLabel("Dataset NOT created.");
            return false;
          }
          else{
            statusBar.setLabel("Dataset created.");
          }
    }
    return true;
  }


  private int showResult(String [] resCstAttr, int _datasetID, boolean moreThanOne){

    JPanel pResult = new JPanel(new GridBagLayout());
    int row = 0;

    for(int i=0; i<cstAttr.length; ++i, ++row){
      pResult.add(new JLabel(cstAttrNames[i] + " : "), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
      ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));
      if(cstAttrNames[i].equalsIgnoreCase("datasetFK") && _datasetID>0){
      pResult.add(new JLabel(Integer.toString(_datasetID)), new GridBagConstraints(1, row, 3, 1, 1.0, 0.0
          ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
      }
      else if(cstAttrNames[i].equalsIgnoreCase("init")){
        JTextArea ta = new JTextArea(resCstAttr[i]);
        ta.setWrapStyleWord(true);
        ta.setLineWrap(true);
        ta.setEditable(false);
        pResult.add(ta, new GridBagConstraints(1, row, 3, 1, 1.0, 0.0
            ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
      }
      else{
        pResult.add(new JLabel(resCstAttr[i]), new GridBagConstraints(1, row, 3, 1, 1.0, 0.0
            ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
      }
    }


    JScrollPane sp = new JScrollPane(pResult);
    sp.setPreferredSize(new Dimension(500,
                                      (int)pResult.getPreferredSize().getHeight() +
                                      (int)sp.getHorizontalScrollBar().getPreferredSize().getHeight() + 5));

    JOptionPane op;
    if(moreThanOne){
       op = new JOptionPane(sp,
          JOptionPane.QUESTION_MESSAGE,
          JOptionPane.YES_NO_CANCEL_OPTION,
          null,
          showResultsOptions,
          showResultsOptions[0]);
    }
    else{
      op = new JOptionPane(sp,
          JOptionPane.QUESTION_MESSAGE,
          JOptionPane.YES_NO_CANCEL_OPTION,
          null,
          showResultsOptions1,
          showResultsOptions[0]);
    }

    JDialog dialog = op.createDialog(JOptionPane.getRootFrame(), "Dataset");
    dialog.setResizable(true);
    dialog.setVisible(true);
    dialog.dispose();


    Object selectedValue = op.getValue();

    if (selectedValue == null)
      return JOptionPane.CLOSED_OPTION;
    for (int i = 0; i < showResultsOptions.length; ++i)
      if (showResultsOptions[i] == selectedValue)
        return i;
    return JOptionPane.CLOSED_OPTION;
  }
}
