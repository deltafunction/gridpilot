package gridpilot;

//import gridpilot.DatasetMgr;
import java.util.*;
import javax.swing.*;
import java.awt.*;

import gridpilot.dbplugins.proddb.ProdDBXmlNode;

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
  private Object[] showResultsOptions = {"OK", "Skip"};

  public JobDefCreator(String _dbName,
                       //DatasetMgr _datasetMgr,
                       boolean _showResults,
                       Vector _constants,
                       String [] _cstAttr,
                       String [] _cstAttrNames,
                       boolean _editing
                       ){

    //datasetMgr = _datasetMgr;
    dbName = _dbName;
    
    /*if(datasetMgr!=null){
      dbPluginMgr = datasetMgr.getDBPluginMgr();
    }
    else{
      dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    }*/
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
      int choice = showResult(cstAttr);
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
      String jobDefIdentifier = dbPluginMgr.getIdentifierField("jobDefinition");
      int id = -1;
      for(int i=0; i<cstAttrNames.length; ++i){
        if(cstAttrNames[i].toString().equalsIgnoreCase(
            jobDefIdentifier)){
          id = Integer.parseInt(cstAttr[i]);
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
    }
    else{
      Debug.debug("Creating..."+cstAttrNames.length+" : "+cstAttr.length, 3);
      try{
        dbPluginMgr.createJobDef(cstAttrNames, cstAttr);
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
  
  private int showResult(String [] resCstAttr){

    JPanel pResult = new JPanel(new GridBagLayout());
    int row = 0;
    ProdDBXmlNode xmlNode = null;

    for(int i=0; i<cstAttr.length; ++i, ++row){
      pResult.add(new JLabel(cstAttrNames[i] + " : "),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
              GridBagConstraints.CENTER, GridBagConstraints.BOTH,
              new Insets(5, 25, 5, 5), 0, 0));
      JComponent jval = null;
      JTextArea textArea = null;
      if(cstAttrNames[i].equalsIgnoreCase("jobXML") ||
          cstAttrNames[i].equalsIgnoreCase("jobPars") ||
         cstAttrNames[i].equalsIgnoreCase("jobOutputs") ||
         cstAttrNames[i].equalsIgnoreCase("jobLogs") ||
         cstAttrNames[i].equalsIgnoreCase("jobInputs")){
        try{
          // Just give it a try with the proddb schema...
          if(resCstAttr[i]!=null && !resCstAttr[i].equals("null") &&
              !resCstAttr[i].equals("")){
            xmlNode = ProdDBXmlNode.parseString(resCstAttr[i], 0);
            xmlNode.fillText();
          }
          textArea = new JTextArea(xmlNode.parsedText);
        }
        catch(Exception e){
          Debug.debug("Could not parse XML. "+e.getMessage(), 2);
          e.printStackTrace();
          // If it doesn't work, show raw XML
          textArea = new JTextArea(resCstAttr[i]);
        }
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
        textArea.setEditable(false);
        jval = textArea;
      }
      else{
        jval = new JLabel(resCstAttr[i]);
      }
      Debug.debug("setting "+cstAttrNames[i]+"->"+resCstAttr[i], 3);
      pResult.add(jval,
          new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
              GridBagConstraints.CENTER,
              GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

    JScrollPane sp = new JScrollPane(pResult);
    int height = (int)pResult.getPreferredSize().getHeight() +
    (int)sp.getHorizontalScrollBar().getPreferredSize().getHeight() + 5;
    int width = (int)pResult.getPreferredSize().getWidth() +
    (int)sp.getVerticalScrollBar().getPreferredSize().getWidth() + 5;
    Dimension screenSize = new Dimension(Toolkit.getDefaultToolkit().getScreenSize());
    if(height>screenSize.height){
      height = 700;
      Debug.debug("Screen height exceeded, setting "+height, 2);
    }
    if(width>screenSize.width){
      width = 550;
      Debug.debug("Screen width exceeded, setting "+width, 2);
    }
    Debug.debug("Setting size "+width+":"+height, 3);
    sp.setPreferredSize(new Dimension(width, height));

    JOptionPane op = new JOptionPane(sp,
                                     JOptionPane.QUESTION_MESSAGE,
                                     JOptionPane.YES_NO_CANCEL_OPTION,
                                     null,
                                     showResultsOptions,
                                     showResultsOptions[0]);
    

    JDialog dialog = op.createDialog(JOptionPane.getRootFrame(), "JobDef");
    
    dialog.requestFocusInWindow();
    dialog.setResizable(true);
    dialog.setVisible(true);
    dialog.dispose();


    Object selectedValue = op.getValue();

    if(selectedValue==null){
      return JOptionPane.CLOSED_OPTION;
    }
    for(int i=0; i<showResultsOptions.length; ++i){
      if(showResultsOptions[i]==selectedValue){
        return i;
      }
    }
    return JOptionPane.CLOSED_OPTION;
  }
}
