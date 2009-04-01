package gridpilot;

import gridfactory.common.DBRecord;
import gridfactory.common.Debug;

import javax.swing.*;
import javax.swing.border.*;
import java.awt.*;

import javax.swing.text.*;

import java.util.*;

/**
 * This panel creates records in the DB table.
 * It is shown inside a CreateEditDialog.
 *
 */
public class JobDefCreationPanel extends CreateEditPanel{

  private static final long serialVersionUID = 1L;
  protected JScrollPane spAttributes = new JScrollPane();
  protected String jobDefinitionID = "-1";
  protected MyJTable table;
  protected String dbName;
  protected DBPluginMgr dbPluginMgr = null;
  protected String datasetID = "-1";
  protected String datasetName;
  protected DBPanel panel = null;
  
  protected JPanel pAttributes = new JPanel();
  protected String [] cstAttributesNames;
  protected JComponent [] tcCstAttributes;
  protected boolean reuseTextFields = true;
  protected Vector tcConstant = new Vector(); // contains all text components
  protected String [] cstAttr = null;
  protected boolean editing = false;
  protected JPanel jobXmlContainer = new JPanel(new GridBagLayout());
  protected static JComponent [] oldTcCstAttributes;
  protected String jobDefIdentifier;
  
  protected static int CFIELDWIDTH = 8;
  protected static int TEXTFIELDWIDTH = 32;

  public JobDefCreationPanel(
      /*this is in case DBPanel was opened from the menu and _datasetID is null*/
      String _dbName,
      String _datasetID,
      DBPanel _panel,
      Boolean bEditing){
    
    editing = bEditing.booleanValue();
    dbName = _dbName;
    panel = _panel;
    table = panel.getTable();
    
    dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);

    if(_datasetID!=null && _datasetID.length()>0){
      datasetID = _datasetID;
      datasetName = dbPluginMgr.getDatasetName(datasetID);
    }
    else{
      datasetID = "-1";
      datasetName = "";
    }

    jobDefIdentifier = MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "jobDefinition");
    cstAttributesNames = dbPluginMgr.getFieldNames("jobDefinition");
    Debug.debug("cstAttributesNames: "+MyUtil.arrayToString(cstAttributesNames), 3);
    cstAttr = new String[cstAttributesNames.length];
        
    // When editing, fill cstAttr from db
    if(table.getSelectedRow()>-1 && editing){
      // Find jobdDefinitionID from db
      for(int i=0; i<table.getColumnNames().length; ++i){
        Object fieldVal = table.getUnsortedValueAt(table.getSelectedRow(),i);
        Debug.debug("Column name: "+table.getColumnNames().length+":"+i+" "+table.getColumnName(i), 3);
        if(fieldVal!=null && table.getColumnName(i).equalsIgnoreCase(jobDefIdentifier)){
          jobDefinitionID = fieldVal.toString();
          break;
        }
      }
      if(jobDefinitionID==null || jobDefinitionID.equals("-1")||
          jobDefinitionID.equals("")){
        Debug.debug("ERROR: could not find jobDefinitionID in table!", 1);
      }
      // Get job definition from db
      DBRecord jobDef = dbPluginMgr.getJobDefinition(jobDefinitionID);
      for(int i=0; i<cstAttributesNames.length; ++i){
        Debug.debug("filling " + cstAttributesNames[i],  3);
        if(jobDef.getValue(cstAttributesNames[i])!=null){
          cstAttr[i] = jobDef.getValue(cstAttributesNames[i]).toString();
        }
        else{
          cstAttr[i] = "";
        }
        Debug.debug("to " + cstAttr[i],  3);
      }
    }
  }

  public void initGUI(){

    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), datasetName));
    
    spAttributes.setPreferredSize(new Dimension(550, 500));
    spAttributes.setMinimumSize(new Dimension(550, 500));
    
    setLayout(new BorderLayout());
    removeAll();
    
    if(!reuseTextFields || tcCstAttributes == null ||
        tcCstAttributes.length != cstAttributesNames.length){
      Debug.debug("Creating new tcCstAttributes, "+
          tcCstAttributes+", "+(tcCstAttributes==null ? "":Integer.toString(tcCstAttributes.length)),
              3);
      tcCstAttributes = new JComponent[cstAttributesNames.length];
    }

    initAttributePanel(cstAttributesNames,
        cstAttr,
        tcCstAttributes,
        pAttributes,
        jobXmlContainer);
    spAttributes.getViewport().add(pAttributes, null);
    
    add(spAttributes);

    setValuesInAttributePanel(cstAttributesNames,
        cstAttr,
        tcCstAttributes);
    
    updateUI();
    
  }

  /**
   * Called when button Create is clicked
   */
  public void create(final boolean showResults, final boolean editing){

    Debug.debug("create",  1);
    
    for(int i=0; i< cstAttr.length; ++i){
      Debug.debug("setting " + cstAttributesNames[i],  3);
      cstAttr[i] = getJTextOrEmptyString(cstAttributesNames[i],
          tcCstAttributes[i], editing);
      Debug.debug("to " + cstAttr[i],  3);
    }

    oldTcCstAttributes = tcCstAttributes;     
    
    for(int i =0; i<cstAttr.length; ++i){
      if(cstAttributesNames[i].equals("jobXML")){
        if(cstAttr[i]==null || cstAttr[i].equals("null") || cstAttr[i].equals("")){
          cstAttr[i] = "";
        }
        else{
          if(!editing && cstAttr[i].indexOf("</jobDef>")<0 && cstAttr[i].indexOf("<jobDef>")<0){
            cstAttr[i] = "<jobDef>"+cstAttr[i]+"</jobDef>";
          }
        }
        break;
      }
    }
    
    Debug.debug("creating new JobDefCreator", 3);  
    JobDefCreator jdc = new JobDefCreator(dbName,
                      showResults,
                      tcConstant,
                      cstAttr,
                      cstAttributesNames,
                      editing
                      );

    if(jdc.anyCreated){
      panel.refresh();
    }
    
  }

  protected Vector getTextFields(){
    Vector v = new Vector();

    v.addAll(tcConstant);

    for(int i=0; i<tcCstAttributes.length; ++i)
      v.add(tcCstAttributes[i]);

    return v;
  }

  public void clearPanel(){
        clearPanel(
        cstAttributesNames,
        tcCstAttributes,
        jobXmlContainer,
        tcConstant
        );
  }


///////////////////////////////////////////////////////////////////////////////
  
  // TODO: this is copy-pasted from ProdDBJobDefCreationPanel and stripped down - fit HSQLDB and MySQL!
    
  protected void initAttributePanel(
      String [] cstAttributesNames,
      String [] cstAttr,
      JComponent [] tcCstAttributes,
      JPanel pAttributes,
      JPanel jobXmlContainer){
    
    GridBagConstraints cl = null;
  
    pAttributes.setLayout(new GridBagLayout());
    pAttributes.removeAll();
      
    if(oldTcCstAttributes != null &&
        tcCstAttributes.length==oldTcCstAttributes.length){
      for(int i=0; i<tcCstAttributes.length; ++i){
        tcCstAttributes[i] = oldTcCstAttributes[i];
      }
    }
  
    for(int i=0; i<cstAttributesNames.length; ++i){
      
      cl = new GridBagConstraints(0, i, 1, 1, 0.0, 0.0,
          GridBagConstraints.CENTER,
          GridBagConstraints.BOTH,
          new Insets(5, 25, 5, 5), 0, 0);
      
      if(cstAttributesNames[i].equalsIgnoreCase("ipConnectivity")){
        pAttributes.add(new JLabel("ipConnectivity" + " : "), cl);
        tcCstAttributes[i] = new JComboBox();
        ((JComboBox) tcCstAttributes[i]).addItem("yes");
        ((JComboBox) tcCstAttributes[i]).addItem("no");
        if(!editing){
          ((JComboBox) tcCstAttributes[i]).setSelectedItem("no");
        }
        if(editing || cstAttr[i]!=null){
          MyUtil.setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("ramUnit")){
        pAttributes.add(new JLabel("ramUnit" + " : "), cl);
        tcCstAttributes[i] = new JComboBox();
        ((JComboBox) tcCstAttributes[i]).addItem("MB");
        ((JComboBox) tcCstAttributes[i]).addItem("GB");
        if(!editing){
          ((JComboBox) tcCstAttributes[i]).setSelectedItem("MB");
        }
        if(editing || cstAttr[i]!=null){
          MyUtil.setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("diskUnit")){
        pAttributes.add(new JLabel("diskUnit" + " : "), cl);
        tcCstAttributes[i] = new JComboBox();
        ((JComboBox) tcCstAttributes[i]).addItem("MB");
        ((JComboBox) tcCstAttributes[i]).addItem("GB");
        if(!editing){
          ((JComboBox) tcCstAttributes[i]).setSelectedItem("GB");
        }
        if(editing || cstAttr[i]!=null){
          MyUtil.setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("currentState")){
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i]==null){
          tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
        }
        MyUtil.setJText(tcCstAttributes[i], cstAttr[i]);
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("taskFK")){
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i]==null){
          tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
        }
        MyUtil.setJText(tcCstAttributes[i], datasetID);
        tcCstAttributes[i].setEnabled(false);
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("outFileMapping") ||
          cstAttributesNames[i].equalsIgnoreCase("metaData") ||
          cstAttributesNames[i].equalsIgnoreCase("validationResult")){
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i]==null){
          tcCstAttributes[i] = MyUtil.createTextArea(TEXTFIELDWIDTH);
        }
        MyUtil.setJText(tcCstAttributes[i], cstAttr[i]);
      }
      else{
        if(cstAttributesNames[i].equalsIgnoreCase("jobPars") ||
            cstAttributesNames[i].equalsIgnoreCase("jobOutputs") ||
            cstAttributesNames[i].equalsIgnoreCase("jobLogs")){
          pAttributes.add(new JLabel(""), cl);
        }
        else{
          pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        }
        if(!reuseTextFields || tcCstAttributes[i]==null){
          tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
        }
        if(cstAttr[i]!=null && !cstAttr[i].equals("")){
          Debug.debug("Setting cstAttr["+i+"]: "+cstAttr[i], 3);
          MyUtil.setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }      
      cl = new GridBagConstraints(
          1, i/*row*/, 3, 1, 1.0, 0.0,
          GridBagConstraints.CENTER,
          GridBagConstraints.HORIZONTAL,
          new Insets(5, 5, 5, 5), 0, 0);
      if( !cstAttributesNames[i].equalsIgnoreCase("jobPars") &&
          !cstAttributesNames[i].equalsIgnoreCase("jobOutputs") &&
          !cstAttributesNames[i].equalsIgnoreCase("jobLogs")){
        pAttributes.add(tcCstAttributes[i], cl);
      }
      if(cstAttributesNames[i].equalsIgnoreCase(jobDefIdentifier)){
        // when creating, zap loaded jobDefinitionID
        if(!editing){
          MyUtil.setJText((JComponent) tcCstAttributes[i],"");
        }
        MyUtil.setJEditable(tcCstAttributes[i], false);
      }
      else if( cstAttributesNames[i].equalsIgnoreCase("created") ||
          cstAttributesNames[i].equalsIgnoreCase("lastModified") ||
          cstAttributesNames[i].equalsIgnoreCase("outTmp") ||
          cstAttributesNames[i].equalsIgnoreCase("errtmp") ||
          cstAttributesNames[i].equalsIgnoreCase("validationResult") ||
          cstAttributesNames[i].equalsIgnoreCase("jobID") ||
          cstAttributesNames[i].equalsIgnoreCase("host") ||
          //cstAttributesNames[i].equalsIgnoreCase("csStatus") ||
          cstAttributesNames[i].equalsIgnoreCase("computingSystem")){
        MyUtil.setJEditable(tcCstAttributes[i], false);
      }
      // TODO: disable also fields filled out by GridPilot and runtime fields
    }
  }

  protected void setEnabledAttributes(boolean enabled,
      String [] cstAttributesNames,
      JComponent [] tcCstAttributes){
    
    if(cstAttributesNames.length!=tcCstAttributes.length){
      Debug.debug(cstAttributesNames.length+"!="+tcCstAttributes.length, 1);
      return;
    }
    Debug.debug(cstAttributesNames.length+"=="+tcCstAttributes.length, 3);
    
    for(int i =0; i<cstAttributesNames.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase("jobXML")){
      }
      else if(!cstAttributesNames[i].equalsIgnoreCase("jobTransFK") &&
              !cstAttributesNames[i].equalsIgnoreCase(jobDefIdentifier) &&
              (!cstAttributesNames[i].equalsIgnoreCase("taskFK") ||
                  datasetID.equals("-1")) &&
              tcCstAttributes[i]!=null){
        MyUtil.setJEditable(tcCstAttributes[i], enabled);
      }
    }

    // the create/update button on the CreateEditDialog panel
    ((JButton) ((JPanel) getParent().getComponent(1)).getComponent(3)).setEnabled(enabled);
    updateUI();
  }

  protected Vector getNonAutomaticFields(String [] cstAttributesNames,
      JComponent [] tcCstAttributes, Vector tcConstant){
    
    Vector v = new Vector();
    v.addAll(tcConstant);
    for(int i=0; i<tcCstAttributes.length; ++i){
      if(!cstAttributesNames[i].equalsIgnoreCase(jobDefIdentifier) &&
          !cstAttributesNames[i].equalsIgnoreCase("jobTransFK") &&
          !cstAttributesNames[i].equalsIgnoreCase("taskFK")){
        v.add(tcCstAttributes[i]);
      }
    }
    return v;
  }

  protected String getJTextOrEmptyString(String attr, JComponent comp,
     boolean editing){
    if(comp==null){
      Debug.debug("WARNING: JComponent is null", 3);
      return "";
    }
    
    String text = "";
    if(comp.getClass().isInstance(new JTextArea())||
        comp.getClass().isInstance(new JTextField())||
        comp.getClass().isInstance(MyUtil.createTextArea(TEXTFIELDWIDTH))){
      text =  ((JTextComponent) comp).getText();
    }
    else if(comp.getClass().isInstance(new JComboBox())){
      if(((JComboBox) comp).getSelectedItem()==null){
        text = "";
      }
      else{
        text = ((JComboBox) comp).getSelectedItem().toString();
      }
    }
    else{
      Debug.debug("WARNING: unsupported component type "+comp.getClass().getCanonicalName(), 1);
    }
    return text;
  }

  public void clearPanel(String [] cstAttributesNames,
      JComponent [] tcCstAttributes,
      JPanel jobXmlContainer,
      Vector tcConstant){
      
    Vector textFields = getNonAutomaticFields(cstAttributesNames,
        tcCstAttributes, tcConstant);
  
    for(int i =0; i<textFields.size(); ++i){
      MyUtil.setJText((JComponent) textFields.get(i),"");
    }  
  }

  protected void setValuesInAttributePanel(String [] cstAttributesNames,
      String [] cstAttr, JComponent [] tcCstAttributes){
    Debug.debug("Setting values...", 3);
  }

}
