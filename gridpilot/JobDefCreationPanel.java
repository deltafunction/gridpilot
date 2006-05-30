package gridpilot;

import gridpilot.Debug;
import gridpilot.DatasetMgr;
import gridpilot.Database.DBRecord;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.text.*;

import java.util.*;

/**
 * This panel creates records in the DB table.
 * It is shown inside a CreateEditDialog.
 *
 */
public class JobDefCreationPanel extends CreateEditPanel{

  private static final long serialVersionUID = 1L;
  protected DatasetMgr datasetMgr;
  protected JPanel pCounter = new JPanel();
  protected JPanel pConstants = new JPanel();
  protected JScrollPane spAttributes = new JScrollPane();
  protected JPanel pButtons = new JPanel();
  protected String jobDefinitionID = "-1";
  protected Table table;
  protected JSpinner sFrom = new JSpinner(new SpinnerNumberModel(1, 1, 999999, 1));
  protected JSpinner sTo = new JSpinner(new SpinnerNumberModel(1, 1, 999999, 1));
  protected String dbName;
  protected DBPluginMgr dbPluginMgr = null;
  protected int datasetID = -1;
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
      /*this is in case DBPanel was opened from the menu and _datasetMgr is null*/
      String _dbName,
      DatasetMgr _datasetMgr,
      DBPanel _panel,
      Boolean bEditing){
    
    editing = bEditing.booleanValue();
    datasetMgr=_datasetMgr;
    dbName = _dbName;
    panel = _panel;
    table = panel.getTable();

    if(datasetMgr!=null){
      dbPluginMgr = datasetMgr.getDBPluginMgr();
      datasetID = datasetMgr.getDatasetID();
      datasetName = datasetMgr.getDatasetName();
    }
    else{
      datasetID = -1;
      dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
      datasetName = "";
    }

    jobDefIdentifier = dbPluginMgr.getIdentifierField("jobDefinition");
    cstAttributesNames = dbPluginMgr.getFieldNames("jobDefinition");
    Debug.debug("cstAttributesNames: "+Util.arrayToString(cstAttributesNames), 3);
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
      DBRecord jobDef = dbPluginMgr.getJobDefinition(Integer.parseInt(jobDefinitionID));
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

    sFrom.addChangeListener(new ChangeListener(){
      public void stateChanged(ChangeEvent e){
        if(((Integer)sTo.getValue()).intValue() < ((Integer)sFrom.getValue()).intValue())
          sTo.setValue(sFrom.getValue());
      }
    });

    sTo.addChangeListener(new ChangeListener(){

      public void stateChanged(ChangeEvent e){
        if(((Integer)sTo.getValue()).intValue() < ((Integer)sFrom.getValue()).intValue())
          sFrom.setValue(sTo.getValue());
      }
    });
  }

  public void initGUI(){

    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white,new Color(165, 163, 151)),datasetName));
    
    spAttributes.setPreferredSize(new Dimension(550, 500));
    spAttributes.setMinimumSize(new Dimension(550, 500));
    
    setLayout(new GridBagLayout());
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
    
    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.VERTICAL;
    ct.insets = new Insets(2,2,2,2);
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth=1;
    ct.gridheight=1;
    
    if(!editing){
      initArithmeticPanel();
      ct.gridx = 0;
      ct.gridy = 0;         
      ct.gridwidth=1;
      ct.gridheight=2;
      add(pButtons,ct);
      
      ct.gridx = 0;
      ct.gridy = 1;
      ct.gridwidth=2;
      ct.gridheight=1;
      add(pCounter,ct);

      ct.gridx = 0;
      ct.gridy = 2;
      ct.gridwidth=3;
      ct.gridheight=1;
      pConstants.setLayout(new GridBagLayout());
      pConstants.setMinimumSize(new Dimension(550, 50));
      add(pConstants,ct);    
            
      ct.gridx = 0;
      ct.gridy = 4;
      ct.gridwidth=3;
      add(spAttributes, ct);
    }
    else{
      ct.gridx = 0;
      ct.gridy = 0;
      ct.gridwidth=2;
      add(spAttributes,ct);
    }

    setValuesInAttributePanel(cstAttributesNames,
        cstAttr,
        tcCstAttributes);
    
    if(!editing){
      setEnabledAttributes(false,
          cstAttributesNames,
          tcCstAttributes);
    }
    
    updateUI();
    
  }

  protected void initArithmeticPanel(){

    // Panel counter
    pCounter.setLayout(new GridBagLayout());
    pCounter.removeAll();
    if(!reuseTextFields){
      sFrom.setValue(new Integer(1));
    }
    if(!reuseTextFields){
      sTo.setValue(new Integer(1));
    }
    pCounter.add(new JLabel("for i = "));
    pCounter.add(sFrom);
    pCounter.add(new JLabel("  to "));
    pCounter.add(sTo);


    // Panel Constants
    if(!reuseTextFields || tcConstant.size() == 0){
      pConstants.removeAll();
      tcConstant.removeAllElements();
      for(int i=0; i<4; ++i){
        addConstant();
      }
    }

    // panel Button
    JButton bLoad = new JButton("Load");
    JButton bSave = new JButton("Save");
    JButton bAddConstant = new JButton("New Constant");
    
    bLoad.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        load();
      }
    });

    bSave.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        save();
      }
    });

    bAddConstant.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        addConstant();
      }
    });

    pButtons.setLayout(new GridBagLayout());

    GridBagConstraints cb = new GridBagConstraints();
    cb.fill = GridBagConstraints.VERTICAL;
    cb.anchor = GridBagConstraints.NORTHWEST;
    cb.gridx = 0;
    cb.gridy = 0;         
    pButtons.add(bLoad, cb);
    bLoad.setEnabled(false);
    cb.gridx = 0;
    cb.gridy = 1;         
    pButtons.add(bSave, cb);
    bSave.setEnabled(false);
    cb.gridx = 0;
    cb.gridy = 2;         
    pButtons.add(bAddConstant, cb);
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
    new JobDefCreator(dbName,
                      datasetMgr,
                      ((Integer)(sFrom.getValue())).intValue(),
                      ((Integer)(sTo.getValue())).intValue(),
                      showResults,
                      tcConstant,
                      cstAttr,
                      cstAttributesNames,
                      editing
                      );

    panel.refresh();
    
  }

  protected void addConstant(){
    if(tcConstant.size() == 26)
      return;
    
    JTextField tf = new JTextField(CFIELDWIDTH);
    char name = (char) ('A' + (char)tcConstant.size());
    int cstByRow = 4;

    int row = tcConstant.size() / cstByRow;
    int col = tcConstant.size() % cstByRow;
    
    tcConstant.add(tf);
    
    GridBagConstraints cc = new GridBagConstraints();

    cc.gridx = col*2;
    cc.gridy = row;   
    pConstants.add(new JLabel("  " + new String(new char[]{name}) + " : "),cc);
    
    cc.gridx = col*2+1;
    cc.gridy = row;   
    pConstants.add(tf,cc);
    
    pConstants.updateUI();
  }

  protected boolean save(){
    Vector v = getTextFields();
    String [] values = new String[v.size()+1];

    values[0] = ""+tcConstant.size();

    for(int i=1; i<values.length; ++i){
      values[i] = ((JTextComponent) v.get(i-1)).getText();
      if(values[i].length() == 0)
        values[i] = " ";
    }
    String user = dbPluginMgr.getUserLabel();
    if(!dbPluginMgr.saveDefVals(datasetID, values, user)){
      Debug.debug("ERROR: Could not save values: "+values, 1);
      return false;
    }
    return true;
  }

  protected void load(){
    
    String user = dbPluginMgr.getUserLabel();

    String [] defValues = dbPluginMgr.getDefVals(datasetID, user);

    if(defValues ==null || defValues.length == 0)
      return;

    try{
      int nbConst = new Integer(defValues[0]).intValue();
      if(nbConst != tcConstant.size()){
        tcConstant.removeAllElements();
        pConstants.removeAll();
        for(int i=0; i<nbConst; ++i)
          addConstant();
      }
    }catch(NumberFormatException nfe){
      nfe.printStackTrace();
    }

    Enumeration tfs = getTextFields().elements();

    int i=1;
    while(tfs.hasMoreElements() && i<defValues.length){
      JTextComponent tf = (JTextComponent) tfs.nextElement();
      Util.setJText(tf, defValues[i].trim());
      ++i;
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
    
    GridBagConstraints cl = new GridBagConstraints();
    cl.fill = GridBagConstraints.VERTICAL;
    cl.gridx = 1;
    cl.gridy = 0;         
    cl.anchor = GridBagConstraints.NORTHWEST;
  
    pAttributes.setLayout(new GridBagLayout());
    pAttributes.removeAll();
      
    if(oldTcCstAttributes != null &&
        tcCstAttributes.length==oldTcCstAttributes.length){
      for(int i=0; i<tcCstAttributes.length; ++i){
        tcCstAttributes[i] = oldTcCstAttributes[i];
      }
    }
  
    for(int i =0; i<cstAttributesNames.length; ++i){
      
      if(cstAttributesNames[i].equalsIgnoreCase("ipConnectivity")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel("ipConnectivity" + " : "), cl);
        tcCstAttributes[i] = new JComboBox();
        ((JComboBox) tcCstAttributes[i]).addItem("yes");
        ((JComboBox) tcCstAttributes[i]).addItem("no");
        if(!editing){
          ((JComboBox) tcCstAttributes[i]).setSelectedItem("no");
        }
        if(editing || cstAttr[i]!=null){
          Util.setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("ramUnit")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel("ramUnit" + " : "), cl);
        tcCstAttributes[i] = new JComboBox();
        ((JComboBox) tcCstAttributes[i]).addItem("MB");
        ((JComboBox) tcCstAttributes[i]).addItem("GB");
        if(!editing){
          ((JComboBox) tcCstAttributes[i]).setSelectedItem("MB");
        }
        if(editing || cstAttr[i]!=null){
          Util.setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("diskUnit")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel("diskUnit" + " : "), cl);
        tcCstAttributes[i] = new JComboBox();
        ((JComboBox) tcCstAttributes[i]).addItem("MB");
        ((JComboBox) tcCstAttributes[i]).addItem("GB");
        if(!editing){
          ((JComboBox) tcCstAttributes[i]).setSelectedItem("GB");
        }
        if(editing || cstAttr[i]!=null){
          Util.setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("currentState")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
        
        Util.setJText(tcCstAttributes[i], cstAttr[i]);
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("taskFK")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
        
        Util.setJText(tcCstAttributes[i], Integer.toString(datasetID));
        tcCstAttributes[i].setEnabled(false);
      }
      else{
        cl.gridx=0;
        cl.gridy=i;
        if(cstAttributesNames[i].equalsIgnoreCase("jobPars") ||
            cstAttributesNames[i].equalsIgnoreCase("jobOutputs") ||
            cstAttributesNames[i].equalsIgnoreCase("jobLogs")){
          pAttributes.add(new JLabel(""), cl);
        }
        else{
          pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        }
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
        
        if(cstAttr[i]!=null && !cstAttr[i].equals("")){
          Debug.debug("Setting cstAttr["+i+"]: "+cstAttr[i], 3);
          Util.setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }      
      cl.gridx=1;
      cl.gridy=i;
      if( !cstAttributesNames[i].equalsIgnoreCase("jobPars") &&
          !cstAttributesNames[i].equalsIgnoreCase("jobOutputs") &&
          !cstAttributesNames[i].equalsIgnoreCase("jobLogs")){
        pAttributes.add(tcCstAttributes[i], cl);
      }
      if(cstAttributesNames[i].equalsIgnoreCase(jobDefIdentifier)){
        // when creating, zap loaded jobDefinitionID
        if(!editing){
          Util.setJText((JComponent) tcCstAttributes[i],"");
        }
        Util.setJEditable(tcCstAttributes[i], false);
      }
      else if( cstAttributesNames[i].equalsIgnoreCase("created") ||
          cstAttributesNames[i].equalsIgnoreCase("lastModified")){
        Util.setJEditable(tcCstAttributes[i], false);
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
                  datasetID==-1) &&
              tcCstAttributes[i]!=null){
        Util.setJEditable(tcCstAttributes[i], enabled);
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
        comp.getClass().isInstance(new JTextField())){
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
      Debug.debug("WARNING: unsupported component type "+comp.getClass().toString(), 1);
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
      Util.setJText((JComponent) textFields.get(i),"");
    }  
  }

  protected void setValuesInAttributePanel(String [] cstAttributesNames,
      String [] cstAttr, JComponent [] tcCstAttributes){
    Debug.debug("Setting values...", 3);
  }

}
