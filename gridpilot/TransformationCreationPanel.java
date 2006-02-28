package gridpilot;

import gridpilot.Debug;
import gridpilot.Database.DBRecord;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.text.*;

import java.util.*;

/**
 * This panel creates records in the DB table. It's shown inside the CreateEditDialog.
 *
 */


public class TransformationCreationPanel extends CreateEditPanel{

  private JPanel pCounter = new JPanel();
  private JPanel pConstants = new JPanel();
  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private JPanel pButtons = new JPanel();
  private String transformationID = "-1";
  private Table table;
  private JSpinner sFrom = new JSpinner(new SpinnerNumberModel(1, 1, 999999, 1));
  private JSpinner sTo = new JSpinner(new SpinnerNumberModel(1, 1, 999999, 1));
  private String [] cstAttributesNames;
  private JComponent [] tcCstAttributes;
  private JComponent [] tcCstJobDefAttributes;
  private boolean reuseTextFields = true;
  private Vector tcConstant = new Vector(); // contains all text components
  private String [] cstAttr = null;
  private boolean editing = false;
  private boolean loaded = false;
  private DBPluginMgr dbPluginMgr = null;
  private String transformationIdentifier;
 
  private static JComponent [] oldTcCstAttributes;
  private static int TEXTFIELDWIDTH = 32;
  private static int CFIELDWIDTH = 8;
  
  
  /**
   * Constructor
   */

  public TransformationCreationPanel(DBPluginMgr _dbPluginMgr, Table _table, boolean _editing){
    
    editing = _editing;
    table = _table;
    dbPluginMgr = _dbPluginMgr;

    transformationIdentifier = "identifier";

    //cstAttributesNames = JobTrans.Fields;
    cstAttributesNames = dbPluginMgr.getFieldNames("transformation");
    cstAttr = new String[cstAttributesNames.length];
    
    // Find transformation ID from table
    if(table.getSelectedRow()>-1 && editing){
      for(int i=0; i<table.getColumnNames().length; ++i){
        Object fieldVal = table.getUnsortedValueAt(table.getSelectedRow(),i);
        Debug.debug("Column name: "+table.getColumnNames().length+":"+i+" "+table.getColumnName(i), 3);
        if(fieldVal!=null && table.getColumnName(i).equalsIgnoreCase(transformationIdentifier)){
          transformationID = fieldVal.toString();
          break;
        }
      }
      if(transformationID==null || transformationID.equals("-1") ||
          transformationID.equals("")){
        Debug.debug("ERROR: could not find transformationID in table!", 1);
      }
      // Fill cstAttr from db
      DBRecord transformation = dbPluginMgr.getTransformation(Integer.parseInt(transformationID));
      for(int i=0; i < cstAttributesNames.length; ++i){
        if(editing){
          Debug.debug("filling " + cstAttributesNames[i],  3);
          if(transformation.getValue(cstAttributesNames[i])!=null){
            cstAttr[i] = transformation.getValue(cstAttributesNames[i]).toString();
          }
          else{
            cstAttr[i] = "";
          }
          
          Debug.debug("to " + cstAttr[i],  3);
        }
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

  /**
   * GUI initialisation
   */

  public void initGUI(){

    /*String homePackage = "";
    if(dbPluginMgr.getJobTransRecord(Integer.parseInt(transformationID))!=null &&
        dbPluginMgr.getJobTransRecord(Integer.parseInt(transformationID)).getValue("homePackage")!=null){
      dbPluginMgr.getJobTransRecord(Integer.parseInt(transformationID)).getValue("homePackage").toString();
    }*/
    
    
    
    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white,new Color(165, 163, 151)), "transformation "+transformationID));
    
    spAttributes.setPreferredSize(new Dimension(550, 500));
    spAttributes.setMinimumSize(new Dimension(550, 500));
    
    setLayout(new GridBagLayout());
    removeAll();

    initAttributePanel();
    
    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.VERTICAL;
    ct.insets = new Insets(2,2,2,2);
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth=1;
    ct.gridheight=1;
    
    if(!editing){
      initArithmeticPanel();
      //ct.gridx = 0;
      //ct.gridy = 0;         
      //add(cbTaskTransSelection,ct);
      
      ct.gridx = 2;
      ct.gridy = 0;
      ct.gridwidth=1;
      ct.gridheight=1;
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
      //ct.gridx = 0;
      //ct.gridy = 0;
      //add(cbTaskTransSelection,ct);
      ct.gridx = 0;
      ct.gridy = 1;
      ct.gridwidth=2;
      add(spAttributes,ct);
    }

    setValuesInAttributePanel();
    
    if(!editing){
      setEnabledAttributes(false);
    }
    
    updateUI();
    
    loaded = true;
    
    }

  private void initArithmeticPanel(){

    /**
     * Called when version is selected in combo box cbTaskTransSelection
     *
     * Initialises text fields with attributes
     */

    // Panel counter

    pCounter.setLayout(new GridBagLayout());

    pCounter.removeAll();

    if(!reuseTextFields)
      sFrom.setValue(new Integer(1));
    if(!reuseTextFields)
      sTo.setValue(new Integer(1));

    pCounter.add(new JLabel("for i = "));
    pCounter.add(sFrom);

    pCounter.add(new JLabel("  to "));

    pCounter.add(sTo);


// Panel Constants

    if(!reuseTextFields || tcConstant.size() == 0){
      pConstants.removeAll();
      tcConstant.removeAllElements();

      for(int i=0; i<4; ++i)
        addConstant();
      
    }

// panel Button

    JButton bAddConstant = new JButton("New Constant");

    bAddConstant.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        addConstant();
      }
    });

    pButtons.setLayout(new GridBagLayout());

    GridBagConstraints cb = new GridBagConstraints();
    cb.fill = GridBagConstraints.VERTICAL;
    cb.anchor = GridBagConstraints.NORTHWEST;
    cb.gridx = 3;
    cb.gridy = 0;         
    pButtons.add(bAddConstant, cb);
  }


  private void initAttributePanel(){
    
    GridBagConstraints cl = new GridBagConstraints();
    cl.fill = GridBagConstraints.VERTICAL;
    cl.gridx = 1;
    cl.gridy = 0;         
    cl.anchor = GridBagConstraints.NORTHWEST;

    pAttributes.setLayout(new GridBagLayout());
    pAttributes.removeAll();

    spAttributes.getViewport().add(pAttributes, cl);
    
    if(oldTcCstAttributes != null){
      tcCstAttributes = oldTcCstAttributes;
      // when creating, zap loaded transformationID
      /*if(!editing){
        for(int i =0; i<tcCstAttributes.length; ++i){
          if(cstAttributesNames[i].equalsIgnoreCase("transformationID")){
            setJText((JComponent) tcCstAttributes[i],"");
            ((JComponent) tcCstAttributes[i]).setEnabled(false);
          }
        }
      }*/
    }

    if(!reuseTextFields || tcCstAttributes == null ||
        tcCstAttributes.length != cstAttributesNames.length){
      Debug.debug("Creating new tcCstAttributes, "+
          tcCstAttributes+", "+(tcCstAttributes==null ? "":Integer.toString(tcCstAttributes.length)),
              3);
      tcCstAttributes = new JComponent[cstAttributesNames.length];
    }
    
    for(int i =0; i<cstAttributesNames.length; ++i){
      
      if(cstAttributesNames[i].equalsIgnoreCase("taskTransFK")){
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel("taskTransFK" + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = createTextComponent(TEXTFIELDWIDTH);
        
        ((JTextComponent) tcCstAttributes[i]).setEnabled(false);
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("formalPars")){
        cl.gridx=0;
        cl.gridy=i;
        JTextArea textArea = new JTextArea(10, TEXTFIELDWIDTH);
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
        textArea.setEditable(true);
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = textArea;
        
        setJText(tcCstAttributes[i], cstAttr[i]);
      }
      else{
        cl.gridx=0;
        cl.gridy=i;
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
        if(!reuseTextFields || tcCstAttributes[i] == null)
          tcCstAttributes[i] = createTextComponent(TEXTFIELDWIDTH);
        
        if(cstAttr[i]!=null && !cstAttr[i].equals("")){
          Debug.debug("Setting cstAttr["+i+"]: "+cstAttr[i], 3);
          setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }      
      cl.gridx=1;
      cl.gridy=i;
      //if(!cstAttributesNames[i].equalsIgnoreCase("formalPars")){
        pAttributes.add(tcCstAttributes[i], cl);
      //}
      if(cstAttributesNames[i].equalsIgnoreCase(transformationIdentifier)){
        // when creating, zap loaded transformationID
        if(!editing){
          setJText((JComponent) tcCstAttributes[i],"");
        }
        ((JTextComponent) tcCstAttributes[i]).setEnabled(false);
      }
    }
  }

  private void setEnabledAttributes(boolean enabled){
    for(int i =0; i<cstAttributesNames.length; ++i){
      if(!cstAttributesNames[i].equalsIgnoreCase("taskTransFK") &&
              !cstAttributesNames[i].equalsIgnoreCase(transformationIdentifier)){
        tcCstAttributes[i].setEnabled(enabled);
      }
    }
  }

  private void setValuesInAttributePanel(){
    
    Debug.debug("Setting values...", 3);

    for(int i =0; i<cstAttributesNames.length; ++i){     
      if(cstAttributesNames[i].equalsIgnoreCase("jobXML") /*&& editing*/){
          tcCstAttributes[i].removeAll();
          GridBagConstraints cv = new GridBagConstraints();
          cv.ipady = 10;
          cv.weighty = 0.5;
          cv.anchor = GridBagConstraints.NORTHWEST;
          cv.fill = GridBagConstraints.VERTICAL;
          cv.weightx = 0.5;
          cv.gridx = 0;
          cv.gridy = 0;
      }
      else{
      }
    }
  }
  
   /**
   * public methods
   */

  public void clear(){
    /**
     * Called by : JobDefinition.button_ActionPerformed()
     */

    Vector textFields = getNonIdTextFields();

    for(int i =0; i<textFields.size(); ++i)
      setJText((JComponent) textFields.get(i),"");
    
    GridBagConstraints cv = new GridBagConstraints();
    cv.fill = GridBagConstraints.VERTICAL;
    cv.weightx = 0.5;
    cv.gridx = 0;
    cv.gridy = 0;         
    cv.ipady = 10;
    cv.weighty = 0.5;
    cv.anchor = GridBagConstraints.NORTHWEST;
    cv.gridx = 0;
    cv.gridy = 1;
    updateUI();
  }


  public void create(final boolean showResults, final boolean editing) {
    /**
     * Called when button Create is clicked in JobTrans
     */

    Debug.debug("create",  1);
    
    for(int i=0; i< cstAttr.length; ++i){
      Debug.debug("setting " + cstAttributesNames[i],  3);
      cstAttr[i] = getJTextOrEmptyString(tcCstAttributes[i], editing);
      Debug.debug("to " + cstAttr[i],  3);
    }

    oldTcCstAttributes = tcCstAttributes;
  
    Debug.debug("creating new TransformationCreator",  3);
    
    new TransformationCreator( dbPluginMgr,
                         ((Integer)(sFrom.getValue())).intValue(),
                         ((Integer)(sTo.getValue())).intValue(),
                         showResults,
                         tcConstant,
                         cstAttr,
                         cstAttributesNames,
                         editing
                         );

  }

  /**
   * Private methods
   */

  private void addConstant(){
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


  private Vector getTextFields(){
    Vector v = new Vector();

    v.addAll(tcConstant);

    for(int i=0; i<tcCstAttributes.length; ++i)
      v.add(tcCstAttributes[i]);

    return v;
  }

  private Vector getNonIdTextFields(){
    Vector v = new Vector();

    v.addAll(tcConstant);

    for(int i=0; i<tcCstAttributes.length; ++i){
      if(!cstAttributesNames[i].equalsIgnoreCase(transformationIdentifier)){
        v.add(tcCstAttributes[i]);
      }
    }

    return v;
  }

  private JTextComponent createTextComponent(){
    JTextArea ta = new JTextArea();
    ta.setBorder(new JTextField().getBorder());
    ta.setWrapStyleWord(true);
    ta.setLineWrap(true);
    return ta;
  }
  
  private JTextComponent createTextComponent(int cols){
    JTextField tf = new JTextField("", cols);
    return tf;
  }
  
  private JTextComponent createTextComponent(String str){
    int length;
    if(str.length()>10){
      length = str.length()-5;
    }
    else{
      length = 6;
    }
    JTextArea ta = new JTextArea(str, 1, length);
    ta.setBorder(new JTextField().getBorder());
    ta.setWrapStyleWord(true);
    ta.setLineWrap(true);
    return ta;
  }
  
  private static String getJTextOrEmptyString(JComponent comp, boolean editing){
    String name = "";
    String label = "";
    String text = "";
    String [] ses;
    JComponent com;
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
  
  private static String setJText(JComponent comp, String text){
    if(comp.getClass().isInstance(new JTextArea()) ||
        comp.getClass().isInstance(new JTextField())){
      ((JTextComponent) comp).setText(text);
    }
    else if(comp.getClass().isInstance(new JComboBox())){
      ((JComboBox) comp).setSelectedItem(text);
    }
    return text;
  }
}
