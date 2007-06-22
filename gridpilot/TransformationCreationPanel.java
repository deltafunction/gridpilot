package gridpilot;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.text.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.util.*;

/**
 * This panel creates records in the DB table.
 * It's shown inside the CreateEditDialog.
 */
public class TransformationCreationPanel extends CreateEditPanel{

  private DBPluginMgr dbPluginMgr;
  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private boolean editing = false;
  private Table table;
  private String transformationID = "-1";
  private String [] cstAttributesNames;
  private String [] cstAttr = null;
  private String transformationIdentifier;
  private boolean reuseTextFields = true;
  private Vector tcConstant = new Vector(); // contains all text components
  private DBPanel panel = null;
  private JPanel pRuntimeEnvironment = new JPanel();
  private String runtimeEnvironmentName = null;
  private JComboBox cbRuntimeEnvironmentSelection;
  private GridBagConstraints ct = new GridBagConstraints();
  private DBRecord transformation = null;
  private DBResult runtimeEnvironments = null;
  private String [] transformationFields = null;
  private JButton jbEditTrans = new JButton("view");

  private static final long serialVersionUID = 1L;
  private static int TEXTFIELDWIDTH = 32;

  public JTextComponent [] tcCstAttributes;

  /**
   * Constructor
   */
  public TransformationCreationPanel(DBPluginMgr _dbPluginMgr,
      DBPanel _panel, boolean _editing){
    dbPluginMgr = _dbPluginMgr;
    editing = _editing;
    panel = _panel;
    table = panel.getTable();
    transformationIdentifier =
      Util.getIdentifierField(dbPluginMgr.getDBName(), "transformation");
    transformationFields = dbPluginMgr.getFieldNames("transformation");
    cstAttributesNames = dbPluginMgr.getFieldNames("transformation");    
    runtimeEnvironments = dbPluginMgr.getRuntimeEnvironments();
    Debug.debug("Got field names: "+Util.arrayToString(cstAttributesNames),3);
    Debug.debug("Number of runtimeEnvironments found: "+runtimeEnvironments.values.length+
        "; "+Util.arrayToString(runtimeEnvironments.fields),3);
    cstAttr = new String[cstAttributesNames.length];
    // Find transformation ID from table
    if(table.getSelectedRow()>-1 && editing){
      Debug.debug("Editing...", 3);
      String [] runtimeReference = Util.getTransformationRuntimeReference(dbPluginMgr.getDBName());
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
      transformation = dbPluginMgr.getTransformation(transformationID);
      for(int i=0; i<cstAttributesNames.length; ++i){
        if(editing){
          Debug.debug("filling " + cstAttributesNames[i],  3);
          if(transformation.getValue(cstAttributesNames[i])!=null){
            cstAttr[i] = transformation.getValue(cstAttributesNames[i]).toString();
            if(cstAttributesNames[i].equalsIgnoreCase(runtimeReference[1])){
              runtimeEnvironmentName = cstAttr[i];
            }
          }
          else{
            cstAttr[i] = "";
          }
          Debug.debug("to " + cstAttr[i],  3);
        }
      }
    }
  }

  /**
   * GUI initialisation
   */

  public void initGUI(){
    
    Debug.debug("Initializing GUI", 3);

    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white,new Color(165, 163, 151)), 
        (transformationID.equals("-1")?"new transformation":"transformation "+transformationID)));
    
    //spAttributes.setPreferredSize(new Dimension(650, 500));
    //spAttributes.setMinimumSize(new Dimension(650, 500));
    
    setLayout(new GridBagLayout());
    removeAll();

    initRuntimeEnvironmentPanel(Integer.parseInt(transformationID));

    //initAttributePanel();
    
    ct.fill = GridBagConstraints.VERTICAL;
    ct.insets = new Insets(2,2,2,2);
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth=1;
    ct.gridheight=1;
    
    ct.gridx = 0;
    ct.gridy = 1;
    ct.gridwidth=2;
    add(spAttributes,ct);

    Debug.debug("Initializing panel", 3);
    initTransformationCreationPanel();
    if(editing){
      Debug.debug("Editing...", 3);
      editTransformation(Integer.parseInt(transformationID), runtimeEnvironmentName);
    }
    else{
      // Disable identifier field when creating
      Debug.debug("Disabling identifier field", 3);
      for(int i =0; i<cstAttributesNames.length; ++i){
        if(cstAttributesNames[i].equalsIgnoreCase(transformationIdentifier) ||
            cstAttributesNames[i].equalsIgnoreCase("created") ||
            cstAttributesNames[i].equalsIgnoreCase("lastModified")){
          Util.setJEditable(tcCstAttributes[i], false);
        }
        else if(runtimeEnvironmentName!=null && !runtimeEnvironmentName.equals("") &&
            cstAttributesNames[i].equalsIgnoreCase("runtimeEnvironmentName")){
          Util.setJText(tcCstAttributes[i], runtimeEnvironmentName);
        }
      }
    }
    updateUI();
   }


  /**
   * Called initially.
   * Initialises text fields with attributes for transformation.
    */
  private void initTransformationCreationPanel(){

    // Panel Attributes
    pAttributes.setLayout(new GridBagLayout());
    pAttributes.removeAll();

    spAttributes.getViewport().add(pAttributes, null);

    add(spAttributes,
        new GridBagConstraints(0, 3, 3, 1, 0.9, 0.9,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

    initAttributePanel();
    
  }
  
  private String[] getRuntimeEnvironmentNames(){
    String [] ret = new String[runtimeEnvironments.values.length];
    for(int i=0; i<runtimeEnvironments.values.length; ++i){
      ret[i] = runtimeEnvironments.getValue(i, "name").toString(); 
      Debug.debug("name is "+ret[i], 3);
    }
    // This is to ensure only unique elements
    // TODO: for some reason this doesn't seam to work
    Arrays.sort(ret);
    Vector vec = new Vector();
    if(runtimeEnvironments.values.length>0){
      vec.add(ret[0]);
    }
    if(runtimeEnvironments.values.length>1){
      for(int i=1; i<runtimeEnvironments.values.length; ++i){
        //Debug.debug("Comparing "+ret[i]+" <-> "+ret[i-1],3);
        if(!ret[i].equalsIgnoreCase(ret[i-1])){
          Debug.debug("Adding "+ret[i],3);
            vec.add(ret[i]);
        }
      }
    }
    String[] arr = new String[vec.size()];
    for(int i=0; i<vec.size(); ++i){
      arr[i]=vec.elementAt(i).toString();
    } 
    return arr;
  }

  private void initRuntimeEnvironmentPanel(int datasetID){
    
    pRuntimeEnvironment.removeAll();
    pRuntimeEnvironment.setLayout(new FlowLayout());

    String [] runtimeEnvironmentNames = getRuntimeEnvironmentNames();

    if(runtimeEnvironmentNames.length==0){
      pRuntimeEnvironment.add(new JLabel("No runtime environments found."));
      jbEditTrans.setEnabled(false);
    }
    else if(runtimeEnvironmentNames.length==1){
      runtimeEnvironmentName = runtimeEnvironmentNames[0];
      pRuntimeEnvironment.add(new JLabel("Runtime environment : " + runtimeEnvironmentName));
    }
    else{
      cbRuntimeEnvironmentSelection = new JComboBox();
      for(int i=0; i<runtimeEnvironmentNames.length; ++i){
          cbRuntimeEnvironmentSelection.addItem(runtimeEnvironmentNames[i]);
      }
      pRuntimeEnvironment.add(new JLabel("Runtime environment: "), null);
      pRuntimeEnvironment.add(cbRuntimeEnvironmentSelection, null);

      cbRuntimeEnvironmentSelection.addActionListener(
        new java.awt.event.ActionListener(){
          public void actionPerformed(java.awt.event.ActionEvent e){
            cbRuntimeSelection_actionPerformed();
          }
        }
      ); 
    }
    ct.gridx = 0;
    ct.gridy = 0;
    ct.gridwidth=1;
    ct.gridheight=1;
    add(pRuntimeEnvironment, ct);
    
    jbEditTrans.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        viewRuntimeEnvironments();
      }
    }
    );
    ct.gridx = 1;
    ct.gridy = 0;
    ct.gridwidth=1;
    ct.gridheight=1;
    add(jbEditTrans, ct);

    updateUI();
  }

  /**
   * Open new pane with corresponding runtime environments.
   */
  private void viewRuntimeEnvironments(){
    if(runtimeEnvironmentName==null || runtimeEnvironmentName.equals("")){
      return;
    }
    GridPilot.getClassMgr().getGlobalFrame().requestFocusInWindow();
    GridPilot.getClassMgr().getGlobalFrame().setVisible(true);
    Thread t = new Thread(){
      public void run(){
        try{
          // Create new panel with jobDefinitions.         
          DBPanel dbPanel = new DBPanel(dbPluginMgr.getDBName(),
              "runtimeEnvironment");
          String nameField =
            Util.getNameField(dbPluginMgr.getDBName(), "runtimeEnvironment");
          dbPanel.selectPanel.setConstraint(nameField,
              runtimeEnvironmentName, 0);
          dbPanel.searchRequest(true, false);           
          GridPilot.getClassMgr().getGlobalFrame().addPanel(dbPanel);
        }
        catch(Exception e){
          Debug.debug("Couldn't create panel for dataset " + "\n" +
                             "\tException\t : " + e.getMessage(), 2);
          e.printStackTrace();
        }
      }
    };
    //SwingUtilities.invokeLater(t);
    t.run();
  }
  
  private void cbRuntimeSelection_actionPerformed(){
    if(cbRuntimeEnvironmentSelection.getSelectedItem()==null){
        return;
    }
    else{
        runtimeEnvironmentName = cbRuntimeEnvironmentSelection.getSelectedItem().toString();
    }
    editTransformation(Integer.parseInt(transformationID),
        runtimeEnvironmentName);
  }
  
  private void initAttributePanel(){
    
    if(!reuseTextFields || tcCstAttributes==null ||
        tcCstAttributes.length!=cstAttributesNames.length){
      tcCstAttributes = new JTextComponent[cstAttributesNames.length];
    }
    int row = 0;
    for(int i=0; i<cstAttributesNames.length; ++i, ++row){
      if(cstAttributesNames[i].equalsIgnoreCase("initLines") ||
          cstAttributesNames[i].equalsIgnoreCase("comment")){
        if(!reuseTextFields || tcCstAttributes[i]==null){
          tcCstAttributes[i] = Util.createTextArea(TEXTFIELDWIDTH);
        }
      }
      else{
        if(!reuseTextFields || tcCstAttributes[i]==null || !tcCstAttributes[i].isEnabled()){
          tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
        }
      }
      if(cstAttributesNames[i].equalsIgnoreCase("definition") ||
         cstAttributesNames[i].equalsIgnoreCase("script") ||
         cstAttributesNames[i].equalsIgnoreCase("validationScript") ||
         cstAttributesNames[i].equalsIgnoreCase("extractionScript")){
        pAttributes.add(Util.createCheckPanel(
            (JFrame) SwingUtilities.getWindowAncestor(getRootPane()),
            cstAttributesNames[i], tcCstAttributes[i], true, true),
            new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 22, 5, 5), 0, 0));
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("inputFiles")){
        pAttributes.add(Util.createCheckPanel(
            (JFrame) SwingUtilities.getWindowAncestor(getRootPane()),
            cstAttributesNames[i], tcCstAttributes[i], false, true),
            new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 22, 5, 5), 0, 0));
      }
      else{
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "),
            new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.BOTH,
            new Insets(5, 25, 5, 5), 0, 0));
      }
      if(cstAttributesNames[i].equalsIgnoreCase("runtimeEnvironmentName")){
        Debug.debug("Setting selection to "+runtimeEnvironmentName, 3);
        if(cbRuntimeEnvironmentSelection!=null &&
            runtimeEnvironmentName!=null && !runtimeEnvironmentName.equals("")){
           cbRuntimeEnvironmentSelection.setSelectedItem(runtimeEnvironmentName);
          cbRuntimeEnvironmentSelection.updateUI();
        }
        Util.setJEditable(tcCstAttributes[i], false);
      }
      pAttributes.add(tcCstAttributes[i],
          new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
          GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
          new Insets(5, 5, 5, 5), 0, 0));
    }
  }

  /**
   *  Edit a transformation
   */
  public void editTransformation(int transformationID,
      String runtimeEnvironmentName){
    for(int i =0; i<tcCstAttributes.length; ++i){
      for(int j=0; j<transformationFields.length;++j){
        if(transformationFields[j].toString().equalsIgnoreCase(
            cstAttributesNames[i].toString()) &&
            !transformationFields[j].toString().equals("")){
          if(tcCstAttributes[i]==null || !tcCstAttributes[i].isEnabled() &&
             tcCstAttributes[i].getText().length()==0){            
            if(cstAttributesNames[i].equalsIgnoreCase("initLines") ||
                cstAttributesNames[i].equalsIgnoreCase("comment")){
              tcCstAttributes[i] = Util.createTextArea(TEXTFIELDWIDTH);
            }
            else{
              tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
            }
            pAttributes.add(tcCstAttributes[i],
                new GridBagConstraints(
                    1,i/*row*/, 3, 1, 1.0, 0.0,
                    GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL,
                    new Insets(5, 5, 5, 5), 0, 0));
          }
          if(editing){
            try{
              Util.setJText(tcCstAttributes[i], transformation.values[j].toString());
                Debug.debug(i+": "+cstAttributesNames[i].toString()+"="+
                    transformationFields[j]+". Setting to "+tcCstAttributes[i].getText(),3);
            }
            catch(java.lang.Exception e){
              Debug.debug("Attribute not found, "+e.getMessage(),1);
            }
          }
          break;
        }
      }
      if(cstAttributesNames[i].equalsIgnoreCase(transformationIdentifier) ||
          cstAttributesNames[i].equalsIgnoreCase("created") ||
          cstAttributesNames[i].equalsIgnoreCase("lastModified")){
        Util.setJEditable(tcCstAttributes[i], false);
        if(!editing){
          try{
            Debug.debug("Clearing identifier",3);
            Util.setJText(tcCstAttributes[i], "");
          }
          catch(java.lang.Exception e){
            Debug.debug("Attribute not found, "+e.getMessage(),1);
          }
        }
      }
      else if(cbRuntimeEnvironmentSelection!=null &&
          runtimeEnvironmentName!=null && !runtimeEnvironmentName.equals("") &&
          cstAttributesNames[i].equalsIgnoreCase("runtimeEnvironmentName")){
        Util.setJText(tcCstAttributes[i], runtimeEnvironmentName);
      }
    }
  }

  public void clearPanel(){

    Vector textFields = getTextFields();

    for(int i =0; i<textFields.size(); ++i)
    if(!(cstAttributesNames[i].equalsIgnoreCase("identifier"))){
      ((JTextComponent) textFields.get(i)).setText("");
    }
  }


  public void create(final boolean showResults, final boolean editing){

    final String [] cstAttr = new String[tcCstAttributes.length];

    for(int i=0; i<cstAttr.length; ++i){
      cstAttr[i] = tcCstAttributes[i].getText();
    }

  Debug.debug("createTransformation",  1);

  TransformationCreator tc = new TransformationCreator(
        dbPluginMgr,
        showResults,
        cstAttr,
        cstAttributesNames,
        editing);
    
    if(tc.anyCreated){
      panel.refresh();
    }
  }

  private Vector getTextFields(){
    Vector v = new Vector();

    v.addAll(tcConstant);

    for(int i=0; i<tcCstAttributes.length; ++i)
      v.add(tcCstAttributes[i]);

    return v;
  }
}
