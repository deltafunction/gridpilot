package gridpilot;

import gridpilot.Debug;
import gridpilot.Database.DBRecord;

import javax.swing.*;
import javax.swing.border.*;
import java.awt.*;

import javax.swing.text.*;

import java.util.*;

/**
 * This panel creates records in the DB table. It's shown inside the CreateEditDialog.
 *
 */
public class DatasetCreationPanel extends CreateEditPanel{

  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private String datasetID = "-1";
  private String datasetIdentifier = "identifier";
  private DBPanel panel;
  private Table table;
  private String [] cstAttributesNames;
  private JComponent [] tcCstAttributes;
  private JComponent [] tcCstJobDefAttributes;
  private boolean reuseTextFields = true;
  private Vector tcConstant = new Vector(); // contains all text components
  private String [] cstAttr = null;
  private boolean editing = false;
  private boolean loaded = false;
  private DBPluginMgr dbPluginMgr = null;
  private static int TEXTFIELDWIDTH = 32;
  private static int CFIELDWIDTH = 8;
  private String transformation = "";
  private String version = "";
  private int [] datasetIDs = new int [] {-1};
  private JPanel pTransformation = new JPanel();
  private JPanel pVersion = new JPanel();
  private JComboBox cbTargetDBSelection;
  private JComboBox cbTransformationSelection;
  private JComboBox cbTransVersionSelection;
  private boolean transformationChosen = false;
  private boolean versionChosen = false;
  private StatusBar statusBar = null;
  private String targetDB = null;
  private DBRecord dataset = null;
  
  GridBagConstraints ct = new GridBagConstraints();

  /**
   * Constructor
   */
  public DatasetCreationPanel(DBPluginMgr _dbPluginMgr, DBPanel _panel, boolean _editing){
    
    editing = _editing;
    panel = _panel;
    dbPluginMgr = _dbPluginMgr;
    
    table = panel.getTable();
    
    statusBar = GridPilot.getClassMgr().getStatusBar();
    
    cstAttributesNames = dbPluginMgr.getFieldNames("dataset");
    cstAttr = new String[cstAttributesNames.length];
    
    datasetIdentifier = dbPluginMgr.getIdentifier(
       dbPluginMgr.getDBName(), "dataset");
    
    // Find identifier index
    int identifierIndex = -1;
    for(int i=0; i<table.getColumnNames().length; ++i){
      Debug.debug("Column name: "+table.getColumnNames().length+":"+i+" "+table.getColumnName(i), 3);
      if(table.getColumnName(i).equalsIgnoreCase(datasetIdentifier)){
        identifierIndex = i;
        break;
      }
    }
    if(identifierIndex ==-1){
      Debug.debug("ERROR: could not find index of dataset, "+datasetIdentifier, 1);
    }
    
    // Find dataset id from table
    if(table.getSelectedRows().length==1 && editing){
      datasetID = table.getUnsortedValueAt(table.getSelectedRow(), identifierIndex).toString();
      if(datasetID==null || datasetID.equals("-1") ||
          datasetID.equals("")){
        Debug.debug("ERROR: could not find datasetID in table!", 1);
        return;
      }
      // Fill cstAttr from db
      Debug.debug("Getting dataset. "+editing, 1);     
      dataset = dbPluginMgr.getDataset(Integer.parseInt(datasetID));
    }
    // Creating; set empty dataset
    else{
      //TODO
    }

    // Dataset(s) selected and not editing - creating with input dataset(s)
    if(table.getSelectedRows().length>0 && !editing){
      // Find input datasets
      int [] selectedIDs = panel.getSelectedIdentifiers();
      datasetIDs = new int [selectedIDs.length];
      for(int i=0; i<selectedIDs.length; ++i){
        datasetIDs[i] = Integer.parseInt(
            table.getUnsortedValueAt(i, identifierIndex).toString());
      }
    } 
  }

  /**
   * GUI initialisation
   */
  public void initGUI(){
    String datasetName = "New dataset";
    if(editing){
      try{
        datasetName = dbPluginMgr.getDatasetName(Integer.parseInt(datasetID));
      }
      catch(Exception e){
        // nothing
      }
    }
    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white,new Color(165, 163, 151)), datasetName));
    
    spAttributes.setPreferredSize(new Dimension(550, 500));
    spAttributes.setMinimumSize(new Dimension(550, 500));
    
    setLayout(new GridBagLayout());
    removeAll();
    
    initTransformationPanel(Integer.parseInt(datasetID));

    initAttributePanel();
    
    ct.fill = GridBagConstraints.VERTICAL;
    ct.insets = new Insets(2,2,2,2);
    
    ct.gridx = 0;
    ct.gridy = 1;   
    ct.gridwidth=2;
    ct.gridheight=1;
    add(spAttributes,ct);
    
    updateUI();
    
    loaded = true;
    
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
    
    if(!reuseTextFields || tcCstAttributes==null ||
        tcCstAttributes.length != cstAttributesNames.length){
      Debug.debug("Creating new tcCstAttributes, "+
          tcCstAttributes+", "+(tcCstAttributes==null ? "":Integer.toString(tcCstAttributes.length)),
              3);
      tcCstAttributes = new JComponent[cstAttributesNames.length];
    }
    
    for(int i =0; i<cstAttributesNames.length; ++i){
      
      if(cstAttributesNames[i].equalsIgnoreCase("actualPars") ||
          cstAttributesNames[i].equalsIgnoreCase("transFormalPars")){
        cl.gridx=0;
        cl.gridy=i;
        JTextArea textArea = new JTextArea(10, TEXTFIELDWIDTH);
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
        textArea.setEditable(true);
        if(!reuseTextFields || tcCstAttributes[i]==null){
          tcCstAttributes[i] = textArea;
        }
        Util.setJText(tcCstAttributes[i], cstAttr[i]);
      }
      // when creating, zap loaded dataset id
      else{
        if(cstAttr[i]!=null){
          Util.setJText(tcCstAttributes[i], cstAttr[i]);
        }
        if(!editing && !reuseTextFields ||
            tcCstAttributes[i]==null){
         // Debug.debug("Creating tcCstAttributes["+i+"]: "+cstAttributesNames[i], 3);
          //tcCstAttributes[i] = createTextComponent(TEXTFIELDWIDTH);
          tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
        }
        if(cstAttr[i]!=null && !cstAttr[i].equals("")){
          Debug.debug("Setting cstAttr["+i+"]: "+cstAttr[i], 3);
          Util.setJText(tcCstAttributes[i], cstAttr[i]);
        }
      }      
      cl.gridx=0;
      cl.gridy=i;
      pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), cl);
      cl.gridx=1;
      cl.gridy=i;
      Debug.debug("Adding cstAttributesNames["+i+"], "+cstAttributesNames[i]+
          " "+tcCstAttributes[i].getClass().toString(), 3);
      pAttributes.add(tcCstAttributes[i], cl);//,
    }
    if(!editing){
      for(int i =0; i<tcCstAttributes.length; ++i){
        if(cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier) ||
            cstAttributesNames[i].equalsIgnoreCase("transFK") ||
            cstAttributesNames[i].equalsIgnoreCase("transformationFK")){
          Util.setJText((JComponent) tcCstAttributes[i],"");
          Util.setJEditable(tcCstAttributes[i], false);
        }
      }
    }
    editDataset(Integer.parseInt(datasetID), transformation, version);
  }

  /*
   * Set special values - like values from xml tags
   */
  private void setValuesInAttributePanel(String transformation,
      String version){
    
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
      else if(cstAttributesNames[i].equalsIgnoreCase("transFK")){
        Database.DBResult res = dbPluginMgr.getTransformations();
        for(int j=0; j<res.values.length; ++j){
          if(res.getValue(j, "name").toString().equalsIgnoreCase(transformation) &&
            res.getValue(j, "version").toString().equalsIgnoreCase(version)){
            String transformationIdentifier = dbPluginMgr.getIdentifier(
                dbPluginMgr.getDBName(), "transformation");
            Debug.debug("Setting transformation FK: "+
                res.getValue(j, transformationIdentifier)+" "+
                tcCstAttributes[i].getClass().toString(), 3);
            Util.setJText(tcCstAttributes[i],
                res.getValue(j, transformationIdentifier).toString());
            break;
          }
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("transformationFK")){
        Database.DBResult res = dbPluginMgr.getTransformations();
        for(int j=0; j<res.values.length; ++j){
          if(res.getValue(j, "name").toString().equalsIgnoreCase(transformation) &&
            res.getValue(j, "version").toString().equalsIgnoreCase(version)){
            String transformationIdentifier = dbPluginMgr.getIdentifier(
                dbPluginMgr.getDBName(), "transformation");
            Debug.debug("Setting transformation FK: "+
                res.getValue(j, transformationIdentifier)+" "+
                tcCstAttributes[i].getClass().toString(), 3);
            Util.setJText(tcCstAttributes[i],
                res.getValue(j, transformationIdentifier).toString());
            break;
          }
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("targetDatabase")){
        // TODO
      }
      else if(cstAttributesNames[i].equalsIgnoreCase("inputDataset")){
        // TODO
      }
      else{
        // nothing
      }
    }
  }
  
   /**
   * public methods
   */

  public void clear(){
    Vector textFields;
    if(editing){
      textFields = getFields(); 
    }
    else{
      textFields = getNonIdTextFields();
    }
    for(int i =0; i<textFields.size(); ++i){
      Util.setJText((JComponent) textFields.get(i),"");
    }
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

  public void create(final boolean showResults, final boolean editing){

    if(editing){
      Debug.debug("edit dataset",  1);
    }
    else{
      Debug.debug("create dataset",  1);
    }
    
    final String [] cstAttr = new String[tcCstAttributes.length];
    
    if(editing){
      for(int i=0; i<cstAttr.length; ++i){
        cstAttr[i] = Util.getJTextOrEmptyString(tcCstAttributes[i]);
        Debug.debug("createDataset: cstAttr["+i+"]: "+cstAttr[i],  3);
      }
      new DatasetUpdater(
          dbPluginMgr,
          showResults,
          cstAttr,
          cstAttributesNames,
          Integer.parseInt(datasetID)
          );
    }
    else{
      if(cbTargetDBSelection.getSelectedItem().toString()!=null &&
          !cbTargetDBSelection.getSelectedItem().toString().equals("") &&
          panel.getSelectedIdentifiers().length>0){
        targetDB = cbTargetDBSelection.getSelectedItem().toString();
      }
      for(int i=0; i<cstAttr.length; ++i){
        cstAttr[i] = Util.getJTextOrEmptyString(tcCstAttributes[i]);
        Debug.debug("createDataset: cstAttr["+i+"]: "+cstAttr[i],  3);
      }
      new DatasetCreator(
          dbPluginMgr,
          showResults,
          cstAttr,
          cstAttributesNames,
          datasetIDs,
          targetDB
          );
    }
  }

  /**
   *  Edit or create a dataset
   */

  public void editDataset(int datasetID,
      String transformation, String version){
    
    Debug.debug("editDataset: " + Integer.toString(datasetID) +
        " " + transformation + " " + version,3); 
    Debug.debug("Got field names: "+Util.arrayToString(cstAttributesNames),3);

    int row = 0;

    if(editing){
      // set values of fields
      Debug.debug("Got dataset record. "+dataset.fields.length+":"+
          tcCstAttributes.length, 3);   
      for(int i =0; i<tcCstAttributes.length; ++i){
        for(int j=0; j<dataset.fields.length;++j){
          //Debug.debug("Checking dataset field, "+dataset.fields[j].toString()+"<->"+cstAttributesNames[i].toString(), 3);
          if(dataset.fields[j].toString().equalsIgnoreCase(cstAttributesNames[i].toString())){
            //if(tcCstAttributes[i]==null /*|| !tcCstAttributes[i].isEnabled() &&
              // Util.getJTextOrEmptyString(tcCstAttributes[i]).length()==0*/){
              //tcCstAttributes[i] = createTextComponent();
            //}
            //if(!cstAttributesNames[i].equalsIgnoreCase("transFK") &&
              //  !cstAttributesNames[i].equalsIgnoreCase("transformationFK")){
              try{
                Debug.debug(cstAttributesNames[i].toString()+"="+dataset.fields[j]+". Setting to "+
                    dataset.values[j].toString(),3);
                Util.setJText(tcCstAttributes[i], dataset.values[j].toString());
              }
              catch(java.lang.Exception e){
                Debug.debug("Field not found or set, "+e.getMessage(),1);
              }
            //}
            break;
          }
        }
        // make identifier and transformation foreign key inactive
        if(cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier) ||
              cstAttributesNames[i].equalsIgnoreCase("transFK") ||
              cstAttributesNames[i].equalsIgnoreCase("transformationFK")){          
          if(!cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier) ||
              datasetID!=-1){
            Util.setJEditable(tcCstAttributes[i], false);
          }
          else{
            try{
              Debug.debug("Clearing identifier",3);
              Util.setJText(tcCstAttributes[i], "");
              Util.setJEditable(tcCstAttributes[i], true);
            }
            catch(java.lang.Exception e){
              Debug.debug("Field not found, "+e.getMessage(),1);
            }
          }
        }
        else{
          Util.setJEditable(tcCstAttributes[i], true);
        }
      }
    }
    setValuesInAttributePanel(transformation, version);
  }

  private String[] getTransNames(){
    Database.DBResult res = dbPluginMgr.getTransformations();
    Debug.debug("Number of transformations found: "+res.values.length+
        "; "+Util.arrayToString(res.fields),3);
    String [] ret = new String[res.values.length];
    for(int i=0; i<res.values.length; ++i){
      ret[i] = res.getValue(i, "name").toString(); 
      Debug.debug("name is "+ret[i], 3);
    }
    // This is to ensure only unique elements
    // TODO: for some reason this doesn't seam to work
    Arrays.sort(ret);
    Vector vec = new Vector();
    if(res.values.length>0){
      vec.add(ret[0]);
    }
    if(res.values.length>1){
      for(int i=1; i<res.values.length; ++i){
        //Debug.debug("Comparing "+ret[i]+" <-> "+ret[i-1],3);
        if(!ret[i].equals(ret[i-1])){
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
  
  private void initTransformationPanel(int datasetID){
    boolean found = false;
    
    Debug.debug("Finding transformations...",3);

    pTransformation.removeAll();
    pTransformation.setLayout(new FlowLayout());

    String [] transformations = getTransNames();

    if(transformations.length==0){
      pTransformation.add(new JLabel("No transformations found."));
      transformationChosen = false;
    }
    else if(transformations.length==1){
      transformation = transformations[0];
      pTransformation.add(new JLabel("Transformation:" + transformation));
      initTransVersionPanel(datasetID, transformation);
      transformationChosen = true;
    }
    else{
      cbTransformationSelection = new JComboBox();
      for(int i=0; i<transformations.length; ++i){
          cbTransformationSelection.addItem(transformations[i]);
      }
      pTransformation.add(new JLabel("Transformation:"), null);
      pTransformation.add(cbTransformationSelection, null);

      cbTransformationSelection.addActionListener(
        new java.awt.event.ActionListener(){
          public void actionPerformed(java.awt.event.ActionEvent e){
            cbTransformationSelection_actionPerformed();
          }
        }
      );
      transformationChosen = false;
    }
    
    ct.gridx = 0;
    ct.gridy = 0;
    ct.gridwidth=1;
    ct.gridheight=1;
    add(pTransformation, ct);

    updateUI();
  }

  private void initTransVersionPanel(int datasetID, String transformation){
    boolean found = false;

    Debug.debug("Finding versions...",3);

    pVersion.removeAll();
    pVersion.setLayout(new FlowLayout());

    String [] versions = dbPluginMgr.getVersions(transformation);
    Debug.debug("Number of versions found: "+versions.length,3);

    if(versions.length==0){

      pVersion.add(new JLabel("No versions found."));
      versionChosen = false;

    }
    else if(versions.length==1){

      version = versions[0];
      pVersion.add(new JLabel("Version:" + version));

        versionChosen = true;
        editDataset(datasetID, transformation, version);
    }
    else{
      cbTransVersionSelection = new JComboBox();

      for(int i=0; i<versions.length; ++i){
        Debug.debug("Adding version "+versions[i],3);
        cbTransVersionSelection.addItem(versions[i]);
      }

      pVersion.add(new JLabel("Version:"), null);
      pVersion.add(cbTransVersionSelection, null);

      cbTransVersionSelection.addActionListener(
        new java.awt.event.ActionListener(){
          public void actionPerformed(java.awt.event.ActionEvent e){
            cbTransVersionSelection_actionPerformed();
          }
        }
      );
      versionChosen = false;
    }

    ct.gridx = 1;
    ct.gridy = 0;
    ct.gridwidth=1;
    ct.gridheight=1;
    add(pVersion, ct);

    updateUI();
  }

  private void cbTransformationSelection_actionPerformed(){
    if(cbTransformationSelection.getSelectedItem()==null){
        //transformation = cbTransformationSelection.getItemAt(0).toString();
        return;
    }
    else{
        transformation = cbTransformationSelection.getSelectedItem().toString();
    }
    transformationChosen = true;
    initTransVersionPanel(Integer.parseInt(datasetID), transformation);
  }

  private void cbTransVersionSelection_actionPerformed(){
    if(cbTransVersionSelection.getSelectedItem()==null){
      //transformation = cbTransVersionSelection.getItemAt(0).toString();
      return;
    }
    else{
      version = cbTransVersionSelection.getSelectedItem().toString();
    }
    versionChosen = true;
    editDataset(Integer.parseInt(datasetID), transformation, version);
  }

  private Vector getFields(){
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
      if(!cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier)){
        v.add(tcCstAttributes[i]);
      }
    }

    return v;
  }

  private Vector getIdTextFields(){
    Vector v = new Vector();

    v.addAll(tcConstant);

    for(int i=0; i<tcCstAttributes.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier)){
        v.add(tcCstAttributes[i]);
      }
    }

    return v;
  }

  /*private JTextComponent createTextComponent(){
    JTextArea ta = new JTextArea();
    ta.setBorder(new JTextField().getBorder());
    ta.setWrapStyleWord(true);
    ta.setLineWrap(true);
    return ta;
  }*/
  
  /*private JTextComponent createTextComponent(int cols){
    JTextField tf = new JTextField("", cols);
    return tf;
  }*/
  
  /*private JTextComponent createTextComponent(String str){
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
  }*/
  
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
}
