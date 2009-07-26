package gridpilot;

import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.text.JTextComponent;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.*;

/**
 * This panel creates records in the DB table. It's shown inside the CreateEditDialog.
 *
 */
public class DatasetCreationPanel extends CreateEditPanel{

  private static final long serialVersionUID = 1L;
  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private String datasetID = "-1";
  private String datasetIdentifier = "identifier";
  private DBPanel panel;
  private MyJTable table;
  private String [] cstAttributesNames;
  private JTextComponent [] tcCstAttributes;
  private boolean reuseTextFields = true;
  private Vector tcConstant = new Vector(); // contains all text components
  private String [] cstAttr = null;
  private boolean editing = false;
  private DBPluginMgr dbPluginMgr = null;
  private DBPluginMgr targetDBPluginMgr = null;
  private static int TEXTFIELDWIDTH = 32;
  private String transformationName = "";
  private String transformationVersion = "";
  private String [] datasetIDs = new String [] {"-1"};
  private JPanel pTop = new JPanel();
  private JPanel pTransformation = new JPanel();
  private JPanel pTargetDBs = new JPanel();
  private JLabel jlTargetDBSelection = null;
  private JPanel pVersion = new JPanel();
  private JComboBox cbTargetDBSelection;
  private JComboBox cbTransformationSelection;
  private JComboBox cbTransVersionSelection;
  private String targetDB = null;
  private DBRecord dataset = null;
  private DBResult transformations = null;
  private String [] datasetTransformationReference;
  private String [] datasetTransformationVersionReference;
  private JButton jbEditTrans;
  private String datasetName = "";
  private String title = "";
  private DBPanel targetPanel;
  
  public boolean editable = true;

  /**
   * Constructor
   */
  public DatasetCreationPanel(DBPluginMgr _dbPluginMgr, DBPanel _panel, boolean _editing){
    
    editing = _editing;
    panel = _panel;
    dbPluginMgr = _dbPluginMgr;
    targetDBPluginMgr = dbPluginMgr;
    table = panel.getTable();
    cstAttributesNames = dbPluginMgr.getFieldNames("dataset");
    cstAttr = new String[cstAttributesNames.length];
    datasetIdentifier = MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "dataset");
    transformations = dbPluginMgr.getTransformations();
    editable = true;
    
    datasetTransformationReference =
      MyUtil.getDatasetTransformationReference(dbPluginMgr.getDBName());
    datasetTransformationVersionReference =
      MyUtil.getDatasetTransformationVersionReference(dbPluginMgr.getDBName());
    
    // Find identifier index
    int identifierIndex = -1;
    for(int i=0; i<table.getColumnNames().length; ++i){
      Debug.debug("Column name: "+table.getColumnNames().length+":"+i+" "+table.getColumnName(i), 3);
      if(table.getColumnName(i).equalsIgnoreCase(datasetIdentifier)){
        identifierIndex = i;
        break;
      }
    }
    if(identifierIndex==-1){
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
      dataset = dbPluginMgr.getDataset(datasetID);
    }
    // Creating; set empty dataset
    else{
      //TODO
    }

    // Dataset(s) selected and not editing - creating with input dataset(s)
    if(table.getSelectedRows().length>0 && !editing){
      // Find input datasets
      datasetIDs = panel.getSelectedIdentifiers();
      Debug.debug("Creating with input datasets "+identifierIndex+":"+datasetID+": "+
          MyUtil.arrayToString(datasetIDs), 3);
    } 
  }

  private void initButtons(){
    URL imgURL;
    ImageIcon imgIcon;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + "search.png");
      imgIcon = new ImageIcon(imgURL);
      jbEditTrans = new JButton(imgIcon);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.ICONS_PATH + "search.png", 3);
      jbEditTrans = new JButton("View");
    }
    jbEditTrans.setToolTipText("View transformation record");
  }

  /**
   * GUI initialisation
   */
  public void initGUI(){
    initButtons();
    setLayout(new BorderLayout());
    pTop.setLayout(new FlowLayout());
     removeAll();
    
    if(editing){
      try{
        datasetName = dbPluginMgr.getDatasetName(datasetID);
      }
      catch(Exception e){
        // nothing
      }
    }
    else if(GridPilot.DB_NAMES!=null && GridPilot.DB_NAMES.length!=0 &&
        datasetIDs!=null && datasetIDs.length!=0 &&
        (datasetIDs.length!=1 || !datasetIDs[0].equals("-1"))){
      if(datasetIDs.length==1){
        if(datasetIDs[0].equals("-1")){
          title = "Define new dataset";
        }
        else{
          title = "Define target dataset for id ";
        }
      }
      else{
        title = "Define target datasets for ids ";
      }
      for(int i=0; i<datasetIDs.length; ++i){
        if(i>0){
          title += ", ";
        }
        title += datasetIDs[i];
        if(i>5){
          title += " ... ";
          break;
        }
      }
      initTargetDBsPanel();
      pTop.add(pTargetDBs);
    }

    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), title));
    
    //spAttributes.setPreferredSize(new Dimension(590, 500));
    spAttributes.setMaximumSize(new Dimension(600, 500));
    //spAttributes.setMinimumSize(new Dimension(300, 300));
    
    pTop.add(pTransformation);

    initAttributePanel();
    setValues();
    checkValues();
    
    if(GridPilot.DB_NAMES==null || GridPilot.DB_NAMES.length==0 ||
        datasetIDs==null || datasetIDs.length==0 ||
        (datasetIDs.length==1 && datasetIDs[0].equals("-1"))){
      initTransformationPanel();
      initTransVersionPanel();
      setComboBoxValues();
    }

    add(pTop, BorderLayout.NORTH);
    add(spAttributes, BorderLayout.CENTER);
    
    jbEditTrans.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        try {
          viewTransformation();
        }
        catch(Exception e1){
          e1.printStackTrace();
        }
      }
    }
    );
    
    updateUI();
  }
  
  private void checkValues(){
    for(int i=0; i<cstAttributesNames.length; ++i){
      // in case the database does not support lookup on dataset id (like DQ2);
      // then the values will be null and we have to take those we can directly from tableResults.
      // If we cannot get all values, we will not allow editing.
      if(editing && (cstAttr[i]==null /*|| cstAttr[i].equals("")*/)){
        try{
          MyJTable tableResults = panel.getTable();
          if(tableResults.getSelectedRows().length==1){
            boolean ok = false;
            for(int k=0; k<tableResults.getColumnCount(); ++k){
              if(tableResults.getColumnName(k).equalsIgnoreCase(cstAttributesNames[i])){
                cstAttr[i] = tableResults.getUnsortedValueAt(tableResults.getSelectedRow(), k).toString();
                ok = true;
                break;
              }
            }
            if(!ok){
              Debug.debug("Dataset has no "+cstAttributesNames[i]+", "+cstAttr[i], 2);
              if(datasetName==null){
              }
              editable = false;
            }
          }
          else{
            Debug.debug("Dataset has no "+cstAttributesNames[i]+", "+cstAttr[i]+" and no row is selected.", 2);
            editable = false;
          }
        }
        catch(Exception e){
          editable = false;
          e.printStackTrace();
        }
      }
    }
    title = "Edit dataset " + 
    (datasetName.length()>32 ? datasetName.substring(0, 28)+"..." : datasetName);
  }

  private void initAttributePanel(){
    
    pAttributes.setLayout(new GridBagLayout());
    pAttributes.removeAll();

    spAttributes.getViewport().add(pAttributes);
    
    if(!reuseTextFields || tcCstAttributes==null ||
        tcCstAttributes.length!=cstAttributesNames.length){
      Debug.debug("Creating new tcCstAttributes, "+tcCstAttributes+", "+
          (tcCstAttributes==null ? "":Integer.toString(tcCstAttributes.length)),
              3);
      tcCstAttributes = new JTextComponent[cstAttributesNames.length];
    }
    for(int i=0; i<cstAttributesNames.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase("metaData")){
        if(!reuseTextFields || tcCstAttributes[i]==null){
          tcCstAttributes[i] = MyUtil.createTextArea(TEXTFIELDWIDTH);
        }
      }
      else{
        if(!editing && !reuseTextFields || tcCstAttributes[i]==null){
          tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
        }
      }
      if(cstAttr[i]!=null && !cstAttr[i].equals("")){
        Debug.debug("Setting cstAttr["+i+"]: "+cstAttr[i], 3);
        MyUtil.setJText(tcCstAttributes[i], cstAttr[i]);
      }
      Debug.debug("Adding cstAttributesNames["+i+"], "+cstAttributesNames[i]+
          " "+tcCstAttributes[i].getClass().toString(), 3);
      if(cstAttributesNames[i].equalsIgnoreCase("outputLocation")){
        pAttributes.add(MyUtil.createCheckPanel(
            (JFrame) SwingUtilities.getWindowAncestor(getRootPane()),
            cstAttributesNames[i], tcCstAttributes[i], true, true, true, false),
            new GridBagConstraints(0, i, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 21, 5, 5), 0, 0));
        pAttributes.add(tcCstAttributes[i],
            new GridBagConstraints(1, i, 3, 1, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
      }
      else{
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "),
            new GridBagConstraints(0, i, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 25, 5, 5), 0, 0));
        pAttributes.add(tcCstAttributes[i],
            new GridBagConstraints(1, i, 3, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(5, 5, 5, 5), 0, 0));
      }
      // when creating, zap loaded dataset id
      if(!editing && cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier)){
        MyUtil.setJText((JComponent) tcCstAttributes[i], "");
        MyUtil.setJEditable(tcCstAttributes[i], false);
      }
    }
    
    for(int i=0; i<cstAttributesNames.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase(datasetTransformationReference[1]) ||
         cstAttributesNames[i].equalsIgnoreCase(datasetTransformationVersionReference[1]) ||
         cstAttributesNames[i].equalsIgnoreCase("created") ||
         cstAttributesNames[i].equalsIgnoreCase("lastModified")){
        MyUtil.setJEditable(tcCstAttributes[i], false);
      }
    }
  }

  private void setComboBoxValues(){
    for(int i=0; i<cstAttributesNames.length; ++i){
      if(cstAttributesNames[i].equalsIgnoreCase(datasetTransformationReference[1])){
        if(cbTransformationSelection!=null &&
            transformationName!=null && transformationName!=null){
          cbTransformationSelection.setSelectedItem(transformationName);
        }
      }
      else if(cstAttributesNames[i].equalsIgnoreCase(datasetTransformationVersionReference[1])){
        if(transformationVersion!=null && cbTransVersionSelection!=null){
          cbTransVersionSelection.setSelectedItem(transformationVersion);
        }
      }
    }
  }
  
  /*
   * Set special values.
   */
  private void setValuesInAttributePanel(){
    
    Debug.debug("Setting values "+transformationName+":"+transformationVersion+":"+transformationName+":"+transformationVersion, 3);

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
      else if(transformationName!=null && !transformationName.equals("") &&
          cstAttributesNames[i].equalsIgnoreCase(datasetTransformationReference[1])){
        MyUtil.setJText(tcCstAttributes[i], transformationName);
      }
      else if(transformationVersion!=null && !transformationVersion.equals("") &&
          cstAttributesNames[i].equalsIgnoreCase(datasetTransformationVersionReference[1])){
        MyUtil.setJText(tcCstAttributes[i], transformationVersion);
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
  
  public void clearPanel(){
    Vector textFields;
    if(editing){
      textFields = getFields(); 
    }
    else{
      textFields = getNonIdTextFields();
    }
    for(int i =0; i<textFields.size(); ++i){
      MyUtil.setJText((JComponent) textFields.get(i),"");
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
      Debug.debug("edit dataset", 1);
    }
    else{
      Debug.debug("create dataset", 1);
    }
    
    final String [] cstAttr = new String[tcCstAttributes.length];
    
    if(editing){
      for(int i=0; i<cstAttr.length; ++i){
        cstAttr[i] = MyUtil.getJTextOrEmptyString(tcCstAttributes[i]);
        Debug.debug("createDataset: cstAttr["+i+"]: "+cstAttr[i], 3);
      }
      DatasetUpdater dsu = new DatasetUpdater(
          dbPluginMgr,
          showResults,
          cstAttr,
          cstAttributesNames,
          datasetID,
          datasetName
          );
      // TODO: refresh results on panel showing datasets from the db - if such a panel is shown
      if(dsu.anyCreated && panel.getDBName().equalsIgnoreCase(dbPluginMgr.getDBName())){
        panel.refresh();
      }
    }
    else{
      if(cbTargetDBSelection!=null && cbTargetDBSelection.getSelectedItem()!=null &&
          !cbTargetDBSelection.getSelectedItem().toString().equals("") &&
          panel.getSelectedIdentifiers().length>0){
        targetDB = cbTargetDBSelection.getSelectedItem().toString();
      }
      for(int i=0; i<cstAttr.length; ++i){
        cstAttr[i] = MyUtil.getJTextOrEmptyString(tcCstAttributes[i]);
        Debug.debug("createDataset: cstAttr["+i+"]: "+cstAttr[i], 3);
      }
      DatasetCreator dsc = new DatasetCreator(
          statusBar,
          dbPluginMgr,
          showResults,
          cstAttr,
          cstAttributesNames,
          datasetIDs,
          targetDB
          );
      if(dsc.anyCreated){
        panel.refresh();
      }
    }
  }

  /**
   *  Edit or create a dataset
   */
  private void setValues(){
    
    Debug.debug("setValues: " + datasetID +
        " " + transformationName + " " + transformationVersion, 3); 
    Debug.debug("Got field names: "+
        MyUtil.arrayToString(cstAttributesNames), 3);

    if(editing){
      // set values of fields
      Debug.debug("Got dataset record. "+dataset.fields.length+":"+
          tcCstAttributes.length, 3);
      for(int i=0; i<tcCstAttributes.length; ++i){
        for(int j=0; j<dataset.fields.length;++j){
          //Debug.debug("Checking dataset field, "+dataset.fields[j].toString()+"<->"+cstAttributesNames[i].toString(), 3);
          if(dataset.fields[j].toString().equalsIgnoreCase(cstAttributesNames[i].toString())){
              try{
                Debug.debug(cstAttributesNames[i].toString()+"="+dataset.fields[j]+". Setting to "+
                    dataset.values[j].toString(), 3);
                MyUtil.setJText(tcCstAttributes[i], dataset.values[j].toString());
                cstAttr[i] = dataset.values[j].toString();
                
                if(cstAttributesNames[i].equalsIgnoreCase(datasetTransformationReference[1])){
                  transformationName = dataset.values[j].toString();
                }
                else if(cstAttributesNames[i].equalsIgnoreCase(datasetTransformationVersionReference[1])){
                  transformationVersion = dataset.values[j].toString();
                }
              }
              catch(java.lang.Exception e){
                Debug.debug("Field not found or set, "+e.getMessage(), 1);
              }
            break;
          }
        }
        // make identifier and transformation foreign key inactive
        if(cstAttributesNames[i].equalsIgnoreCase(datasetIdentifier) ||
              cstAttributesNames[i].equalsIgnoreCase(datasetTransformationReference[1]) ||
              cstAttributesNames[i].equalsIgnoreCase(datasetTransformationVersionReference[1]) ||
              cstAttributesNames[i].equalsIgnoreCase("created") ||
              cstAttributesNames[i].equalsIgnoreCase("lastModified")){          
          try{
            if(datasetID==null || datasetID.equals("-1")){
              Debug.debug("Clearing identifier", 3);
              MyUtil.setJText(tcCstAttributes[i], "");
            }
            MyUtil.setJEditable(tcCstAttributes[i], false);
          }
          catch(java.lang.Exception e){
            Debug.debug("Field not found, "+e.getMessage(), 1);
          }
        }
        else{
          MyUtil.setJEditable(tcCstAttributes[i], true);
        }
      }
    }
    setValuesInAttributePanel();
  }

  private String[] getTransNames(){
    String [] ret = new String[transformations.values.length];
    Debug.debug("number of transformations: "+transformations.values.length, 3);
    Debug.debug("fields: "+MyUtil.arrayToString(transformations.fields), 3);
    for(int i=0; i<transformations.values.length; ++i){
      Debug.debug("#"+i, 3);
      Debug.debug("name: "+transformations.getValue(i, "name"), 3);
      Debug.debug("values: "+MyUtil.arrayToString(transformations.values[i]), 3);
      ret[i] = transformations.getValue(i, "name").toString(); 
    }
    // This is to ensure only unique elements
    Arrays.sort(ret);
    Vector vec = new Vector();
    if(transformations.values.length>0){
      vec.add(ret[0]);
    }
    if(transformations.values.length>1){
      for(int i=1; i<transformations.values.length; ++i){
        //Debug.debug("Comparing "+ret[i]+" <-> "+ret[i-1],3);
        if(!ret[i].equals(ret[i-1])){
          Debug.debug("Adding "+ret[i], 3);
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
  
  private void initTransformationPanel(){
    
    Debug.debug("Finding transformations...", 3);

    pTransformation.removeAll();
    pTransformation.setLayout(new FlowLayout());

    // if there's no transforation table, just skip
    String [] transNames = {};
    try{
      transNames = getTransNames();
    }
    catch(Exception e){
      Debug.debug("Could not get transformations, probably no transformation table.", 1);
    }

    if(transNames.length==0){
      pTransformation.add(new JLabel("No transformations found."));
      initTransVersionPanel();
      jbEditTrans.setEnabled(false);
    }
    else if(transNames.length==1){
      transformationName = transNames[0];
      pTransformation.add(new JLabel("Transformation:   " + transformationName));
      initTransVersionPanel();
    }
    else{
      cbTransformationSelection = new JComboBox();
      for(int i=0; i<transNames.length; ++i){
          cbTransformationSelection.addItem(transNames[i]);
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
    }
  }

  private void initTransVersionPanel(){

    pTop.add(pVersion);

    Debug.debug("Finding versions...", 3);

    pVersion.removeAll();
    pVersion.setLayout(new FlowLayout());

    String [] versions = {};
    
    try{
      versions = targetDBPluginMgr.getVersions(transformationName);
    }
    catch(Exception e){
    }
    if(versions==null){
      versions = new String [] {};
    }
    
    Debug.debug("Number of versions found: "+versions.length, 3);

    if(versions.length==0){
      try{
        pTop.remove(jbEditTrans);
      }
      catch(Exception e){
      }
      if(editing){
        pVersion.add(new JLabel("No versions found."));
      }
    }
    else if(versions.length==1){
      transformationVersion = versions[0];
      pVersion.add(new JLabel(transformationVersion));
      //setValues();
      setValuesInAttributePanel();
      pTop.add(jbEditTrans);
    }
    else{
      cbTransVersionSelection = new JComboBox();

      for(int i=0; i<versions.length; ++i){
        Debug.debug("Adding version "+versions[i], 3);
        cbTransVersionSelection.addItem(versions[i]);
      }

      pVersion.add(cbTransVersionSelection, null);

      cbTransVersionSelection.addActionListener(
        new java.awt.event.ActionListener(){
          public void actionPerformed(java.awt.event.ActionEvent e){
            cbTransVersionSelection_actionPerformed();
          }
        }
      );
    }
    pTop.updateUI();
  }

  /**
   * Open new pane with corresponding transformations.
   * @throws InvocationTargetException 
   * @throws InterruptedException 
   */
  private void viewTransformation() throws InterruptedException, InvocationTargetException{
    if(transformationName==null || transformationName.equals("") ||
        transformationVersion==null || transformationVersion.equals("")){
      return;
    }
    
    MyResThread t1 = new MyResThread(){
      public void run(){
        try{
          GridPilot.getClassMgr().getGlobalFrame().requestFocusInWindow();
          //GridPilot.getClassMgr().getGlobalFrame().setVisible(true);
          // Create new panel with jobDefinitions.         
          targetPanel = new DBPanel();
          targetPanel.initDB(targetDBPluginMgr.getDBName(), "transformation");
          targetPanel.initGUI();
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
    };
    if(SwingUtilities.isEventDispatchThread()){
      t1.run();
    }
    else{
      SwingUtilities.invokeAndWait(t1);
    }

    MyResThread t2 = new MyResThread(){
      public void run(){
        try{
          targetPanel.initDB(targetDBPluginMgr.getDBName(),
              "transformation");
          String idField =
            MyUtil.getIdentifierField(targetDBPluginMgr.getDBName(), "transformation");
          String id = targetDBPluginMgr.getTransformationID(transformationName,
              transformationVersion);
          targetPanel.getSelectPanel().setConstraint(idField, id, 0);
          targetPanel.searchRequest(true, false);           
          GridPilot.getClassMgr().getGlobalFrame().addPanel(targetPanel);
        }
        catch(Exception e){
          Debug.debug("Couldn't create panel for dataset " + "\n" +
                             "\tException\t : " + e.getMessage(), 2);
          e.printStackTrace();
        }
      }
    };
    if(SwingUtilities.isEventDispatchThread()){
      t2.run();
    }
    else{
      SwingUtilities.invokeLater(t2);
    }
  }

  private void cbTransformationSelection_actionPerformed(){
    if(cbTransformationSelection.getSelectedItem()==null){
        return;
    }
    else{
        transformationName = cbTransformationSelection.getSelectedItem().toString();
    }
    try{
      pTop.remove(jbEditTrans);
    }
    catch(Exception e){
    }
    initTransVersionPanel();
  }

  private void cbTransVersionSelection_actionPerformed(){
    if(cbTransVersionSelection.getSelectedItem()==null){
      return;
    }
    else{
      transformationVersion = cbTransVersionSelection.getSelectedItem().toString();
    }
    try{
      pTop.remove(jbEditTrans);
    }
    catch(Exception e){
    }
    //setValues();
    setValuesInAttributePanel();
    pTop.add(jbEditTrans);
    pTop.validate();
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

  private void initTargetDBsPanel(){
    
    Debug.debug("Finding target dataset databases...",3);
  
    pTargetDBs.removeAll();
    pTargetDBs.setLayout(new FlowLayout());
  
    cbTargetDBSelection = new JComboBox();
    
    cbTargetDBSelection.addItem("");
    for(int i=0; i<GridPilot.DB_NAMES.length; ++i){
      // If this DB has no job definition table, there's no point in
      // allowing the creation of datasets with an input dataset in it.
      try{
        if(!GridPilot.getClassMgr().getDBPluginMgr(GridPilot.DB_NAMES[i]).isJobRepository()){
          continue;
        }
      }
      catch(Exception e){
        continue;
      }
      cbTargetDBSelection.addItem(GridPilot.DB_NAMES[i]);
    }

    jlTargetDBSelection = new JLabel("DB:");
    pTargetDBs.add(jlTargetDBSelection, null);
    pTargetDBs.add(cbTargetDBSelection, null);
    
    cbTargetDBSelection.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(java.awt.event.ActionEvent e){
        cbTargetDBSelection_actionPerformed();
    }});
  
  }

  /**
   * Action Events
   */
  
  /**
   * Modify attributes according to target database selected:
   * map source fields to target fields if possible.
   */
  private void cbTargetDBSelection_actionPerformed(){
    if(cbTargetDBSelection.getSelectedItem()==null){
      return;
    }
    // only do anything if there are input dataset(s) selected
    Debug.debug("datasetIDs[0]="+datasetIDs[0], 3);
    if(datasetIDs[0].equals("-1")){
      return;
    }
    String [] sourceFields = cstAttributesNames;
    String [] sourceAttr = new String[cstAttributesNames.length];
    for(int i=0; i<tcCstAttributes.length; ++i){
      sourceAttr[i] = MyUtil.getJTextOrEmptyString(tcCstAttributes[i]);
    }
    targetDB = cbTargetDBSelection.getSelectedItem().toString();
    targetDBPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(
        targetDB);
    String [] targetFields = targetDBPluginMgr.getFieldNames("dataset");
    String [] targetAttr = new String[targetFields.length];
    for(int j=0; j<targetFields.length; ++j){ 
      targetAttr[j] = "";
      //Do the mapping.
      for(int k=0; k<sourceFields.length; ++k){
        if(!sourceFields[k].equalsIgnoreCase("inputDataset") &&
            !sourceFields[k].equalsIgnoreCase("inputDB") &&
            sourceFields[k].equalsIgnoreCase(targetFields[j])){
          targetAttr[j] = sourceAttr[k];
          break;
        }
      }
      // Get values from source dataset in question, excluding
      // transformationName and any other filled-in values.
      // Construct name for new target dataset.
      if(targetFields[j].equalsIgnoreCase(datasetTransformationReference[1]) ||
          targetFields[j].equalsIgnoreCase(datasetTransformationVersionReference[1])){
      }
      else if(targetFields[j].equalsIgnoreCase(
          MyUtil.getNameField(targetDBPluginMgr.getDBName(), "dataset"))){
        targetAttr[j] = dbPluginMgr.getTargetDatasetName(
            targetDB, dbPluginMgr.getDatasetName(datasetIDs[0]),
            transformationName, transformationVersion);
      }
      //else if(targetFields[j].equalsIgnoreCase("runNumber")){
      //  targetAttr[j] = dbPluginMgr.getRunNumber(datasetIDs[0]);
      //}
      else if(targetFields[j].equalsIgnoreCase("InputDataset")){
        targetAttr[j] = dbPluginMgr.getDatasetName(datasetIDs[0]);
      }
      else if(targetFields[j].equalsIgnoreCase("InputDB")){
        targetAttr[j] = dbPluginMgr.getDBName();
      }
      else if(targetFields[j].equalsIgnoreCase("identifier") ||
          targetFields[j].equalsIgnoreCase("percentageValidatedFiles") ||
          targetFields[j].equalsIgnoreCase("percentageFailedFiles ") ||
          //targetFields[j].equalsIgnoreCase("totalFiles") ||
          //targetFields[j].equalsIgnoreCase("totalEvents") ||
          targetFields[j].equalsIgnoreCase("averageEventSize") ||
          targetFields[j].equalsIgnoreCase("totalDataSize") ||
          targetFields[j].equalsIgnoreCase("averageCPUTime") ||
          targetFields[j].equalsIgnoreCase("totalCPUTime") ||
          targetFields[j].equalsIgnoreCase("created") ||
          targetFields[j].equalsIgnoreCase("lastModified") ||
          targetFields[j].equalsIgnoreCase("lastStatusUpdate")){
        targetAttr[j] = "";
      }
      // For unset fields:
      // see if attribute is in target dataset and set. If not, ignore.
      else if(targetAttr[j]==null || targetAttr[j].equals("")){
        boolean fieldPresent = false;
        DBRecord inputDataset0 = dbPluginMgr.getDataset(datasetIDs[0]);
        for(int l=0; l<sourceFields.length; ++l){
          if(targetFields[j].equalsIgnoreCase(sourceFields[l])){
            fieldPresent = true;
            break;
          }
        }
        if(fieldPresent){
          try{
            if(inputDataset0.getValue(targetFields[j])!=null){
              targetAttr[j] = inputDataset0.getValue(targetFields[j]).toString();
            }
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    }
    
    cstAttributesNames = targetFields;
    cstAttr = targetAttr;
    tcCstAttributes = null;
    Debug.debug("initAttributePanel", 3);
    initAttributePanel();
    
    for(int i=0; i<tcCstAttributes.length; ++i){
      Debug.debug("Setting "+targetFields[i]+"->"+targetAttr[i], 3);
      MyUtil.setJText(tcCstAttributes[i], targetAttr[i]);
      if((cstAttributesNames[i].equalsIgnoreCase("runNumber") ||
          cstAttributesNames[i].equalsIgnoreCase("InputDataset") ||
          cstAttributesNames[i].equalsIgnoreCase("InputDB") ||
          cstAttributesNames[i].equalsIgnoreCase(datasetTransformationReference[1]) ||
          cstAttributesNames[i].equalsIgnoreCase(datasetTransformationVersionReference[1]))){         
        try{
          MyUtil.setJEditable(tcCstAttributes[i], false);
        }
        catch(java.lang.Exception e){
          Debug.debug("Attribute not found, "+e.getMessage(),1);
        }
      }
    }

    transformations = targetDBPluginMgr.getTransformations();    
    Debug.debug("initAttributePanel done, setting values, "+
        targetAttr.length+", "+tcCstAttributes.length, 3);
    initTransformationPanel();
    pTop.updateUI();
    
  }
  
}
