package gridpilot;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkListener;

import java.awt.*;
import java.io.File;
import java.util.*;

import javax.swing.text.*;

/**
 * This panel creates records in the DB table.
 * It's shown inside the CreateEditDialog.
 */
public class TransformationCreationPanel extends CreateEditPanel{

  private static final long serialVersionUID = 1L;
  private DBPluginMgr dbPluginMgr;
  private StatusBar statusBar;
  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private boolean editing = false;
  private Table table;
  private String transformationID = "-1";
  private String [] cstAttributesNames;
  private String [] cstAttr = null;
  private String transformationIdentifier;
  private static int TEXTFIELDWIDTH = 32;
  private boolean reuseTextFields = true;
  private Vector tcConstant = new Vector(); // contains all text components
  private DBPanel panel = null;
  private JPanel pRuntimeEnvironment = new JPanel();
  private String runtimeEnvironmentName = null;
  private JComboBox cbRuntimeEnvironmentSelection;
  private GridBagConstraints ct = new GridBagConstraints();
  private Database.DBRecord transformation = null;
  Database.DBResult runtimeEnvironments = null;
  private String [] transformationFields = null;

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
    statusBar = GridPilot.getClassMgr().getStatusBar();
    transformationIdentifier = "identifier";
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
      transformation = dbPluginMgr.getTransformation(Integer.parseInt(transformationID));
      for(int i=0; i<cstAttributesNames.length; ++i){
        if(editing){
          Debug.debug("filling " + cstAttributesNames[i],  3);
          if(transformation.getValue(cstAttributesNames[i])!=null){
            cstAttr[i] = transformation.getValue(cstAttributesNames[i]).toString();
            if(cstAttributesNames[i].equalsIgnoreCase("runtimeEnvironment")){
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
    
    spAttributes.setPreferredSize(new Dimension(650, 500));
    spAttributes.setMinimumSize(new Dimension(650, 500));
    
    setLayout(new GridBagLayout());
    removeAll();

    initRuntimeEnvironmentPanel(Integer.parseInt(transformationID));

    initAttributePanel();
    
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
            cstAttributesNames[i].equalsIgnoreCase("runtimeEnvironment")){
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

    add(spAttributes,   new GridBagConstraints(0, 3, 3, 1, 0.9, 0.9
        ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

    initAttributePanel();
    
  }

  private JEditorPane createCheckPanel(
      final String name, final JTextComponent jt,
      final DBPluginMgr dbPluginMgr){
    final Frame frame = (Frame) SwingUtilities.getWindowAncestor(getRootPane());
    String markup = "<b>"+name+" : </b><br>"+
      "<a href=\"http://check/\">check</a>";
    JEditorPane checkPanel = new JEditorPane("text/html", markup);
    checkPanel.setEditable(false);
    checkPanel.setOpaque(false);
    checkPanel.addHyperlinkListener(
      new HyperlinkListener(){
      public void hyperlinkUpdate(HyperlinkEvent e){
        if(e.getEventType()==HyperlinkEvent.EventType.ACTIVATED){
          Debug.debug("URL: "+e.getURL().toExternalForm(), 3);
          if(e.getURL().toExternalForm().equals("http://check/")){
            String httpScript = jt.getText();
            String url = null;
            try{
              if(httpScript.startsWith("/")){
                url = (new File(httpScript)).toURL().toExternalForm();
              }
              else if(httpScript.startsWith("file://")){
                url = (new File(httpScript.substring(6))).toURL().toExternalForm();
              }
              else if(httpScript.startsWith("file://")){
                url = (new File(httpScript.substring(5))).toURL().toExternalForm();
              }
              else{
                url = httpScript;
              }
            }
            catch(Exception ee){
              Debug.debug("Could not open URL "+httpScript+". "+ee.getMessage(), 1);
              ee.printStackTrace();
              GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL "+httpScript);
              return;
            }
            Debug.debug("URL: "+url, 3);
            setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            final String finUrl = url;
            final String finBaseUrl = "";//url;
            MyThread t = new MyThread(){
              public void run(){
                WebBox wb = null;
                try{
                  wb = new WebBox(//GridPilot.getClassMgr().getGlobalFrame(),
                                  frame,
                                  "Choose script",
                                  finUrl,
                                  finBaseUrl,
                                  true,
                                  true);
                }
                catch(Exception eee){
                  Debug.debug("Could not open URL "+finBaseUrl+". "+eee.getMessage(), 1);
                  GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL "+finBaseUrl+". "+eee.getMessage());
                  ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()/*,"",""*/); 
                  try{
                    confirmBox.getConfirm("URL not found",
                                         "The URL "+finBaseUrl+" was not found. "+eee.getMessage(),
                                      new Object[] {"OK"});
                  }
                  catch(Exception eeee){
                    Debug.debug("Could not get confirmation, "+eeee.getMessage(), 1);
                  }
                }
                if(wb!=null && wb.lastURL!=null &&
                    wb.lastURL.startsWith(finBaseUrl)){
                  // Set the text: the URL browsed to with case URL removed
                  jt.setText(wb.lastURL.substring(
                      finBaseUrl.length()));
                  statusBar.setLabel("");
                }
                else{
                  // Don't do anything if we cannot get a URL
                  Debug.debug("ERROR: Could not open URL "+finBaseUrl, 1);
                }
                setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
                statusBar.setLabel("");
              }
            };
            t.start();
          }
        }
      }
    });
    return checkPanel;
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

    updateUI();
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
    
    if(!reuseTextFields || tcCstAttributes==null || tcCstAttributes.length!=cstAttributesNames.length)
      tcCstAttributes = new JTextComponent[cstAttributesNames.length];

    int row = 0;
    
    //// Constants attributes
    for(int i = 0; i<cstAttributesNames.length; ++i, ++row){
      if(cstAttributesNames[i].equalsIgnoreCase("definition") ||
          cstAttributesNames[i].equalsIgnoreCase("code") ||
         cstAttributesNames[i].equalsIgnoreCase("script") ||
         cstAttributesNames[i].equalsIgnoreCase("validationScript") ||
         cstAttributesNames[i].equalsIgnoreCase("extractionScript")){
        pAttributes.add(createCheckPanel(
            cstAttributesNames[i], tcCstAttributes[i],
            dbPluginMgr),
            new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 25, 5, 5), 0, 0));
      }
      else{
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "),
            new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH,
            new Insets(5, 25, 5, 5), 0, 0));
      }
      
      if(!reuseTextFields || tcCstAttributes[i]==null || !tcCstAttributes[i].isEnabled()){
        tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
      }
      if(cstAttributesNames[i].equalsIgnoreCase("runtimeEnvironment")){
        Util.setJEditable(tcCstAttributes[i], false);
      }
      pAttributes.add(tcCstAttributes[i],
          new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
          GridBagConstraints.CENTER,
          GridBagConstraints.HORIZONTAL,
          new Insets(5, 5, 5, 5), 0, 0));
    }
  }

  /**
   *  Edit a transformation
   */
  public void editTransformation(int transformationID,
      String runtimeEnvironmentName){

    //// Constants attributes
    for(int i =0; i<tcCstAttributes.length; ++i){
      for(int j=0; j<transformationFields.length;++j){
        if(transformationFields[j].toString().equalsIgnoreCase(
            cstAttributesNames[i].toString()) &&
            !transformationFields[j].toString().equals("")){
          if(tcCstAttributes[i]==null || !tcCstAttributes[i].isEnabled() &&
             tcCstAttributes[i].getText().length()==0){
            tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
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
      else if(runtimeEnvironmentName!=null && !runtimeEnvironmentName.equals("") &&
          cstAttributesNames[i].equalsIgnoreCase("runtimeEnvironment")){
        Util.setJText(tcCstAttributes[i], runtimeEnvironmentName);
        Debug.debug("Setting selection to "+runtimeEnvironmentName, 3);
        cbRuntimeEnvironmentSelection.setSelectedItem(runtimeEnvironmentName);
        cbRuntimeEnvironmentSelection.updateUI();
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

    new TransformationCreator(
        dbPluginMgr,
        showResults,
        cstAttr,
        cstAttributesNames,
        editing);
    
    panel.refresh();
  }

  private Vector getTextFields(){
    Vector v = new Vector();

    v.addAll(tcConstant);

    for(int i=0; i<tcCstAttributes.length; ++i)
      v.add(tcCstAttributes[i]);

    return v;
  }
}
