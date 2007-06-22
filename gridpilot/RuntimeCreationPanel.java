package gridpilot;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.text.*;
import java.awt.*;
import java.util.*;

/**
 * This panel creates records in the DB table. It's shown inside the CreateEditDialog.
 */
public class RuntimeCreationPanel extends CreateEditPanel{

  private static final long serialVersionUID = 1L;
  private DBPluginMgr dbPluginMgr;
  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private boolean editing = false;
  private Table table;
  private String packID = "-1";
  private String [] cstAttributesNames;
  private String [] cstAttr = null;
  private String packIdentifier;
  private boolean reuseTextFields = true;  
  private Vector tcConstant = new Vector(); // contains all text components
  private static int TEXTFIELDWIDTH = 32;
  private DBPanel panel = null;
  private DBRecord pack = null;
  
  public JTextComponent [] tcCstAttributes;
  
  /**
   * Constructor
   */
  public RuntimeCreationPanel(DBPluginMgr _dbPluginMgr, DBPanel _panel,
      boolean _editing){
    dbPluginMgr = _dbPluginMgr;
    editing = _editing;
    panel = _panel;
    table = panel.getTable();
    packIdentifier = "identifier";
    cstAttributesNames = dbPluginMgr.getFieldNames("runtimeEnvironment");
    Debug.debug("Got field names: "+Util.arrayToString(cstAttributesNames),3);
    cstAttr = new String[cstAttributesNames.length];
    Debug.debug("edit? "+table.getSelectedRow()+":"+ editing, 3);
    // Find pack ID from table
    if(table.getSelectedRow()>-1 && editing){
      Debug.debug("Editing... "+table.getColumnNames().length+" : "+Util.arrayToString(table.getColumnNames()), 3);
      for(int i=0; i<table.getColumnNames().length; ++i){
        Object fieldVal = table.getUnsortedValueAt(table.getSelectedRow(),i);
        Debug.debug("Column name: "+table.getColumnNames().length+":"+i+" "+table.getColumnName(i), 3);
        if(fieldVal!=null && table.getColumnName(i).equalsIgnoreCase(packIdentifier)){
          packID = fieldVal.toString();
          break;
        }
      }
      if(packID==null || packID.equals("-1") ||
          packID.equals("")){
        Debug.debug("ERROR: could not find packID in table!", 1);
      }
      // Fill cstAttr from db
      pack = dbPluginMgr.getRuntimeEnvironment(packID);
      for(int i=0; i <cstAttributesNames.length; ++i){
        if(editing){
          Debug.debug("filling " + cstAttributesNames[i],  3);
          if(pack.getValue(cstAttributesNames[i])!=null){
            cstAttr[i] = pack.getValue(cstAttributesNames[i]).toString();
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
        Color.white, new Color(165, 163, 151)),
        (packID.equals("-1")?"new runtime environment":"runtimeEnvironment "+packID)));
    
    //spAttributes.setPreferredSize(new Dimension(600, 500));
    //spAttributes.setMinimumSize(new Dimension(600, 500));
    
    setLayout(new GridBagLayout());
    removeAll();

    initAttributePanel();
    
    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.VERTICAL;
    ct.insets = new Insets(2,2,2,2);
    
    ct.gridx = 0;
    ct.gridy = 1;
    ct.gridwidth=2;
    ct.gridheight=1;
    add(spAttributes, ct);

    Debug.debug("Initializing panel", 3);
    initPackCreationPanel();
    if(editing){
      Debug.debug("Editing...", 3);
      editPack(Integer.parseInt(packID));
    }
    else{
      Debug.debug("Disabling identifier field", 3);
      // Disable identifier field when creating
      for(int i =0; i<cstAttributesNames.length; ++i){
        if(cstAttributesNames[i].equalsIgnoreCase(packIdentifier) ||
           cstAttributesNames[i].equalsIgnoreCase("created") ||
           cstAttributesNames[i].equalsIgnoreCase("lastModified")){
          Util.setJEditable(tcCstAttributes[i], false);
        }
      }
    }
    updateUI();
   }


  /**
   * Called initially.
   * Initialises text fields with attributes for runtime environment.
    */
  private void initPackCreationPanel(){

    // Panel Attributes
    pAttributes.setLayout(new GridBagLayout());
    pAttributes.removeAll();

    spAttributes.getViewport().add(pAttributes, null);

    add(spAttributes, new GridBagConstraints(0, 3, 3, 1, 0.9, 0.9,
        GridBagConstraints.CENTER, GridBagConstraints.BOTH,
        new Insets(0, 0, 0, 0), 0, 0));

    initAttributePanel();
    
  }
  
  private void initAttributePanel(){
    
    if(!reuseTextFields || tcCstAttributes==null || tcCstAttributes.length != cstAttributesNames.length)
      tcCstAttributes = new JTextComponent[cstAttributesNames.length];
    int row = 0;
    for(int i = 0; i<cstAttributesNames.length; ++i, ++row){
      if(cstAttributesNames[i].equalsIgnoreCase("scriptRepository")){
           pAttributes.add(Util.createCheckPanel(
               (JFrame) SwingUtilities.getWindowAncestor(getRootPane()),
               cstAttributesNames[i], tcCstAttributes[i], true, true),
               new GridBagConstraints(0, row, 1, 1, 0.0, 0.0,
                   GridBagConstraints.CENTER,
                   GridBagConstraints.BOTH,
                   new Insets(5, 25, 5, 5), 0, 0));
      }
      else{
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "),
            new GridBagConstraints(0, row, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));        
        if(!editing && !reuseTextFields ||
            tcCstAttributes[i]==null){
          if(cstAttributesNames[i].toString().equalsIgnoreCase("initLines") ||
              cstAttributesNames[i].toString().equalsIgnoreCase("certificate")){
            tcCstAttributes[i] = Util.createTextArea(TEXTFIELDWIDTH);
          }
          else{
            tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
          }
        }
        if(cstAttr[i]!=null && !cstAttr[i].equals("")){
          Debug.debug("Setting cstAttr["+i+"]: "+cstAttr[i], 3);
          Util.setJText(tcCstAttributes[i], cstAttr[i]);
        }
        // when creating, zap loaded dataset id
        if(!editing && cstAttributesNames[i].toString().equalsIgnoreCase("identifier")){
          Util.setJText(tcCstAttributes[i], "");
        }
      }
      
      if(!reuseTextFields || tcCstAttributes[i]==null || !tcCstAttributes[i].isEnabled())
        tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
  
      pAttributes.add(tcCstAttributes[i],
          new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
          GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
          new Insets(5, 5, 5, 5), 0, 0));

    }
  }

  /**
   *  Edit a runtime environment
   */
  public void editPack(int packID){
    for(int i =0; i<tcCstAttributes.length; ++i){
      Debug.debug("Length of res: "+
          pack.fields.length+":"+tcCstAttributes.length, 3);
      for(int j=0; j<pack.fields.length;++j){
        //Debug.debug(res.fields[j].toString()+" <-> "+cstAttributesNames[i].toString(),3);
        if(pack.fields[j].toString().equalsIgnoreCase(
            cstAttributesNames[i].toString()) &&
            !pack.fields[j].toString().equals("")){
          // Initilize non-initialized fields
          if(tcCstAttributes[i]==null || !tcCstAttributes[i].isEnabled() &&
             tcCstAttributes[i].getText().length()==0){
            if(cstAttributesNames[i].toString().equalsIgnoreCase("initLines")){
              tcCstAttributes[i] = Util.createTextArea(TEXTFIELDWIDTH);
            }
            else{
              tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
            }
            pAttributes.add(tcCstAttributes[i], new GridBagConstraints(1, i/*row*/, 3, 1, 1.0, 0.0
               ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
          }
          try{
            tcCstAttributes[i].setText(pack.values[j].toString());
                Debug.debug(i+": "+cstAttributesNames[i].toString()+"="+pack.fields[j]+". Setting to "+tcCstAttributes[i].getText(),3);
          }
          catch(java.lang.Exception e){
            Debug.debug("Attribute not found, "+e.getMessage(),1);
          }
          break;
        }
      }
      if(cstAttributesNames[i].equalsIgnoreCase(packIdentifier) ||
          cstAttributesNames[i].equalsIgnoreCase("created") ||
          cstAttributesNames[i].equalsIgnoreCase("lastModified")){
        Util.setJEditable(tcCstAttributes[i], false);
        Debug.debug("Disabling identifier "+cstAttributesNames[i],3);
        if(!editing){
          try{
            Debug.debug("Clearing identifier",3);
            tcCstAttributes[i].setText("");
          }
          catch(java.lang.Exception e){
            Debug.debug("Attribute not found, "+e.getMessage(),1);
          }
        }
      }
    }
  }

  public void clearPanel(){
    Vector textFields = getTextFields();
    for(int i =0; i<textFields.size(); ++i){
      if(!(cstAttributesNames[i].equalsIgnoreCase("identifier"))){
        ((JTextComponent) textFields.get(i)).setText("");
      }
    }
  }


  public void create(final boolean showResults, final boolean editing){

    for(int i=0; i<cstAttr.length; ++i){
      cstAttr[i] = tcCstAttributes[i].getText();
      Debug.debug("Field text: "+cstAttr[i], 3);
    }

    Debug.debug("create runtineEnvironment",  1);

    RuntimeCreator rtf = new RuntimeCreator(
        dbPluginMgr,
        showResults,
        cstAttr,
        cstAttributesNames,
        editing);
    
    if(rtf.anyCreated){
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
