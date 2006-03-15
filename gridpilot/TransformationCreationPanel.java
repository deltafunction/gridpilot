package gridpilot;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkListener;

import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.net.URL;
import java.util.*;

import javax.swing.text.*;

/**
 * This panel creates records in the DB table.
 * It's shown inside the CreateEditDialog.
 */
public class TransformationCreationPanel extends CreateEditPanel{

  private DBPluginMgr dbPluginMgr;
  private StatusBar statusBar;
  private String version = "";
  private String transformation;
  private JPanel pConstants = new JPanel(new GridBagLayout());
  private JPanel pAttributes = new JPanel();
  private JScrollPane spAttributes = new JScrollPane();
  private JPanel pButtons = new JPanel(new GridBagLayout());
  private boolean editing = false;
  private Table table;
  private String transformationID = "-1";
  private String [] cstAttributesNames;
  private String [] cstAttr = null;
  private String transformationIdentifier;
  private static int TEXTFIELDWIDTH = 32;
  private boolean reuseTextFields = true;
  private Map id = new HashMap();
  private Vector tcConstant = new Vector(); // contains all text components
  private static WebBox wb;

  public JTextComponent [] tcCstAttributes;

  /**
   * Constructor
   */
  public TransformationCreationPanel(DBPluginMgr _dbPluginMgr, Table _table, boolean _editing){
    dbPluginMgr = _dbPluginMgr;
    editing = _editing;
    table = _table;
    statusBar = GridPilot.getClassMgr().getStatusBar();

    transformationIdentifier = "identifier";
    
    cstAttributesNames = dbPluginMgr.getFieldNames("transformation");
    Debug.debug("Got field names: "+Util.arrayToString(cstAttributesNames),3);

    //initGUI();
    
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
      Database.DBRecord transformation = dbPluginMgr.getTransformation(Integer.parseInt(transformationID));
      for(int i=0; i <cstAttributesNames.length; ++i){
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
  }

  /**
   * GUI initialisation
   */

  public void initGUI(){
    
    Debug.debug("Initializing GUI", 3);

    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white,new Color(165, 163, 151)), 
        (transformationID.equals("-1")?"new transformation":"transformation "+transformationID)));
    
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
    
    ct.gridx = 0;
    ct.gridy = 1;
    ct.gridwidth=2;
    add(spAttributes,ct);

    Debug.debug("Initializing panel", 3);
    initTransformationCreationPanel();
    if(editing){
      Debug.debug("Editing...", 3);
      editTransformation(Integer.parseInt(transformationID));
    }
    else{
      Debug.debug("Disabling identifier field", 3);
      // Disable identifier field when creating
      for(int i =0; i<cstAttributesNames.length; ++i){
        if(cstAttributesNames[i].equalsIgnoreCase(transformationIdentifier)){
          tcCstAttributes[i].setEnabled(false);
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


  private JEditorPane createCheckPanel(final String name, final JTextComponent jt){
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
            String httpscript = Util.getURL(jt.getText());
            if(statusBar != null){
              statusBar.setLabel("Looking for file ...");
              statusBar.animateProgressBar();
              statusBar.setIndeterminateProgressBarToolTip("click here to cancel");
              statusBar.addIndeterminateProgressBarMouseListener(new MouseAdapter(){
                public void mouseClicked(MouseEvent e){
                  wb.dispose();
                  statusBar.setLabel("Search interrupted.");
                  statusBar.stopAnimation();
                }
              });
            }
            try{
              wb = new WebBox(GridPilot.getClassMgr().getGlobalFrame(), "choose script",
                 new URL(httpscript));
            }
            catch(Exception ee){
              Debug.debug("Could not open URL "+httpscript+". "+ee.getMessage(), 1);
              statusBar.stopAnimation();
              GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL "+httpscript);
              try{
                wb = new WebBox(GridPilot.getClassMgr().getGlobalFrame(), "choose script",
                    new URL(GridPilot.url));
              }
              catch(Exception eee){
                Debug.debug("Could not open URL "+GridPilot.url+". "+eee.getMessage(), 1);
              }
            }
            statusBar.stopAnimation();
            if(GridPilot.lastURL!=null && GridPilot.lastURL.toExternalForm().startsWith(GridPilot.url)){
              // Set the text: the URL browsed to with AtCom.url removed
              jt.setText(GridPilot.lastURL.toExternalForm().substring(
                  GridPilot.url.length()));
            }
            else{
              // Don't do anything if we cannot get a URL
              Debug.debug("ERROR: Could not open URL "+GridPilot.url+". "+GridPilot.lastURL, 1);
            }
            statusBar.setLabel("");
          }
        }
      }
    });
    return checkPanel;
  }
  
  private void initAttributePanel(){
    
    if(!reuseTextFields || tcCstAttributes==null || tcCstAttributes.length != cstAttributesNames.length)
      tcCstAttributes = new JTextComponent[cstAttributesNames.length];

    int row = 0;
    
    //// Constants attributes
    for(int i = 0; i<cstAttributesNames.length; ++i, ++row){
      
      if(cstAttributesNames[i].equalsIgnoreCase("definition") ||
         cstAttributesNames[i].equalsIgnoreCase("valScript") ||
         cstAttributesNames[i].equalsIgnoreCase("xtractScript") ||
         cstAttributesNames[i].equalsIgnoreCase("code") ||
         cstAttributesNames[i].equalsIgnoreCase("script") ||
         cstAttributesNames[i].equalsIgnoreCase("validationScript") ||
         cstAttributesNames[i].equalsIgnoreCase("extractionScript")){
        pAttributes.add(createCheckPanel(cstAttributesNames[i], tcCstAttributes[i]), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
            ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));
      }
      else{
        pAttributes.add(new JLabel(cstAttributesNames[i] + " : "), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
            ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));
      }
    	
	    if(!reuseTextFields || tcCstAttributes[i]==null || !tcCstAttributes[i].isEnabled())
	      tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
	
	    pAttributes.add(tcCstAttributes[i], new GridBagConstraints(1, row, 3, 1, 1.0, 0.0
	        ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

    }
  }

  /**
   *  Edit a transformation
   */

  public void editTransformation(int transformationID){

    int row = 0;

    //// Constants attributes
    Database.DBRecord res = dbPluginMgr.getTransformation(transformationID);
    for(int i =0; i<tcCstAttributes.length; ++i){
      Debug.debug("Length of res: "+res.fields.length,3);
      for(int j=0; j<res.fields.length;++j){
        //Debug.debug(res.fields[j].toString()+" <-> "+cstAttributesNames[i].toString(),3);
        if(res.fields[j].toString().equals(cstAttributesNames[i].toString()) && !res.fields[j].toString().equals("")){
          if(tcCstAttributes[i]==null || !tcCstAttributes[i].isEnabled() &&
             tcCstAttributes[i].getText().length()==0){
            tcCstAttributes[i] = new JTextField("", TEXTFIELDWIDTH);
            pAttributes.add(tcCstAttributes[i], new GridBagConstraints(1, i/*row*/, 3, 1, 1.0, 0.0
               ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
          }
        try{
          tcCstAttributes[i].setText(res.values[j].toString());
              Debug.debug(i+": "+cstAttributesNames[i].toString()+"="+res.fields[j]+". Setting to "+tcCstAttributes[i].getText(),3);
        }
        catch(java.lang.Exception e){Debug.debug("Attribute not found, "+e.getMessage(),1);}
          break;
        }
      }
      if((cstAttributesNames[i].equals("identifier"))){         
        tcCstAttributes[i].setEditable(false);
        tcCstAttributes[i].setBackground(Color.lightGray);
        //tcCstAttributes[i].setEnabled(false);
        id.put(cstAttributesNames[i].toString(), Integer.toString(i));
        if(cstAttributesNames[i].equals("datasetFK")){
          try{
            tcCstAttributes[i].setText(Integer.toString(transformationID));
          }
          catch(java.lang.Exception e){Debug.debug("Attribute not found, "+e.getMessage(),1);}
        }
        if(cstAttributesNames[i].equals("identifier") &&
            res.values.length != 1){
          try{
            Debug.debug("Clearing identifier",3);
            tcCstAttributes[i].setText("");
          }
          catch(java.lang.Exception e){Debug.debug("Attribute not found, "+e.getMessage(),1);}
        }
      }
    }
  }

  /**
   *  Delete a transformation
   */

  public boolean deleteTransformation(int transformationIdentifier){
  	boolean skip = false;
  
  	int choice = 1;
  	  
  	  ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()/*,"",""*/);	
  	  
  	  try{
  		 choice = confirmBox.getConfirm("Confirm delete",
  						  "Really delete transformation # "+transformationIdentifier+"?",
  						   new Object[] {"OK", "Cancel"});
  	  }catch(java.lang.Exception e){Debug.debug("Could not get confirmation, "+e.getMessage(),1);}
  
  	  switch(choice){
  		case 0  : skip = false;  break;  // OK
  		case 1  : skip = true ; break;   // Skip
  		default : skip = true;    // other (closing the dialog). Same action as "Skip"
  	  }
  	  
  	if(!skip){
  	  Debug.debug("deleting transformation # " + transformationIdentifier, 2);
  	  if(dbPluginMgr.deleteTransformation(transformationIdentifier)){
        statusBar.setLabel("Transformation # " + transformationIdentifier + " deleted.");
        return true;
      }
      else{
        statusBar.setLabel("Transformation # " + transformationIdentifier + " NOT deleted.");
      }
  	}
    return false;
  }

  public void clear(){

    Vector textFields = getTextFields();

    for(int i =0; i<textFields.size(); ++i)
	if(!(cstAttributesNames[i].equals( "identifier"))){
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
  }

  private Vector getTextFields(){
    Vector v = new Vector();

    v.addAll(tcConstant);

    for(int i=0; i<tcCstAttributes.length; ++i)
      v.add(tcCstAttributes[i]);

    return v;
  }
}
