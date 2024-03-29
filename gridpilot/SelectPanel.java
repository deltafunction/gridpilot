package gridpilot;

import gridfactory.common.Debug;

import javax.swing.*;

import java.awt.*;

import javax.swing.border.*;
import java.awt.event.*;


/**
 * GUI for creation of selection requests.
 * The request is composed using the drop-down menus and text fields,
 * represented using simple internal format, which is parsed into the
 * format used by the active DB plugin, using the method <code>ParseSelectRequest</code>
 * of the DB plugin.
 *
 * One instance of this class is created by each DBPanel instance.
 *
 */
public class SelectPanel extends JPanel{

  private static final long serialVersionUID = 1L;
  // lists of field names with table name as key
  private String [] fieldNames = null;
  private String tableName;
  private String [] relationNames = {"=", "CONTAINS", "<", ">", "!="};
  private GridBagConstraints gbcVC;
  private SPanel.ConstraintPanel spcp;
  private SPanel sPanel;
  private static final int SEARCH_FIELD_SIZE = 42;
  private KeyAdapter enterKeyAdapter = null;

  /**
  * Constructors
  */

  /**
   * Creates a SelectPanel, with n tables.
   * Called by : DBPanel.DBPanel()
   */
  public SelectPanel(String _tableName, String [] _fieldNames)
      throws Exception{
    fieldNames = _fieldNames;
    tableName = _tableName;
  }
  
 /**
  * GUI Initialisation
  */  
  public void initGUI() throws Exception {
    
    gbcVC = new GridBagConstraints();
    gbcVC.fill = GridBagConstraints.VERTICAL;
    
    this.setLayout(new GridBagLayout());
      
    Debug.debug("creating sPanel", 2);
    sPanel = new SPanel(tableName, fieldNames);
   
    sPanel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
       Color.white,new Color(165, 163, 151)),this.getName()));
     
   
    Debug.debug("Created SPanel with " + ((JPanel) sPanel.getComponent(1)).getComponentCount() + " components", 3);
             
    gbcVC.gridx = 0;
    gbcVC.gridy = 0;
    gbcVC.anchor = GridBagConstraints.NORTHWEST;
    this.add(sPanel, gbcVC);
    
    sPanel.setName(tableName);
  }

  /**
   * Reads the selection panel and returns a representation of the selection in
   * basic SQL. Uses shownFields for SELECT, the selection for WHERE.
   */
  public String getRequest(String [] shownFields){
    String ret = "SELECT ";
    Debug.debug("Checking "+sPanel.spDisplayList.getComponentCount()+":"+shownFields.length, 3);
    for(int i=0; i<sPanel.spDisplayList.getComponentCount(); ++i){
      SPanel.DisplayPanel cb = ((SPanel.DisplayPanel) sPanel.spDisplayList.getComponent(i));
      for(int j=0; j<shownFields.length; ++j){
        Debug.debug("Checking fields in getRequest "+tableName+"."+
            cb.cbDisplayAttribute.getSelectedItem().toString()+"<->"+shownFields[j], 3);
        if(shownFields[j].equals(tableName+".*") ||
            (tableName+"."+cb.cbDisplayAttribute.getSelectedItem().toString()
                ).equalsIgnoreCase(shownFields[j])){
          if(i>0 && !cb.cbDisplayAttribute.getSelectedItem().toString().equals("")){
            ret += ", ";
          }
          ret += cb.cbDisplayAttribute.getSelectedItem();
          break;
        }
      }
    }    
    ret += " FROM " + tableName;
    if(sPanel.spConstraintList.getComponentCount() > 0 &&
        !((SPanel.ConstraintPanel) sPanel.spConstraintList.getComponent(0)).tfConstraintValue.getText().equals("")){
      ret += " WHERE ";
    }
    for(int i=0; i<sPanel.spConstraintList.getComponentCount();  ++i){
      SPanel.ConstraintPanel cb =
        ((SPanel.ConstraintPanel) sPanel.spConstraintList.getComponent(i));
      if(!cb.tfConstraintValue.getText().equals("")){
        if(i>0){
          ret += " AND ";
        }
        ret += cb.cbConstraintAttribute.getSelectedItem() + " ";
        ret += cb.cbConstraintRelation.getSelectedItem() + " ";
        ret += cb.tfConstraintValue.getText();
      }
    }
    Debug.debug("Search request: " + ret, 3);
    return ret;
  }
  
  /**
   * Initialises the panel # 'panel'.
   * Called by : this.initGUI()
   */
  protected class SPanel extends JPanel{
    private static final long serialVersionUID = 1L;
    private JButton bAddConstraintRow;
    private JButton bRemoveConstraintRow ;
    private JPanel spConstraintList;
    private JPanel spConstraints;
    private JButton bAddDisplayRow;
    private JButton bRemoveDisplayRow;
    private JPanel spDisplays;
    private JPanel spDisplayList;
    private String [] fieldList;
    private String name = "";
    
    private void initButtons(){
      bAddConstraintRow = MyUtil.mkButton1("more.png", "Add another constraint", "+");
      bAddDisplayRow = MyUtil.mkButton1("more.png", "Add another column to display", "+");
      bRemoveConstraintRow = MyUtil.mkButton1("less.png", "Remove a constraint", "-");
      bRemoveDisplayRow = MyUtil.mkButton1("less.png", "Remove a column from display", "-");
    }

       
    public SPanel(String _name, String [] _fieldList){
      name = _name;
      fieldList = _fieldList;
      initButtons();
      spConstraints = new JPanel();
      spConstraintList = new JPanel();
      spDisplayList = new JPanel();
      spDisplays = new JPanel();
      
      this.setLayout(new GridBagLayout());
      
      spConstraints.setLayout(new FlowLayout(FlowLayout.LEFT, 4, 14));
        
      //// Add constraints
      //Label
      spConstraints.add(new JLabel("Constraints :"));

      // Button More
      bAddConstraintRow.addActionListener(new java.awt.event.ActionListener() {
        public void actionPerformed(ActionEvent e) { bAddConstraintRow_actionPerformed();}});
  
      spConstraints.add(bAddConstraintRow);
  
      // Button Less
      bRemoveConstraintRow.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){bRemoveConstraintRow_actionPerformed(); }});
      bRemoveConstraintRow.setEnabled(false);
  
      spConstraints.add(bRemoveConstraintRow);
      
      gbcVC.gridx = 0;
      gbcVC.gridy = 0;
      gbcVC.anchor = GridBagConstraints.WEST;
      this.add(spConstraints, gbcVC);
      spcp = new ConstraintPanel();
      spConstraintList.add(spcp);
      gbcVC.gridx = 1;
      gbcVC.gridy = 0;
      this.add(spConstraintList, gbcVC);
                   
      //// Display attributes
    
      // Label
      spDisplays.add(new JLabel("Show :         "));
  
      // Button More
      bAddDisplayRow.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){bAddDisplayRow_actionPerformed();
      }});
  
      spDisplays.add(bAddDisplayRow);
 
      // Button Less
      bRemoveDisplayRow.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){bRemoveDisplayRow_actionPerformed();
      }});
  
      spDisplays.add(bRemoveDisplayRow);
      bRemoveDisplayRow.setEnabled(false);
      
      gbcVC.gridx = 0;
      gbcVC.gridy = 1;
      this.add(spDisplays, gbcVC);
      spDisplayList.add(new DisplayPanel(true));
      gbcVC.gridx = 1;
      gbcVC.gridy = 1;
      this.add(spDisplayList, gbcVC);
    }
   
    protected class ConstraintPanel extends JPanel{
      private static final long serialVersionUID = 1L;
      private JComboBox cbConstraintAttribute;
      private JComboBox cbConstraintRelation;
      private JTextField tfConstraintValue;
      ConstraintPanel(){
        // Combobox attribute
        if(fieldList==null){
          Debug.debug("fieldlist null", 2);
          return;     
        }
        if(relationNames==null){
          Debug.debug("relationNames null", 2);
          return;
        }
        cbConstraintAttribute = new JComboBox();
        for(int i=0;i<fieldList.length; ++i){
          cbConstraintAttribute.insertItemAt(fieldList[i], i);
        }
        cbConstraintAttribute.setSelectedIndex(0);
        this.add(cbConstraintAttribute);
        // Combobox relation
        cbConstraintRelation = new JComboBox();
        for(int i=0; i<relationNames.length; ++i){
          cbConstraintRelation.insertItemAt(relationNames[i], i);
        }
        cbConstraintRelation.setSelectedIndex(0);
        this.add(cbConstraintRelation);
        // Textfield value
        tfConstraintValue = new JTextField(SEARCH_FIELD_SIZE);
        this.add(tfConstraintValue);
        if(enterKeyAdapter!=null){
          this.tfConstraintValue.addKeyListener(enterKeyAdapter);
        }
      }
    }        
     
    protected class DisplayPanel extends JPanel{
      private static final long serialVersionUID = 1L;
      private JComboBox cbDisplayAttribute;
      DisplayPanel(boolean withStar){
        // Combobox attribute
        cbDisplayAttribute = new JComboBox();
        if(withStar){
          cbDisplayAttribute.insertItemAt("*", 0);
        }
       if(fieldList == null){
          Debug.debug("fieldlist null", 2);
         return;
       }
        for(int i=0;i<fieldList.length; ++i){
          if(withStar){
            cbDisplayAttribute.insertItemAt(fieldList[i], i+1);
          }
          else{
            cbDisplayAttribute.insertItemAt(fieldList[i], i);
          }
        }
        cbDisplayAttribute.setSelectedIndex(0);
        this.add(cbDisplayAttribute);
      }
      public String getSelected() {
        return (String) cbDisplayAttribute.getSelectedItem();
      }
      
    }

     /**
      * Action Events
      */
     
    private void bAddConstraintRow_actionPerformed(){
      spConstraintList.add(new ConstraintPanel());
      bRemoveConstraintRow.setEnabled(true);
      spConstraintList.updateUI();
    }
    
    private void bRemoveConstraintRow_actionPerformed(){
      spConstraintList.remove(spConstraintList.getComponentCount()-1);
      if(spConstraintList.getComponentCount() == 1){
        bRemoveConstraintRow.setEnabled(false);
      }
      spConstraintList.updateUI();
    }
    
    private void bAddDisplayRow_actionPerformed(){
      spDisplayList.add(new DisplayPanel(false));
      bRemoveDisplayRow.setEnabled(true);
      spDisplayList.updateUI();
    }
    
    private void bRemoveDisplayRow_actionPerformed(){
      spDisplayList.remove(spDisplayList.getComponentCount()-1);
      if(spDisplayList.getComponentCount()==1){
        bRemoveDisplayRow.setEnabled(false);
      }
      spDisplayList.updateUI();
    }
    
  }
    
  /**
   * Resets the constraint box to have only one constraint
   * box and have the first value selected in the drop downs
   * of this box.
   */
  public void resetConstraintList(String tableName){
    int comps = sPanel.spConstraintList.getComponentCount();
    if(comps>1){
      for(int i=comps-1; i>0; --i){
        sPanel.spConstraintList.remove(i);
      }
    }
    spcp = ((SPanel.ConstraintPanel)sPanel.spConstraintList.getComponent(0));
    if(spcp.cbConstraintAttribute==null ||
        spcp.cbConstraintRelation==null){
      return;
    }
    Component[] parts = spcp.cbConstraintAttribute.getComponents();
    if((parts!=null) && (parts.length>0)){
      spcp.cbConstraintAttribute.setSelectedIndex(0);
    }
    parts = spcp.cbConstraintRelation.getComponents();
    if((parts!=null) && (parts.length>0)){
      spcp.cbConstraintRelation.setSelectedIndex(0);
    }
    spcp.tfConstraintValue.setText("");
  }

  /**
   * Sets the constraint box to key = value.
   */
  public void setConstraint(String key, String value,
      int constraintRelationIndex){
    int comps = sPanel.spConstraintList.getComponentCount();
    if(comps>1){
      for(int i=comps-1; i>0; --i){
        sPanel.spConstraintList.remove(i);
      }
    }
    spcp = ((SPanel.ConstraintPanel)sPanel.spConstraintList.getComponent(0));
    if(spcp.cbConstraintAttribute==null){
      return;
    }
    if(spcp.cbConstraintRelation==null){
      return;
    }
    Component[] parts = spcp.cbConstraintAttribute.getComponents();
    if((parts!=null) && (parts.length>0)){
      for(int i=0; i<fieldNames.length; ++i){
        if(fieldNames[i].equalsIgnoreCase(key)){
          spcp.cbConstraintAttribute.setSelectedIndex(i);
        }
      }
    }
    parts = spcp.cbConstraintRelation.getComponents();
    if((parts!=null) && (parts.length>0)){
      spcp.cbConstraintRelation.setSelectedIndex(constraintRelationIndex);
    }
    spcp.tfConstraintValue.setText(value);
  }

  public void setDisplayFieldValues(String [][] values){
    SPanel spanel;
    SPanel thisSPanel = null;
    boolean fieldOk = false;
    for(int h=0; h<values.length; ++h){
      spanel = sPanel;
      Debug.debug("Table: "+h+" "+values[h][0]+h+" "+values[h][1]+
          " "+spanel.name, 3);
      Debug.debug("Table: "+spanel.name, 3);
      if(spanel.name.equals(values[h][0])){
        thisSPanel = spanel;
        if((values[h].length>0) && (spanel.spDisplayList!=null)){
          Debug.debug("Setting value", 3);
          String val = values[h][1];
          // make sure we have enough display panels
          fieldOk = false;
          for(int k=0; k<spanel.fieldList.length; ++k){
            if(val.equalsIgnoreCase(spanel.fieldList[k])){
              fieldOk = true;
              break;
            }
          }
          if(fieldOk){
            if(spanel.spDisplayList.getComponentCount()</*nr*/h+2){
              spanel.spDisplayList.add(spanel.new DisplayPanel(false));
              spanel.bRemoveDisplayRow.setEnabled(true);
            }
            Component firstcomp = spanel.spDisplayList.getComponent(h);
            Component secondcomp = null;
            Component comps[] = null;
            if(firstcomp != null){
              comps = ((SelectPanel.SPanel.DisplayPanel) firstcomp).getComponents();
            }
            if(comps!=null && comps.length>-1){
              secondcomp = ((SelectPanel.SPanel.DisplayPanel) firstcomp).getComponent(0);
            }
            if((val!=null) && (secondcomp!=null)){
              Debug.debug("Setting selected "+val, 3);
              // the various databases like to upper-case the names,
              // so try all possibilities
              ((JComboBox) secondcomp).setSelectedItem(val);
              ((JComboBox) secondcomp).setSelectedItem(val.toUpperCase());
              ((JComboBox) secondcomp).setSelectedItem(val.toLowerCase());
            }
          }
        }
      }
    }
    if(thisSPanel != null){
      if(thisSPanel.spDisplayList != null){
        for(int j=thisSPanel.spDisplayList.getComponentCount()-1;
        j+1>values.length; --j){
          // remove excess display panels
          Debug.debug("removing panel "+j, 3);
          thisSPanel.spDisplayList.remove(j);
          if(j==1){
            thisSPanel.bRemoveDisplayRow.setEnabled(false);
          }
        }
      }
    }
  }
  
  public int getDisplayFieldsCount() {
    return sPanel.spDisplayList.getComponentCount();
  }

  public Component getDisplayPanel(int i) {
    return sPanel.spDisplayList.getComponent(i);
  }

  public void addListenerForEnter(KeyAdapter adapter) {
    enterKeyAdapter = adapter;
    spcp.tfConstraintValue.addKeyListener(enterKeyAdapter);
  }
}
