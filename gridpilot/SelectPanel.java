package gridpilot;

import gridpilot.Debug;

import javax.swing.*;

import java.awt.*;
import javax.swing.border.*;
import java.awt.event.*;
import java.util.HashMap;



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

 public class SelectPanel extends JPanel {

   private HashMap fieldNames = new HashMap();
   private int numberOfTables;
   private String[] tableNames;
   private HashMap pTable = new HashMap();
   private String [] relationNames = {"=", "CONTAINS", "<", ">", "!="};
   private GridBagConstraints gbcVC;

   /**
   * Constructors
   */

   /**
    * Creates a SelectPanel, with n tables.
    * Called by : DBPanel.DBPanel()
    */

   public SelectPanel(String dbName, String stepName, String [] _tables)
       throws Exception{
     tableNames = _tables;
     numberOfTables = tableNames.length;
     Debug.debug("Starting SelectPanel for " + dbName + " " + stepName, 3);
     for(int i = 0; i < numberOfTables; ++i){
       fieldNames.put(tableNames[i], GridPilot.getClassMgr().getDBPluginMgr(dbName, stepName).getFieldNames(tableNames[i]));
     }
   }
   
  /**
   * GUI Initialisation
   */
  
  public void initGUI() throws Exception {
    
    gbcVC = new GridBagConstraints();
    gbcVC.fill = GridBagConstraints.VERTICAL;
    
    this.setLayout(new GridBagLayout());
    
    SPanel sPanel;
  
    for(int i=0; i<numberOfTables; ++i){
      Debug.debug2("creating sPanel");
     sPanel = new SPanel(tableNames[i],
          (String []) fieldNames.get(tableNames[i]));
     
     sPanel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
         Color.white,new Color(165, 163, 151)),this.getName()));
       
     
     Debug.debug("Created SPanel with " + ((JPanel) sPanel.getComponent(1)).getComponentCount() + " components", 3);
               
     gbcVC.gridx = 0;
     gbcVC.gridy = 0;
     gbcVC.anchor = GridBagConstraints.NORTHWEST;
     this.add(sPanel, gbcVC);
      
     pTable.put(tableNames[i], sPanel);
      ((JPanel) pTable.get(tableNames[i])).setName(tableNames[i]);
      
    }  
  }
  //TODO: implement
  public String getRequest(String tableName){
    String ret = "SELECT ";
    for(int i = 0; i <
    ((SPanel) pTable.get(tableName)).spDisplayList.getComponentCount();
    ++i){
      SPanel.DisplayPanel cb = ((SPanel.DisplayPanel) ((SPanel) pTable.get(tableName)).spDisplayList.getComponent(i));
      if(i>0){
        ret += ", ";
      }
      ret += cb.cbDisplayAttribute.getSelectedItem();
    }
    ret += " FROM " + tableName;
    if(((SPanel) pTable.get(tableName)).spConstraintList.getComponentCount() > 0 &&
        !((SPanel.ConstraintPanel) ((SPanel) pTable.get(tableName)).spConstraintList.getComponent(0)).tfConstraintValue.getText().equals("")){
      ret += " WHERE ";
    }
    for(int i = 0; i <
    ((SPanel) pTable.get(tableName)).spConstraintList.getComponentCount();
    ++i){
      SPanel.ConstraintPanel cb = ((SPanel.ConstraintPanel) ((SPanel) pTable.get(tableName)).spConstraintList.getComponent(i));
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
  //TODO: implement !
  public void clear(){
  }

  /**
   * Returns the index of the string s in the array array, or -1
   */
  private int getIndexOf(String [] array, String s){
    if(array == null || s == null)
      return -1;
    for(int i=0; i<array.length; ++i)
      if(array[i].equals(s))
        return i;
  
    return -1;
  }
  
  /**
   * Gets panel number of the panel which contains the source of the current event.
   * @return index in sources of e.getSource
   */
  private int getPanel(ActionEvent e, Object [] sources){
    if(e.getSource().getClass() == JButton.class)
      return ((JButton)(e.getSource())).getMnemonic();

    for(int i=0; i<sources.length; ++i){
      if(e.getSource() == sources[i])
        return i;
    }
    System.err.println("SelectPanel.getPanel : source not found");
    return -1;
  }

  /**
     * Initialises the panel n0 'panel'.
     * Called by : this.initGUI()
     */
    private class SPanel extends JPanel{
      public String name = "";
      private JButton bAddConstraintRow;
      private JButton bRemoveConstraintRow ;
      private JPanel spConstraintList;
      private JPanel spConstraints;
      private JButton bAddDisplayRow;
      private JButton bRemoveDisplayRow;
      private JPanel spDisplayList;
      private JPanel spDisplays;
      protected String [] fieldList;
          
      public SPanel (String _name, String [] _fieldList){
       name = _name;
       fieldList = _fieldList;
       //Debug.debug("Initializing SPanel for " + name + " with " + fieldList.length + " fields",3);
       bAddConstraintRow = new JButton();
       bRemoveConstraintRow = new JButton();
       spConstraints = new JPanel();
       spConstraintList = new JPanel();
       bAddDisplayRow = new JButton();
       bRemoveDisplayRow = new JButton();
       spDisplayList = new JPanel();
       spDisplays = new JPanel();
       
       this.setLayout(new GridBagLayout());
         
  //// Add constraints
      //Label
      spConstraints.add(new JLabel("Constraints :"));

      // Button More
      bAddConstraintRow.setText("More");
      bAddConstraintRow.addActionListener(new java.awt.event.ActionListener() {
        public void actionPerformed(ActionEvent e) { bAddConstraintRow_actionPerformed();}});
  
      spConstraints.add(bAddConstraintRow);
  
      // Button Less
      bRemoveConstraintRow.setText("Less");
      bRemoveConstraintRow.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){bRemoveConstraintRow_actionPerformed(); }});
      bRemoveConstraintRow.setEnabled(false);
  
      spConstraints.add(bRemoveConstraintRow);
      
      gbcVC.gridx = 0;
      gbcVC.gridy = 0;
      gbcVC.anchor = GridBagConstraints.WEST;
      this.add(spConstraints, gbcVC);
      spConstraintList.add(new ConstraintPanel());
      gbcVC.gridx = 1;
      gbcVC.gridy = 0;
      this.add(spConstraintList, gbcVC);
                   
      //// Display attributes
    
      // Label
      spDisplays.add(new JLabel("Display : "));
  
      // Button More
      bAddDisplayRow.setText("More");
      bAddDisplayRow.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){bAddDisplayRow_actionPerformed();
      }});
  
      spDisplays.add(bAddDisplayRow);
 
      // Button Less
      bRemoveDisplayRow.setText("Less");
      bRemoveDisplayRow.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){bRemoveDisplayRow_actionPerformed();
      }});
  
      spDisplays.add(bRemoveDisplayRow);
      bRemoveDisplayRow.setEnabled(false);
      
      gbcVC.gridx = 0;
      gbcVC.gridy = 1;
      this.add(spDisplays, gbcVC);
      spDisplayList.add(new DisplayPanel());
      gbcVC.gridx = 1;
      gbcVC.gridy = 1;
      this.add(spDisplayList, gbcVC);
     }
     
     protected class ConstraintPanel extends JPanel{
       private JComboBox cbConstraintAttribute;
       private JComboBox cbConstraintRelation;
       private JTextField tfConstraintValue;
       ConstraintPanel(){
         // Combobox attribute
	   if (fieldList == null) {
       Debug.debug2("fieldlist null");
	     return;
	   
	   }
	   if (relationNames == null) {
       Debug.debug2("relationNames null");
	     return;
	   }
         cbConstraintAttribute = new JComboBox();
         for(int i=0;i<fieldList.length; ++i)
           cbConstraintAttribute.insertItemAt(fieldList[i], i);
           
         cbConstraintAttribute.setSelectedIndex(0);
     
         this.add(cbConstraintAttribute);
     
         // Combobox relation
         cbConstraintRelation = new JComboBox();
         for(int i=0; i<relationNames.length; ++i)
             cbConstraintRelation.insertItemAt(relationNames[i], i);
         cbConstraintRelation.setSelectedIndex(0);
     
         this.add(cbConstraintRelation);
     
         // Textfield value
         tfConstraintValue = new JTextField(12);
         this.add(tfConstraintValue);
       }
     }        
     
     protected class DisplayPanel extends JPanel{
       private JComboBox cbDisplayAttribute;
       DisplayPanel(){
         // Combobox attribute
         cbDisplayAttribute = new JComboBox();
         //cbDisplayAttribute.insertItemAt("*", 0);
	   if (fieldList == null) {
	       System.out.println("fieldlist null");
	       return;
	   
	   }
         for(int i=0;i<fieldList.length; ++i)
           cbDisplayAttribute.insertItemAt(fieldList[i], i/*+1*/) ;
         
         cbDisplayAttribute.setSelectedIndex(0);
     
         this.add(cbDisplayAttribute);
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
       spDisplayList.add(new DisplayPanel());
       bRemoveDisplayRow.setEnabled(true);
       spDisplayList.updateUI();
     }
     private void bRemoveDisplayRow_actionPerformed(){
       spDisplayList.remove(spDisplayList.getComponentCount()-1);
       if(spDisplayList.getComponentCount() == 1){
         bRemoveDisplayRow.setEnabled(false);
       }
       spDisplayList.updateUI();
     }

    }
    
  public void resetConstraintList(String tableName){
    int comps =
      ((SPanel) pTable.get(tableName)).spConstraintList.getComponentCount();
    if(comps>1){
      for(int i=comps-1; i>0; --i){
        ((SPanel) pTable.get(tableName)).spConstraintList.remove(i);
      }
    }
    SPanel.ConstraintPanel spcp =
      ((SPanel.ConstraintPanel)((SPanel) pTable.get(tableName)).spConstraintList.getComponent(0));
    if (spcp.cbConstraintAttribute == null) return;
    if (spcp.cbConstraintRelation == null) return;
    Component[] parts = spcp.cbConstraintAttribute.getComponents();
    if ((parts != null) && (parts.length > 0)) spcp.cbConstraintAttribute.setSelectedIndex(0);
    parts = spcp.cbConstraintRelation.getComponents();
    if ((parts != null) && (parts.length > 0)) spcp.cbConstraintRelation.setSelectedIndex(0);
    spcp.tfConstraintValue.setText("");
  }
 
  public void setDisplayFieldValue(String [][] values){
    SPanel spanel;
    SPanel thisSPanel = null;
    //int nr = 0;
    for(int h=0; h < values.length; ++h){
      spanel = (SPanel) pTable.get(values[h][0]);
      Debug.debug("Table: "+h+" "+values[h][0], 3);
      Debug.debug("Table: "+spanel.name, 3);
      if(spanel.name.equals(values[h][0])){
        thisSPanel = spanel;
         // make sure we have enough display panels
        if(spanel.spDisplayList.getComponentCount() < /*nr*/h+1){
          spanel.spDisplayList.add(spanel.new DisplayPanel());
          spanel.bRemoveDisplayRow.setEnabled(true);
        }
      	if ((values[h].length > 0) && (spanel.spDisplayList != null)) {
      		String val = values[h][1];
      		Component firstcomp = spanel.spDisplayList.getComponent(h);
      		Component secondcomp = null;
      		Component comps[] = null;
      		if (firstcomp != null){
            comps = ((SelectPanel.SPanel.DisplayPanel) firstcomp).getComponents();
          }
      		if (comps != null && comps.length > -1){
            secondcomp = ((SelectPanel.SPanel.DisplayPanel) firstcomp).getComponent(0);
          }
      		if ((val != null) && (secondcomp != null)){
            Debug.debug("Setting selected "+val, 3);
            ((JComboBox) ((SelectPanel.SPanel.DisplayPanel) spanel.spDisplayList.getComponent(/*nr*/h)).getComponent(0)).setSelectedItem(val.toUpperCase());
          }
        }
      }
      //++nr;
    }
    if (thisSPanel != null)
    	if (thisSPanel.spDisplayList != null)
    for(int j=thisSPanel.spDisplayList.getComponentCount()-1; j+1>values.length; --j){
      // remove excess display panels
      Debug.debug("removing panel "+j, 3);
      thisSPanel.spDisplayList.remove(j);
      if(j == 1){
        thisSPanel.bRemoveDisplayRow.setEnabled(false);
      }
    }
  }
}
