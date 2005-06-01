package gridpilot;

import gridpilot.Debug;

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

 public class SelectPanel extends JPanel {

   // lists of field names with table name as key
   private String [] fieldNames = null;
   private String tableName;
   private String [] relationNames = {"=", "CONTAINS", "<", ">", "!="};
   private GridBagConstraints gbcVC;
   public SPanel.ConstraintPanel spcp;
   private SPanel sPanel;

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
   * basic SQL
   */
  public String getRequest(){
    String ret = "SELECT ";
    for(int i = 0; i <
    sPanel.spDisplayList.getComponentCount();
    ++i){
      SPanel.DisplayPanel cb = ((SPanel.DisplayPanel) sPanel.spDisplayList.getComponent(i));
      if(i>0){
        ret += ", ";
      }
      ret += cb.cbDisplayAttribute.getSelectedItem();
    }
    ret += " FROM " + tableName;
    if(sPanel.spConstraintList.getComponentCount() > 0 &&
        !((SPanel.ConstraintPanel) sPanel.spConstraintList.getComponent(0)).tfConstraintValue.getText().equals("")){
      ret += " WHERE ";
    }
    for(int i = 0; i <
    sPanel.spConstraintList.getComponentCount();
    ++i){
      SPanel.ConstraintPanel cb = ((SPanel.ConstraintPanel) sPanel.spConstraintList.getComponent(i));
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
     * Initialises the panel # 'panel'.
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
          
      public SPanel(String _name, String [] _fieldList){
       name = _name;
       fieldList = _fieldList;
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
      spcp = new ConstraintPanel();
      spConstraintList.add(spcp);
      gbcVC.gridx = 1;
      gbcVC.gridy = 0;
      this.add(spConstraintList, gbcVC);
                   
//// Display attributes
    
      // Label
      spDisplays.add(new JLabel("Select : "));
  
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
      spDisplayList.add(new DisplayPanel(true));
      gbcVC.gridx = 1;
      gbcVC.gridy = 1;
      this.add(spDisplayList, gbcVC);
     }
     
     protected class ConstraintPanel extends JPanel{
       private JComboBox cbConstraintAttribute;
       private JComboBox cbConstraintRelation;
       public JTextField tfConstraintValue;
       ConstraintPanel(){
         // Combobox attribute
	     if (fieldList == null) {
         Debug.debug("fieldlist null", 2);
	       return;	   
	       }
	       if (relationNames == null) {
           Debug.debug("relationNames null", 2);
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
       DisplayPanel(boolean withStar){
         // Combobox attribute
         cbDisplayAttribute = new JComboBox();
         if(withStar){
           cbDisplayAttribute.insertItemAt("*", 0);
         }
	   if (fieldList == null) {
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
       if(spDisplayList.getComponentCount() == 1){
         bRemoveDisplayRow.setEnabled(false);
       }
       spDisplayList.updateUI();
     }

    }
    
  public void resetConstraintList(String tableName){
    int comps = sPanel.spConstraintList.getComponentCount();
    if(comps>1){
      for(int i=comps-1; i>0; --i){
        sPanel.spConstraintList.remove(i);
      }
    }
    spcp = ((SPanel.ConstraintPanel)sPanel.spConstraintList.getComponent(0));
    if (spcp.cbConstraintAttribute == null) return;
    if (spcp.cbConstraintRelation == null) return;
    Component[] parts = spcp.cbConstraintAttribute.getComponents();
    if ((parts != null) && (parts.length > 0)) spcp.cbConstraintAttribute.setSelectedIndex(0);
    parts = spcp.cbConstraintRelation.getComponents();
    if ((parts != null) && (parts.length > 0)) spcp.cbConstraintRelation.setSelectedIndex(0);
    spcp.tfConstraintValue.setText("");
  }

  /**
   * Sets the constraint box to key = value.
   */
  public void setConstraint(String tableName, String key, String value,
      int constraintRelationIndex){
    int comps = sPanel.spConstraintList.getComponentCount();
    if(comps>1){
      for(int i=comps-1; i>0; --i){
        sPanel.spConstraintList.remove(i);
      }
    }
    spcp = ((SPanel.ConstraintPanel)sPanel.spConstraintList.getComponent(0));
    if (spcp.cbConstraintAttribute == null) return;
    if (spcp.cbConstraintRelation == null) return;
    Component[] parts = spcp.cbConstraintAttribute.getComponents();
    if ((parts != null) && (parts.length > 0)){
      for(int i=0; i<fieldNames.length; ++i){
        if(fieldNames[i].equalsIgnoreCase(key)){
          spcp.cbConstraintAttribute.setSelectedIndex(i);
        }
      }
    }
    parts = spcp.cbConstraintRelation.getComponents();
    if ((parts != null) && (parts.length > 0)) spcp.cbConstraintRelation.setSelectedIndex(constraintRelationIndex);
    spcp.tfConstraintValue.setText(value);
  }

  public void setDisplayFieldValue(String [][] values){
    SPanel spanel;
    SPanel thisSPanel = null;
    for(int h=0; h < values.length; ++h){
      spanel = sPanel;
      Debug.debug("Table: "+h+" "+values[h][0], 3);
      Debug.debug("Table: "+spanel.name, 3);
      if(spanel.name.equals(values[h][0])){
        thisSPanel = spanel;
         // make sure we have enough display panels
        if(spanel.spDisplayList.getComponentCount() < /*nr*/h+1){
          spanel.spDisplayList.add(spanel.new DisplayPanel(false));
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
