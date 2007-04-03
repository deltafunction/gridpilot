package gridpilot;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import javax.swing.JTree;
import javax.swing.text.JTextComponent;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeSelectionModel;
import javax.swing.border.EtchedBorder;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;

import java.util.Iterator;
import java.util.Vector;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class PreferencesPanel extends JPanel implements TreeSelectionListener, ActionListener {

  private static final long serialVersionUID = 1L;
  private JTree tree;
  private JPanel configEditorPanel;
  private JButton bOk = new JButton();
  private JButton bCancel = new JButton();
  private JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
  
  public PreferencesPanel(ConfigNode topNode) {
    super(new GridLayout(1, 0));

    DefaultMutableTreeNode top = new DefaultMutableTreeNode(topNode);
    createNodes(top);

    // Create a tree that allows one selection at a time.
    tree = new JTree(top);
    tree.getSelectionModel().setSelectionMode(
        TreeSelectionModel.SINGLE_TREE_SELECTION);

    // Listen for when the selection changes.
    tree.addTreeSelectionListener(this);

    // Create the scroll pane and add the tree to it.
    JScrollPane treeView = new JScrollPane(tree);
    
    JPanel buttonPanel = new JPanel();
    bOk.setText("OK");
    bOk.addActionListener(this);
    bCancel.setText("Cancel");
    bCancel.addActionListener(this);

    buttonPanel.add(bOk);
    buttonPanel.add(bCancel);
    
    treeView.add(buttonPanel);

    // Create the viewing pane.
    configEditorPanel = new JPanel();

    // Add the scroll panes to a split pane.
    splitPane.setLeftComponent(treeView);
    treeView.setMinimumSize(new Dimension(170, 500));
    splitPane.setRightComponent(configEditorPanel);
    setPreferredSize(new Dimension(600, 500));
    setSize(new Dimension(600, 500));
    splitPane.setDividerLocation(170);
        
    // Add the split pane to this panel.
    add(splitPane);
  }

  /**
   * Required by TreeSelectionListener interface.
   **/
  public void valueChanged(TreeSelectionEvent e){
    DefaultMutableTreeNode node =
      (DefaultMutableTreeNode) tree.getLastSelectedPathComponent();

    if(node==null){
      return;
    }

    Object nodeInfo = node.getUserObject();
    int dividerLocation = splitPane.getDividerLocation();
    if(node.isLeaf()){
      ConfigNode configNode = (ConfigNode) nodeInfo;
      splitPane.setRightComponent(createConfigPanel(configNode));
    }
    else{
      ConfigNode configNode = (ConfigNode) nodeInfo;
      splitPane.setRightComponent(createConfigDescriptionPanel(configNode));
    }
    splitPane.setDividerLocation(dividerLocation);
  }
  
  private JComponent createConfigDescriptionPanel(ConfigNode configNode){
    JPanel configPanel = new JPanel(new BorderLayout());
    JScrollPane ret = new JScrollPane();
    if(configNode.getDescription()!=null && configNode.getDescription().length()>0){
      JEditorPane ep = new JEditorPane();
      ep.setContentType("text/html");
      ep.setText("<font size=-1>"+configNode.getDescription()+"</font>");
      ep.setEditable(false);
      configPanel.add(ep, BorderLayout.NORTH);
      configPanel.add(createConfigPanel(configNode), BorderLayout.CENTER);
      ret.getViewport().add(configPanel);
    }
    else{
      ret.getViewport().add(createConfigPanel(configNode));
    }
    return ret;
  }

  private JComponent createConfigPanel(ConfigNode configNode){
    JPanel configPanel = new JPanel(new GridBagLayout());
    Vector nodes = configNode.getConfigNodes();
    ConfigNode node = null;
    JLabel jlAttribute = null;
    JTextComponent jtcValue = null;
    JPanel jpRow = null;
    int row = 0;
    for(Iterator it=nodes.iterator(); it.hasNext();){
      jpRow = new JPanel(/*new BorderLayout()*/
          new FlowLayout(FlowLayout.LEFT, 2, 0));
      node = (ConfigNode) it.next();
      if(node.getConfigNodes().size()>0){
        continue;
      }
      jlAttribute = new JLabel(node.getName()+":  "); 
      if(node.getValue()!=null && node.getValue().length()>40){
        jtcValue = new JTextArea(1, 24);
        ((JTextArea) jtcValue).setMargin(new Insets(0, 0, 0, 0));
        ((JTextArea) jtcValue).setBorder(new EtchedBorder(EtchedBorder.RAISED,
            Color.white, new Color(165, 163, 151)));
        ((JTextArea) jtcValue).setWrapStyleWord(true);
        ((JTextArea) jtcValue).setLineWrap(true);
        jtcValue.setText(node.getValue());
      }
      else{
        jtcValue = new JTextField(16);
        jtcValue.setText(node.getValue());
      }
      jtcValue.setMaximumSize(jtcValue.getPreferredSize());
      jpRow.add(jlAttribute, BorderLayout.WEST);
      jpRow.add(jtcValue, BorderLayout.CENTER);
      if(node.getDescription()!=null){
        jpRow.setToolTipText(node.getDescription().replaceAll("<br>", ""));
      }
      configPanel.add(jpRow, new GridBagConstraints(0, row, 1, 1, 1.0, 0.0,
          GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
          new Insets(5, 5, 5, 5), 0, 0));
      ++row;
    }
    configPanel.validate();
    JScrollPane jsp = new JScrollPane();
    jsp.getViewport().add(configPanel);
    return jsp;
  }

  private void createNodes(DefaultMutableTreeNode top){

    ConfigNode topNode = (ConfigNode) top.getUserObject();
    ConfigNode node = null;
    for(Iterator it=topNode.getConfigNodes().iterator(); it.hasNext();){
      node = (ConfigNode) it.next();
      if(node.getConfigNodes()==null){
      }
      else if(node.getConfigNodes().size()==0){
        //DefaultMutableTreeNode treeNode = new DefaultMutableTreeNode(node);
        //top.add(treeNode);
      }
      else{
        DefaultMutableTreeNode treeNode = new DefaultMutableTreeNode(node);
        top.add(treeNode);
        createNodes(treeNode);
      }
    }
  }

  public void windowClosing(){
    savePrefs();
  }
  
  public void savePrefs(){
    // TODO
  }

  public void actionPerformed(ActionEvent e){
    try{
      if(e.getSource()==bOk){
      }
      else if(e.getSource()==bCancel){
      }
    }
    catch(Exception ex){
      GridPilot.getClassMgr().getLogFile().addMessage("ERROR: ", ex);
      Debug.debug("ERROR: "+ex.getMessage(), 3);
      ex.printStackTrace();
    }
  }

}
