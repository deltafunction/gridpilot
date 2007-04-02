package gridpilot;

import javax.swing.JButton;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextField;

import javax.swing.JTree;
import javax.swing.text.JTextComponent;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeSelectionModel;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;

import java.util.Iterator;
import java.util.Vector;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class PreferencesPanel extends JPanel implements TreeSelectionListener, ActionListener {

  private static final long serialVersionUID = 1L;
  private JTree tree;
  private JPanel configEditorPanel;
  private JButton bOk = new JButton();
  private JButton bCancel = new JButton();
  private JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
  
  private static int TEXTFIELDWIDTH = 32;

  public PreferencesPanel() {
    super(new GridLayout(1, 0));

    // Create the nodes.
    GridPilot.getClassMgr().getConfigFile().getSections();
    DefaultMutableTreeNode top = new DefaultMutableTreeNode(
        GridPilot.getClassMgr().getConfigFile().getHeadNode());
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
    treeView.setMinimumSize(new Dimension(200, 300));
    splitPane.setRightComponent(configEditorPanel);
    setPreferredSize(new Dimension(500, 300));
    setSize(new Dimension(500, 300));
    splitPane.setDividerLocation(200);
    
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
    if(node.isLeaf()){
      ConfigNode configNode = (ConfigNode) nodeInfo;
      splitPane.setRightComponent(createConfigPanel(configNode));
    }
    else{
      ConfigNode configNode = (ConfigNode) nodeInfo;
      splitPane.setRightComponent(createConfigDescriptionPanel(configNode));
    }
  }
  
  private JEditorPane createConfigDescriptionPanel(ConfigNode configNode){
    JEditorPane ep = new JEditorPane();
    ep.setContentType("text/html");
    ep.setText(configNode.getDescription());
    ep.setEditable(false);
    return ep;
  }

  private JPanel createConfigPanel(ConfigNode configNode){
    JPanel configPanel = new JPanel();
    configPanel.add(new JLabel(configNode.getName()));
    Vector nodes = configNode.getConfigNodes();
    ConfigNode node = null;
    JLabel jlAttribute = null;
    JTextComponent jtcValue = null;
    JPanel jpRow = null;
    for(Iterator it=nodes.iterator(); it.hasNext();){
      jpRow = new JPanel(new FlowLayout(FlowLayout.RIGHT, 0, 0));
      node = (ConfigNode) it.next();
      jlAttribute = new JLabel(node.getName()); 
      if(node.getValue()!=null && node.getValue().length()>TEXTFIELDWIDTH){
        jtcValue = Util.createTextArea();
      }
      else{
        jtcValue = new JTextField("", TEXTFIELDWIDTH);
      }      
      jpRow.add(jlAttribute);
      jpRow.add(jtcValue);
      configPanel.add(jpRow);
    }
    return configPanel;
  }

  private void createNodes(DefaultMutableTreeNode top){

    ConfigNode topNode = (ConfigNode) top.getUserObject();
    ConfigNode node = null;
    for(Iterator it=topNode.getConfigNodes().iterator(); it.hasNext();){
      node = (ConfigNode) it.next();
      if(node.getConfigNodes()==null || node.getConfigNodes().size()==0){
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
