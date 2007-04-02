package gridpilot;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;

import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeSelectionModel;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;

import java.util.Iterator;
import java.awt.Dimension;
import java.awt.GridLayout;

public class PreferencesPanel extends JPanel implements TreeSelectionListener {

  private static final long serialVersionUID = 1L;
  private JTree tree;
  private JScrollPane configEditorPanel;

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

    // Create the HTML viewing pane.
    configEditorPanel = new JScrollPane();

    // Add the scroll panes to a split pane.
    JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
    splitPane.setTopComponent(treeView);
    splitPane.setBottomComponent(configEditorPanel);

    Dimension minimumSize = new Dimension(100, 50);
    configEditorPanel.setMinimumSize(minimumSize);
    treeView.setMinimumSize(minimumSize);
    splitPane.setDividerLocation(100); // XXX: ignored in some releases
    // of Swing. bug 4101306
    // workaround for bug 4101306:
    // treeView.setPreferredSize(new Dimension(100, 100));

    splitPane.setPreferredSize(new Dimension(500, 300));

    // Add the split pane to this panel.
    add(splitPane);
  }

  /**
   * Required by TreeSelectionListener interface.
   **/
  public void valueChanged(TreeSelectionEvent e) {
    DefaultMutableTreeNode node =
      (DefaultMutableTreeNode) tree.getLastSelectedPathComponent();

    if(node==null){
      return;
    }

    Object nodeInfo = node.getUserObject();
    if(node.isLeaf()){
      ConfigNode configNode = (ConfigNode) nodeInfo;
      configEditorPanel.remove(0);
      configEditorPanel.add(createConfigJPanel(configNode));
    }
    else{
    }
  }
  
  private JPanel createConfigJPanel(ConfigNode configNode){
    JPanel configPanel = new JPanel();
    return configPanel;
  }

  private void createNodes(DefaultMutableTreeNode top){

    ConfigNode topNode = (ConfigNode) top.getUserObject();
    ConfigNode node = null;
    for(Iterator it=topNode.getConfigNodes().iterator(); it.hasNext();){
      node = (ConfigNode) it.next();
      if(node.getConfigNodes()==null || node.getConfigNodes().size()==0){
        DefaultMutableTreeNode treeNode = new DefaultMutableTreeNode(node);
        top.add(treeNode);
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

}
