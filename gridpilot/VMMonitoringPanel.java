package gridpilot;

import gridfactory.common.StatusBar;
import gridpilot.GridPilot;
import gridpilot.MyJTable;

import java.awt.*;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.ClipboardOwner;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.ListSelectionModel;
import javax.swing.TransferHandler;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

/**
 * Panel showing the status of virtual machines and containing buttons for
 * starting and stopping virtual machines.
 * 
 * Available images: <selectable list>  [launch]
 * Running images: <selectable list> [configure access] [stop]
 * 
 * This class should be extended with actual implementations of
 * the non-private methods and fields.
 * 
 */
public class VMMonitoringPanel extends JPanel implements ClipboardOwner{

  private static final long serialVersionUID = 1L;
  
  private JMenuItem miCopyDNS = new JMenuItem("Copy DNS to clipboard");
  private JMenuItem miCopyCredentials = new JMenuItem("Copy login information to clipboard");
  private JMenuItem miRunShell = new JMenuItem("Run shell on instance");
  private StatusBar statusBar = null;
  private JButton bTerminate = new JButton("Terminate");
  private JButton bLaunch = new JButton("Launch");

  protected MyJTable imageTable = null;
  protected MyJTable instanceTable = null;
  
  protected JPanel pImagesButtons = new JPanel();
  protected String [] imageColorMapping = null;  
  protected String [] instanceColorMapping = null;  
  protected String [] sshCommand = null;
  protected String [] IMAGE_FIELDS = new String [] {"Image ID", "Manifest", "State", "Owner"};
  protected String [] INSTANCE_FIELDS = new String [] {"Reservation ID", "Owner", "Instance ID", "Image", "State",
      "Public DNS", "SSH key"};
  protected int dnsField = 5;
  protected int idField = 2;
  protected int imIdField = 0;
  protected int stateField = 4;
  protected String runningString = "running";
 
  public VMMonitoringPanel() throws Exception{
    // use status bar on main window until a monitoring panel is actually created
    statusBar = GridPilot.getClassMgr().getStatusBar();
    initGUI();
    bLaunch.setEnabled(false);
    bTerminate.setEnabled(false);
  }
  
  private void initGUI() throws Exception{
    
    this.setLayout(new BorderLayout());
    
    statusBar = new StatusBar();
    
    JPanel jp = new JPanel(new BorderLayout());
    JPanel availableImagesPanel = createImagesPanel();
    JPanel runningInstancesPanel = createInstancesPanel();
    availableImagesPanel.setPreferredSize(new Dimension(600, 150));
    runningInstancesPanel.setPreferredSize(new Dimension(600, 150));
    availableImagesPanel.setMinimumSize(new Dimension(600, 100));
    runningInstancesPanel.setMinimumSize(new Dimension(600, 100));

    JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT,
        availableImagesPanel, runningInstancesPanel);
    splitPane.setOneTouchExpandable(true);
    splitPane.setContinuousLayout(true);
    jp.add(splitPane);
    jp.add(statusBar, BorderLayout.SOUTH);
    
    JScrollPane sp = new JScrollPane();
    sp.getViewport().add(jp);
    this.add(sp);

    /*this.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
    this.addWindowListener(new WindowAdapter(){
      public void windowClosing(WindowEvent we){
        Debug.debug("Thwarted user attempt to close window.", 3);
      }
    });

    jp.setPreferredSize(new Dimension(600, 300));
    this.pack();*/
    splitPane.setDividerLocation(0.5);
    splitPane.setResizeWeight(0.5);
    
    // Disable clipboard handling inherited from JPanel
    TransferHandler th = new TransferHandler(null);
    instanceTable.setTransferHandler(th);
    
    makeMenu();
  }
  
  private JPanel createImagesPanel() throws Exception{
    JPanel panel = new JPanel(new BorderLayout()); 
    imageTable = new MyJTable(new String [] {}, IMAGE_FIELDS, imageColorMapping);
    imageTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    imageTable.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        imageSelectionEvent(e);
      }
    });
    imageTable.setTable(IMAGE_FIELDS);
    imageTable.updateUI();
    JScrollPane sp = new JScrollPane();
    sp.getViewport().add(imageTable);
    sp.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Available Images"));
    panel.add(sp);
    // buttons
    JButton bRefresh = new JButton("Refresh");
    bRefresh.setToolTipText("Refresh the list of Images");
    bRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          imageTable.setTable(getAvailableImages(), IMAGE_FIELDS);
          bLaunch.setEnabled(false);
          bTerminate.setEnabled(false);
          makeMenu();
        }
        catch(Exception e1){
           e1.printStackTrace();
        }
      }
    });
    bLaunch.setToolTipText("Launch an instance of the selected image");
    bLaunch.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          launchImages();
        }
        catch(Exception e1){
           e1.printStackTrace();
           GridPilot.getClassMgr().getLogFile().addMessage("Could not lauch VM.", e1);
        }
      }
    });
    pImagesButtons.add(bRefresh);
    pImagesButtons.add(new JLabel("|"));
    pImagesButtons.add(bLaunch);
    panel.add(pImagesButtons, BorderLayout.SOUTH);
    return panel;
  }

  private JPanel createInstancesPanel() throws Exception{
    JPanel panel = new JPanel(new BorderLayout()); 
    instanceTable = new MyJTable(new String [] {}, INSTANCE_FIELDS, instanceColorMapping);
    imageTable.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
    instanceTable.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        instanceSelectionEvent(e);
      }
    });
    instanceTable.setTable(INSTANCE_FIELDS);
    instanceTable.updateUI();
    JScrollPane sp = new JScrollPane();
    sp.getViewport().add(instanceTable);
    sp.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Your instances"));
    panel.add(sp);
    JPanel pButtons = new JPanel();
    JButton bRefresh = new JButton("Refresh");
    bRefresh.setToolTipText("Refresh the list of instances");
    bRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          instanceTable.setTable(getRunningInstances(), INSTANCE_FIELDS);
          bLaunch.setEnabled(false);
          bTerminate.setEnabled(false);
          makeMenu();
        }
        catch(Exception e1){
           e1.printStackTrace();
        }
      }
    });
    bTerminate.setToolTipText("Terminate the selected instance(s)");
    bTerminate.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          terminateInstances();
        }
        catch(Exception e1){
           e1.printStackTrace();
        }
      }
    });
    pButtons.add(bRefresh);
    pButtons.add(new JLabel("|"));
    pButtons.add(bTerminate);
    panel.add(pButtons, BorderLayout.SOUTH);
    return panel;
  }

  /**
   * Updates selected and unselected menu items or button regarding to the selection.
   */
  private void imageSelectionEvent(ListSelectionEvent e){
    // ignore extra messages.
    if(e.getValueIsAdjusting()){
      return;
    }
    ListSelectionModel lsm = (ListSelectionModel)e.getSource();
    if(lsm.isSelectionEmpty()){
      bLaunch.setEnabled(false);
    }
    else{
      //int [] rows = imageTable.getSelectedRows();
      bLaunch.setEnabled(!lsm.isSelectionEmpty() && lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
    }
  }
  
  /**
   * Updates selected and unselected menu items or button according to the selection.
   */
  private void instanceSelectionEvent(ListSelectionEvent e){
    // ignore extra messages.
    if(e.getValueIsAdjusting()){
      return;
    }
    ListSelectionModel lsm = (ListSelectionModel)e.getSource();
    if(lsm.isSelectionEmpty()){
      bTerminate.setEnabled(false);
      miCopyDNS.setEnabled(false);
      miCopyCredentials.setEnabled(false);
      miRunShell.setEnabled(false);
    }
    else{
      int [] rows = instanceTable.getSelectedRows();
      bTerminate.setEnabled(true);
      for(int i=0; i<rows.length; ++i){
        boolean ok = !lsm.isSelectionEmpty() && lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex() &&
        ((String) instanceTable.getUnsortedValueAt(rows[i], stateField)).equalsIgnoreCase(runningString);
        bTerminate.setEnabled(ok);
        miCopyDNS.setEnabled(ok);
        miCopyCredentials.setEnabled(ok);
        miRunShell.setEnabled(ok);
      }
      //bTerminate.setEnabled(!lsm.isSelectionEmpty());
    }
  }

  /**
   * Makes the menu shown when the user right-clicks on the status table
   */
  private void makeMenu(){
    miCopyDNS.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        (new Thread(){
          public void run(){
            copyDNSToClipBoard();
          }
        }).start();
      }
    });
    miCopyCredentials.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        (new Thread(){
          public void run(){
            copyCredentialsToClipBoard();
          }
        }).start();
      }
    });
    miRunShell.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        (new Thread(){
          public void run(){
            runShell();
          }
        }).start();
      }
    });
    //instanceTable.addMenuSeparator();
    instanceTable.addMenuItem(miCopyDNS);
    instanceTable.addMenuItem(miCopyCredentials);
    instanceTable.addMenuItem(miRunShell);
  }
  
  private void copyDNSToClipBoard(){
    int row = instanceTable.getSelectedRow();
    String dns = (String) instanceTable.getUnsortedValueAt(row, dnsField);
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    StringSelection stringSelection = new StringSelection(dns);
    clipboard.setContents(stringSelection, this);
  }

  private void copyCredentialsToClipBoard(){
    // Set e.g. the key file location
    String creds = getCredentials();
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    StringSelection stringSelection = new StringSelection(creds);
    clipboard.setContents(stringSelection, this);
  }
  
  public void lostOwnership(Clipboard clipboard, Transferable contents) {
  }
  
///////////////////////// Implement methods below ///////////////////////////
  
  public String getName(){
    return "Virtual machines";
  }

  protected String [][] getAvailableImages() throws Exception{
    // Create table of image descriptions
    String [][] imageArray = new String [0][IMAGE_FIELDS.length];
    // Fill the table
    return imageArray;
  }
  
  protected String [][] getRunningInstances() throws Exception{
    // Create table of image descriptions
    String [][] instanceArray = new String[0][INSTANCE_FIELDS.length];
    // Fill the table
    return instanceArray;
  }
  
  protected void launchImages() throws Exception {
  }

  protected void terminateInstances() throws Exception {
  }

  
  /**
   * Get a string representing some credentials to login to an instance.
   * @return a string representing some credentials to login to an instance
   */
  protected String getCredentials(){
    return null;
  }

  protected void runShell(){
  }

}

