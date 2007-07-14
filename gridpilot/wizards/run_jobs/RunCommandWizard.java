package gridpilot.wizards.run_jobs;

import gridpilot.BrowserPanel;
import gridpilot.ConfirmBox;
import gridpilot.DBPluginMgr;
import gridpilot.DBRecord;
import gridpilot.GPFrame;
import gridpilot.GridPilot;
import gridpilot.MyThread;
import gridpilot.Util;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.HashMap;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.border.LineBorder;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkListener;

public class RunCommandWizard extends GPFrame{
  
  private static final long serialVersionUID=-722418889258036597L;
  private JButton bSubmit = new JButton("Submit job(s)");
  private JPopupMenu pmSubmitMenu = new JPopupMenu();
  private RunCommandWizard thisFrame = this;
  private JTextArea jtf = null;
  private JScrollPane sp = new JScrollPane();

  private static int TEXTFIELDWIDTH = 40;
  private static String myDatasetName = "my_dataset";
  private static String myTransformationName = "my_transformation";
  private static String myTransformationVersion = "0.1";
  
  public RunCommandWizard() {
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String confirmString =
    "This wizard will modify the transformation "+myTransformationName+"-"+myTransformationVersion+ " and\n" +
    "the dataset "+myDatasetName+" as well as any associated jobDefinitions.\n\n" +
    "If you have made changes to any of these that you would like to keep,\n" +
    "you should click \"Cancel\". Otherwise, click \"OK\" to proceed.\n\n";
    try{
      if(confirmBox.getConfirm("Warning", confirmString, new Object[] {"OK", "Cancel"})!=0){
        return;
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
    jtf = Util.createTextArea(TEXTFIELDWIDTH);
    
    JPanel jp = mkPanel();
    sp.getViewport().add(jp);
    add(sp);
    
    int maxHeight = Toolkit.getDefaultToolkit().getScreenSize().height-10;
    int maxWidth = Toolkit.getDefaultToolkit().getScreenSize().width-10;
    this.setMaximumSize(new Dimension(maxWidth, maxHeight>400?400:maxHeight));
    this.setPreferredSize(new Dimension(660, 440));
    this.pack();
    setVisible(true);
  }

  /**
   * - query for a command and a CS
   * - modify my_transformation script
   * - delete jobDefinition of my_dataset
   * - create jobDefinition of my_dataset
   * - submit job
   */
  private JPanel mkPanel(){
        
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    setTitle("Run command");
    String msg = 
      "This wizard helps you run a UNIX/Linux commands on any of the supported computing systems.\n\n" +
      "Type in a command, choose any input file(s) the command may need, type the names of any\n" +
      "output files the command may produce, click \"Run\" and use right-click menu on the job\n" +
      "monitoring panel to follow the progress of your job.\n\n";
    JLabel jlDirInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");

    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.BOTH;
    ct.anchor = GridBagConstraints.WEST;
    ct.weightx = 0;
    ct.ipady = 0;
    ct.ipadx = 0;
    ct.insets = new Insets(7, 7, 7, 7);
    
    ct.gridx = 0;
    ct.gridy = 0;
    ct.gridwidth = 3;
    ct.gridheight = 1;
    panel.add(jlDirInstructions, ct);
    
    ct.gridy = ct.gridy+1;
    ct.gridwidth = 1;
    panel.add(new JLabel("Command(s) "), ct);
    ct.gridx = 1;
    ct.gridwidth = 2;
    jtf.setText("echo \"hello world\"");
    panel.add(jtf, ct);
        
    ct.gridwidth = 3;
    ct.gridx = 0;
    ct.gridy = ct.gridy+1;
    JPanel row = new JPanel(new BorderLayout());
    ct.gridy = ct.gridy+1;
    JTextField field = new JTextField(TEXTFIELDWIDTH);
    if(GridPilot.gridHomeURL!=null && !GridPilot.gridHomeURL.equals("")){
      field.setText(GridPilot.gridHomeURL);
    }
    row.add(Util.createCheckPanel1(thisFrame, "Output directory", field,
        true, true, true), BorderLayout.WEST);
    JPanel fieldPanel = new JPanel();
    fieldPanel.add(field);
    row.add(fieldPanel, BorderLayout.CENTER);
    panel.add(row, ct);
    
    ct.gridwidth = 3;
    ct.gridx = 0;
    ct.gridy = ct.gridy+1;
    //panel.add(new JLabel("Output file(s)"), ct);
    InOutPanel outputsPanel = new InOutPanel(false);
    panel.add(createHyperLinkPanel("Output file(s)", outputsPanel), ct);
    outputsPanel.setVisible(false);

    ct.gridy = ct.gridy+1;
    outputsPanel.addRow("file 1");
    panel.add(outputsPanel, ct);

    ct.gridwidth = 3;
    ct.gridx = 0;
    ct.gridy = ct.gridy+1;
    //panel.add(new JLabel("Input file(s)"), ct);
    InOutPanel inputsPanel = new InOutPanel(true);
    panel.add(createHyperLinkPanel("Input file(s)", inputsPanel), ct);
    inputsPanel.setVisible(false);

    ct.gridy = ct.gridy+1;
    inputsPanel.addRow("file 1");
    panel.add(inputsPanel, ct);

    ct.gridy = ct.gridy+1;
    ct.gridwidth = 3;
    ct.gridx = 0;
    JPanel buttonsPanel = new JPanel();
    String enabled = "no";
    for(int i=0; i<GridPilot.csNames.length; ++i){
      try{
        enabled = GridPilot.getClassMgr().getConfigFile().getValue(GridPilot.csNames[i], "Enabled");
      }
      catch(Exception e){
        continue;
      }
      if(enabled==null || !enabled.equalsIgnoreCase("yes") &&
          !enabled.equalsIgnoreCase("true")){
        continue;
      }
      JMenuItem mi = new JMenuItem(GridPilot.csNames[i]);
      mi.addActionListener(new ActionListener(){
        public void actionPerformed(final ActionEvent e){
          runJob(e);
        }});
      pmSubmitMenu.add(mi);
    }
    bSubmit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        bSubmit_mousePressed();
      }
    });
    JPanel pCont = new JPanel();
    pCont.add(bSubmit);
    buttonsPanel.add(pCont);
    
    JButton bCancel = new JButton("Cancel");
    bCancel.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        thisFrame.dispose();
      }
    }
    );
    JPanel pCancel = new JPanel();
    pCancel.add(bCancel);
    buttonsPanel.add(pCancel, ct);

    panel.add(buttonsPanel, ct);
        
    return panel;
  }
  
  /**
   * Create hyperlink that toggles the visibility of a JPanel.
   * @param text the text of the hyperlink
   * @param panel the JPanel
   * @return a JEditorPane with the hyperlink
   */
  public static JEditorPane createHyperLinkPanel(final String text, final JPanel panel){
    String markup = "<a href=\"http://check/\">"+text+"</a>";
    JEditorPane checkPanel = new JEditorPane("text/html", markup);
    checkPanel.setEditable(false);
    checkPanel.setOpaque(false);
    checkPanel.addHyperlinkListener(
      new HyperlinkListener(){
      public void hyperlinkUpdate(HyperlinkEvent e){
        if(e.getEventType()==HyperlinkEvent.EventType.ACTIVATED){
          panel.setVisible(!panel.isVisible());
        }
      }
    });
    return checkPanel;
  }

  /**
   * Class for the expanding input files sub-panel. 
   */
  private class InOutPanel extends JPanel{
    private static final long serialVersionUID=-1784804615025727509L;
    /**
     * HashMap of String label -> JTextField value
     */
    protected HashMap fieldMap;
    private GridBagConstraints ct = new GridBagConstraints();
    private JButton plusButton = new JButton("+");
    private JButton minusButton = new JButton("-");
    private JPanel pmPanel = new JPanel();
    private boolean withFileChooser = true;
    InOutPanel(boolean _withFileChooser){
      withFileChooser = _withFileChooser;
      pmPanel.add(plusButton);
      pmPanel.add(minusButton);
      plusButton.addActionListener(new ActionListener(){
        public void actionPerformed(final ActionEvent e){
          addRow("file "+(ct.gridy+2));
        }});
      minusButton.addActionListener(new ActionListener(){
        public void actionPerformed(final ActionEvent e){
          removeRow();
        }});
      plusButton.setLayout(new BorderLayout());
      minusButton.setLayout(new BorderLayout());
      plusButton.setBorder(new LineBorder(Color.gray , 1));
      minusButton.setBorder(new LineBorder(Color.gray , 1));
      plusButton.setPreferredSize(new Dimension(24, 24));
      minusButton.setPreferredSize(new Dimension(24, 24));
      minusButton.setEnabled(false);
      fieldMap = new HashMap();
      ct.fill = GridBagConstraints.BOTH;
      ct.anchor = GridBagConstraints.WEST;
      ct.weightx = 0;
      ct.ipady = 0;
      ct.ipadx = 0;
      ct.insets = new Insets(0, 0, 0, 0);
      ct.gridx = 0;
      ct.gridy = -1;
      this.setLayout(new GridBagLayout());
    }
    protected void addRow(String label){
      // remove the +- buttons on the current last row
      if(this.getComponentCount()>0){
        minusButton.setEnabled(true);
        ((JPanel) this.getComponent(this.getComponentCount()-1)).remove(pmPanel);
      }
      // create and add the new row
      JPanel row = new JPanel(new BorderLayout());
      ct.gridy = ct.gridy+1;
      JTextField field = new JTextField(TEXTFIELDWIDTH);
      if(withFileChooser){
        row.add(Util.createCheckPanel1(thisFrame, label, field,
            true, true, false), BorderLayout.WEST);
      }
      else{
        JPanel lPanel = new JPanel();
        lPanel.add(new JLabel(label));
        lPanel.add(new JLabel(" "));
        row.add(lPanel, BorderLayout.WEST);
      }
      JPanel fieldPanel = new JPanel();
      fieldPanel.add(field);
      row.add(fieldPanel, BorderLayout.CENTER);
      row.add(pmPanel, BorderLayout.EAST);
      fieldMap.put(label, field);
      this.add(row, ct);
      this.updateUI();
    }
    protected void removeRow(){
      if(this.getComponentCount()==1){
        return;
      }
      else if(this.getComponentCount()==2){
        minusButton.setEnabled(false);
      }
      ct.gridy = ct.gridy-1;
      this.remove(this.getComponentCount()-1);
      ((JPanel) this.getComponent(this.getComponentCount()-1)).add(pmPanel, BorderLayout.EAST);
      this.updateUI();
    }
  }
  
  /**
   * Create and submit job.
   */
  MyThread workThread = null;
  private void runJob(final ActionEvent e){
    final String cmd = jtf.getText().trim();
    // TODO: get the input file URLs
    //final String [] inputURLs = jtf.getText().trim();
    workThread = new MyThread(){
      public void run(){
        statusBar.setLabel("Preparing jobs, please wait...");
        setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        statusBar.animateProgressBar();
        statusBar.setIndeterminateProgressBarToolTip("click here to interrupt (not recommended)");
        statusBar.addIndeterminateProgressBarMouseListener(new MouseAdapter(){
          public void mouseClicked(MouseEvent me){
            setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
            statusBar.stopAnimation();
            if(workThread!=null){
              workThread.interrupt();
            }
          }
        });        
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr("My_DB_Local");
        // write cmd in the transformation script
        String transformationID = dbPluginMgr.getTransformationID(myTransformationName, myTransformationVersion);
        //dbPluginMgr.updateTransformation(transformationID);
        // Create the job definition
        String [] fields = new String [] {
            "datasetName",
            "name",
            "userInfo",
            "inputFileURLs",
            "outFileMapping",
            "transPars",
            "stdoutDest",
            "stderrDest"};
        Object [] values = new String [] {
            "datasetName",
            "name",
            "userInfo",
            "inputFileURLs",
            "outFileMapping",
            "transPars",
            "stdoutDest",
            "stderrDest"};
        DBRecord jobDefinition = null;
        try{
          jobDefinition = dbPluginMgr.createJobDef(fields, values);
        }
        catch(Exception e1){
          e1.printStackTrace();
        }
        Vector jobDefinitions = new Vector();
        jobDefinitions.add(jobDefinition);
        String csName = ((JMenuItem)e.getSource()).getText();
        // submit the jobs
        statusBar.setLabel("Submitting. Please wait...");
        GridPilot.getClassMgr().getSubmissionControl().submitJobDefinitions(jobDefinitions, csName, dbPluginMgr);
        statusBar.stopAnimation();
        statusBar.setLabel("");
        setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      }
    };

    workThread.start();
  }

  private void addHyperLinkListener(JEditorPane pane, final JPanel jPanel){
    pane.addHyperlinkListener(
        new HyperlinkListener(){
          public void hyperlinkUpdate(final HyperlinkEvent e){
            if(e.getEventType()==HyperlinkEvent.EventType.ACTIVATED){
              System.out.println("Launching browser...");
              final Window window = (Window) SwingUtilities.getWindowAncestor(jPanel.getRootPane());
              window.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
              MyThread t = new MyThread(){
                public void run(){
                  try{
                    new BrowserPanel(
                          window,
                          "Browser",
                          e.getURL().toString(),
                          null,
                          true,
                          /*filter*/false,
                          /*navigation*/true,
                          null,
                          null,
                          false,
                          false,
                          false);
                  }
                  catch(Exception e){
                    e.printStackTrace();
                    try{
                      window.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
                      Util.showError("WARNING: could not open URL. "+e.getMessage());
                    }
                    catch(Exception e2){
                      e2.printStackTrace();
                    }
                  }
                }
              };
              SwingUtilities.invokeLater(t);
            }
          }
        });
  }

  private void bSubmit_mousePressed(){
    if(jtf.getText()==null || jtf.getText().equals("")){
      Util.showError("You must give a command");
      return;
    }
    // if a jobDefinition is selected, show the menu with computing systems
    pmSubmitMenu.show(this, 0, 0); // without this, pmSubmitMenu.getWidth == 0
    pmSubmitMenu.show(bSubmit, -pmSubmitMenu.getWidth(),
                      -pmSubmitMenu.getHeight() + bSubmit.getHeight());
  }

}
