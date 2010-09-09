package gridpilot.wizards.run_jobs;

import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.GFrame;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.ResThread;
import gridpilot.DBPluginMgr;

import gridpilot.GridPilot;

import gridpilot.MyUtil;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.LineBorder;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkListener;

import org.safehaus.uuid.UUIDGenerator;

public class RunCommandWizard extends GFrame{
  
  private static final long serialVersionUID=-722418889258036597L;
  private JButton bSubmit = null;
  private JPopupMenu pmSubmitMenu = new JPopupMenu();
  private RunCommandWizard thisFrame = this;
  private JTextArea tfCommand = null;
  private JTextField tfOutputDir = null;
  private JScrollPane sp = new JScrollPane();
  private InOutPanel inputsPanel = null;
  private InOutPanel outputsPanel = null;

  private static String DB_NAME = "My_DB_Local";
  private static int TEXTFIELDWIDTH = 40;
  private static String myDatasetName = "my_dataset";
  private static String myExecutableName = "my_executable";
  private static String myExecutableVersion = "0.1";
  
  public RunCommandWizard() {
    
    bSubmit = MyUtil.mkButton("run.png", "Submit", "Submit job");
    
    tfCommand = MyUtil.createTextArea(TEXTFIELDWIDTH);
    tfOutputDir = new JTextField(TEXTFIELDWIDTH);
    
    JPanel jp = mkPanel();
    sp.getViewport().add(jp);
    add(sp);
    
    int maxHeight = Toolkit.getDefaultToolkit().getScreenSize().height-10;
    int maxWidth = Toolkit.getDefaultToolkit().getScreenSize().width-10;
    this.setMaximumSize(new Dimension(maxWidth, maxHeight>400?400:maxHeight));
    this.setPreferredSize(new Dimension(660, 700));
    this.pack();
    setVisible(true);
  }
  
  private String getInitialCmdTxt(){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(DB_NAME);
    String executableID = dbPluginMgr.getExecutableID(myExecutableName, myExecutableVersion);
    String scriptFile = (String) dbPluginMgr.getExecutable(executableID).getValue("executableFile");
    String txt = null;
    try{
      txt = LocalStaticShell.readFile(MyUtil.clearTildeLocally(MyUtil.clearFile(scriptFile)));
    }
    catch(Exception e){
      e.printStackTrace();
    }
    if(txt==null || txt.equals("")){
      txt = "echo \"hello world\"";
    }
    return txt;
  }

  private String getInitialOutputDir(){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(DB_NAME);
    String datasetID = dbPluginMgr.getDatasetID(myDatasetName);
    String txt = "";
    try{
      txt = (String) dbPluginMgr.getDataset(datasetID).getValue("outputLocation");
    }
    catch(Exception e){
      e.printStackTrace();
    }
    if(txt==null || txt.equals("")){
      if(GridPilot.GRID_HOME_URL!=null && !GridPilot.GRID_HOME_URL.equals("")){
        txt = GridPilot.GRID_HOME_URL;
      }
    }
    return txt;
  }
  
  /**
   * - query for a command and a CS
   * - modify my_executable script
   * - delete jobDefinition of my_dataset
   * - create jobDefinition of my_dataset
   * - submit job
   */
  private JPanel mkPanel(){
        
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    setTitle("Run command");
    String msg = 
      "This wizard helps you run a UNIX/Linux script on any of the supported computing systems.\n\n" +
      "Type in some commands, choose an output directory for the stdout/stderr and any output files\n" +
      "the commands may produce.\n\n" +
      "If the commands produce any output files (apart from stdout/stderr), click on \"Output files\"\n" +
      "and fill in the names of these.\n\n" +
      "If the job needs any input files, click on \"Input files\" and fill in the URLs.\n\n" +
      "When you're done, click \"Submit job\" and use right-click menu on the job monitoring panel\n" +
      "to follow the progress of your job.\n\n" +
      "Notice that the job you create belongs to the dataset "+myDatasetName+" which in turn uses the \n" +
      "executable "+myExecutableName+". You can inspect and edit these from the corresponding \n" +
      "tabs on the main window.\n\n";
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
    tfCommand.setText(getInitialCmdTxt());
    panel.add(tfCommand, ct);
        
    ct.gridwidth = 3;
    ct.gridx = 0;
    ct.gridy = ct.gridy+1;
    JPanel row = new JPanel(new BorderLayout());
    ct.gridy = ct.gridy+1;
    tfOutputDir.setText(getInitialOutputDir());
    row.add(MyUtil.createCheckPanel1(thisFrame, "Output directory", tfOutputDir,
        true, true, true, false), BorderLayout.WEST);
    JPanel fieldPanel = new JPanel();
    fieldPanel.add(tfOutputDir);
    row.add(fieldPanel, BorderLayout.CENTER);
    panel.add(row, ct);
    
    ct.gridwidth = 3;
    ct.gridx = 0;
    ct.gridy = ct.gridy+1;
    //panel.add(new JLabel("Output file(s)"), ct);
    outputsPanel = new InOutPanel(false);
    panel.add(createHyperLinkPanel("Output file(s)", outputsPanel), ct);
    outputsPanel.setVisible(false);

    ct.gridy = ct.gridy+1;
    outputsPanel.addRow("file 1");
    panel.add(outputsPanel, ct);

    ct.gridwidth = 3;
    ct.gridx = 0;
    ct.gridy = ct.gridy+1;
    //panel.add(new JLabel("Input file(s)"), ct);
    inputsPanel = new InOutPanel(true);
    panel.add(createHyperLinkPanel("Input file(s)", inputsPanel), ct);
    inputsPanel.setVisible(false);

    ct.gridy = ct.gridy+1;
    inputsPanel.addRow("file 1");
    panel.add(inputsPanel, ct);

    ct.gridy = ct.gridy+1;
    ct.gridwidth = 3;
    ct.gridx = 0;
    JPanel buttonsPanel = new JPanel();
    for(int i=0; i<GridPilot.CS_NAMES.length; ++i){
      if(!MyUtil.checkCSEnabled(GridPilot.CS_NAMES[i])){
        continue;
      }
      JMenuItem mi = new JMenuItem(GridPilot.CS_NAMES[i]);
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
    
    JButton bCancel = MyUtil.mkButton("cancel.png", "Cancel", "Cancel");
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
    String markup = "<a href=\""+MyUtil.CHECK_URL+"\">"+text+"</a>";
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
    private JButton plusButton = MyUtil.mkButton1("more.png", "Unfold this panel", "+");
    private JButton minusButton = MyUtil.mkButton1("less.png", "Collapse this panel", "-");
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
        row.add(MyUtil.createCheckPanel1(thisFrame, label, field,
            true, true, false, false), BorderLayout.WEST);
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
   * Create a string suited for insertion as outputFileMapping in a job record.
   * @param names the output file names
   * @param outDirUrl the directory where all files should end up
   * @return the outputFileMapping String
   */
  private String createOutputMappingStr(String [] names, String outDirUrl){
    String mapStr = "";
    for(int i=0; i<names.length; ++i){
      mapStr += (i>0?"":" ") + names[i] + " " +outDirUrl+(outDirUrl.endsWith("/")?"":"/")+names[i];
    }
    return mapStr;
  }

  private String [][] createOutputMapping(String [] names, String outDirUrl){
    String [][] mapArr = new String [names.length][2];
    for(int i=0; i<names.length; ++i){
      mapArr[i][0] = names[i];
      mapArr[i][1] = outDirUrl+(outDirUrl.endsWith("/")?"":"/")+names[i];
    }
    return mapArr;
  }

  /**
   * Create and submit job.
   */
  ResThread workThread = null;
  private void runJob(final ActionEvent e){
    // get the command
    final String cmd = tfCommand.getText().trim();
    // get the output directory
    final String outputDir = tfOutputDir.getText().trim()+
       (tfOutputDir.getText().trim().endsWith("/")?"":"/");
    // get the output file names
    final String [] outputFiles;
    if(outputsPanel.isVisible()){
      outputFiles = new String [outputsPanel.fieldMap.values().size()];
      int i = 0;
      for(Iterator it=outputsPanel.fieldMap.values().iterator(); it.hasNext();){
        outputFiles[i] = ((JTextField) it.next()).getText().trim();
        ++i;
      }
    }
    else{
      outputFiles = null;
    }
    workThread = new ResThread(){
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
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(DB_NAME);
        // write cmd in the executable script
        String executableID = dbPluginMgr.getExecutableID(myExecutableName, myExecutableVersion);
        String scriptFile = (String) dbPluginMgr.getExecutable(executableID).getValue("script");
        try{
          LocalStaticShell.writeFile(MyUtil.clearTildeLocally(MyUtil.clearFile(scriptFile)),
              cmd, false);
        }
        catch(IOException e2){
          e2.printStackTrace();
          MyUtil.showError("ERROR: could not write executable script"+scriptFile+".\n" +
                "Check permissions.");
          return;
        }
        // update my_dataset with the output location and
        // my_executable with the output files
        String datasetID = null;
        try{
          datasetID = dbPluginMgr.getDatasetID(myDatasetName);
          dbPluginMgr.updateDataset(datasetID, myDatasetName,
              new String [] {"outputLocation"}, new String [] {outputDir});
          if(outputsPanel.isVisible()){
            dbPluginMgr.updateExecutable(executableID,
                new String [] {"outputFiles"}, new String [] {MyUtil.arrayToString(outputFiles)});
          }
          // If no output files are specified, zap the outputFiles of my_executable
          else{
            dbPluginMgr.updateExecutable(executableID,
                new String [] {"outputFiles"}, new String [] {""});
          }
        }
        catch(Exception e){
          e.printStackTrace();
          return;
        }
        // get the input file URLs
        String [] inputURLs;
        if(inputsPanel.isVisible()){
          inputsPanel.fieldMap.put("executable script",
              new JTextField(MyUtil.clearTildeLocally(MyUtil.clearFile(scriptFile))));
          inputURLs = new String [inputsPanel.fieldMap.values().size()];
          int i = 0;
          for(Iterator it=inputsPanel.fieldMap.values().iterator(); it.hasNext();){
            inputURLs[i] = ((JTextField) it.next()).getText().trim();
            ++i;
          }
        }
        else{
          inputURLs = new String [] {MyUtil.clearTildeLocally(MyUtil.clearFile(scriptFile))};
        }
        // create a guid. This will be used so we can get the identifier, which is needed
        // to submit...
        String uuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
        // create the job definition
        String [] fields = new String [] {
            "datasetName",
            "name",
            "guid",
            "status",
            "inputFileURLs",
            "outFileMapping",
            //"executableParameters",
            "stdoutDest",
            "stderrDest",
            "outputFileBytes"};
        String [] values = new String [] {
            myDatasetName,
            myDatasetName+".1",
            uuid,
            DBPluginMgr.getStatusName(DBPluginMgr.DEFINED),
            MyUtil.arrayToString(inputURLs).trim(),
            outputFiles==null?"":createOutputMappingStr(outputFiles, outputDir).trim(),
            //"executableParameters",
            outputDir+myDatasetName+".1.stdout",
            outputDir+myDatasetName+".1.stderr",
            "-1"};
        DBRecord jobDefinition = null;
        try{
          jobDefinition = dbPluginMgr.createJobDef(fields, values);
          /*dbPluginMgr.createJobDefinition(
              myDatasetName, fields, values, new String [] {},
              outputFiles==null?new String [][] {}:createOutputMapping(outputFiles, outputDir),
              outputDir+myDatasetName+".1.stdout",
              outputDir+myDatasetName+".1.stderr");*/
          // now get the identifier of the newly created jobDefinition record
          String idField = MyUtil.getIdentifierField(DB_NAME, "jobDefinition");
          String id = null;
          DBResult jobDefs = dbPluginMgr.getJobDefinitions(
              datasetID,
              new String [] {"guid", idField},
              new String [] {DBPluginMgr.getStatusName(DBPluginMgr.DEFINED)},
              null);
          Object checkGuid = null;
          for(int i=0; i<jobDefs.values.length; ++i){
            checkGuid = jobDefs.getValue(i, "guid");
            if(checkGuid!=null && checkGuid.equals(uuid)){
              id = (String) jobDefs.getValue(i,idField);
              break;
            }
          }
          if(id==null || id.equals("-1") || id.equals("")){
            throw new Exception("ERROR: could not get identifier of new jobDefinition.");
          }
          jobDefinition.setValue(idField, id);
        }
        catch(Exception e1){
          e1.printStackTrace();
        }
        Vector jobDefinitions = new Vector();
        jobDefinitions.add(jobDefinition);
        // submit the jobs
        statusBar.setLabel("Submitting. Please wait...");
        String csName = ((JMenuItem)e.getSource()).getText();
        GridPilot.getClassMgr().getSubmissionControl().submitJobDefinitions(jobDefinitions, csName, dbPluginMgr);
        statusBar.stopAnimation();
        statusBar.setLabel("");
        setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      }
    };

    workThread.start();
  }

  private void bSubmit_mousePressed(){
    if(tfCommand.getText()==null || tfCommand.getText().equals("")){
      MyUtil.showError("You must give a command");
      return;
    }
    // if a jobDefinition is selected, show the menu with computing systems
    pmSubmitMenu.show(this, 0, 0); // without this, pmSubmitMenu.getWidth == 0
    pmSubmitMenu.show(bSubmit, -pmSubmitMenu.getWidth(),
                      -pmSubmitMenu.getHeight() + bSubmit.getHeight());
  }

}
