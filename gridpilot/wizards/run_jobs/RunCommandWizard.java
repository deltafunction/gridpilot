package gridpilot.wizards.run_jobs;

import gridpilot.BrowserPanel;
import gridpilot.DBPluginMgr;
import gridpilot.DBRecord;
import gridpilot.GPFrame;
import gridpilot.GridPilot;
import gridpilot.MyThread;
import gridpilot.Util;
import gridpilot.wizards.manage_software.CreateSoftwarePackageWizard;

import java.awt.Color;
import java.awt.Cursor;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkListener;

public class RunCommandWizard extends GPFrame{
  
  private JButton bSubmit = new JButton("Submit job(s)");
  private JPopupMenu pmSubmitMenu = new JPopupMenu();
  private RunCommandWizard thisFrame = this;
  private JTextArea jtf = null;

  private static int TEXTFIELDWIDTH = 20;
  
  public RunCommandWizard() {
    jtf = Util.createTextArea(TEXTFIELDWIDTH);
  }

  /**
   * - query for a command and a CS
   * - modify no_files_transformation script
   * - delete jobDefinition of no_files_dataset
   * - create jobDefinition of no_files_dataset
   * - submit job
   */
  private JPanel mkPanel(){
        
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    setTitle("Run command");
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Step 1/4: choose name"));
    String msg = 
      "This wizard will help you run one or several UNIX/Linux commands and view the text output\n" +
      "this produces.\n\n." +
      "Simply type in a command - or multiple commands separated with semi-colons or spaces,\n" +
      "choose a computing system and click \"Run\"; then use right-click menu on the job\n" +
      "monitoring panel to follow the progress of your job.";
    JLabel jlDirInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");
    JPanel textPanel = new JPanel();
    textPanel.add(jtf);

    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.BOTH;
    ct.anchor = GridBagConstraints.WEST;
    ct.weightx = 0;
    ct.insets = new Insets(7, 7, 7, 7);
    
    ct.gridx = 0;
    ct.gridy = 0;
    ct.gridwidth = 4;
    ct.gridheight = 1;
    panel.add(jlDirInstructions, ct);
    
    ct.gridy = ct.gridy+1;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    panel.add(new JLabel("Command(s)"), ct);
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.ipady = -10;
    panel.add(textPanel, ct);
    ct.ipady = -5;
    
    JButton continueButton = new JButton("Cancel");
    continueButton.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        thisFrame.dispose();
      }
    }
    );
    ct.gridx = 2;
    JPanel pCont = new JPanel();
    pCont.add(continueButton);
    panel.add(pCont, ct);

    ct.gridx = 3;
    ct.weightx = 10;
    panel.add(new JLabel(" "), ct);
    
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
      //mi.setMnemonic(i);
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


    
    return panel;
  }
  
  /**
   * Create and submit job.
   */
  MyThread workThread = null;
  private void runJob(final ActionEvent e){
    String cmd = jtf.getText().trim();
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
        // Create the job definition
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr("My_DB_Local");
        String [] fields = new String [] {"datasetName", "name", "userInfo",
            "stdoutDest", "stderrDest"};
        Object [] values = new String [] {"datasetName", "name", "userInfo",
            "stdoutDest", "stderrDest"};
        DBRecord jobDefinition = null;
        try{
          jobDefinition = dbPluginMgr.createJobDef(fields, values);
        }
        catch (Exception e1) {
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
