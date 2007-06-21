package gridpilot.wizards.manage_software;

import gridpilot.Debug;
import gridpilot.GPFrame;
import gridpilot.GridPilot;
import gridpilot.Util;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Rectangle;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.io.IOException;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;

public class CreateSoftwarePackageWizard extends GPFrame{
  
  private static final long serialVersionUID = 493635130266263014L;

  private static int TEXTFIELDWIDTH = 20;
  private static int TEXTFIELDHEIGHT = 20;
  private static String title = "Create new software package (runtime environment)";
  
  private JPanel namePanel = null;
  private JPanel dirPanel = null;
  private JPanel scriptsPanel = null;
  private JPanel uploadPanel = null;
  private JButton cancelButton = new JButton("Cancel");
  private JButton okButton = new JButton("OK");
  private JScrollPane sp = new JScrollPane();
  
  private CreateSoftwarePackageWizard thisFrame = this;

  /**
   *                    "Create software package" : - ask for name
   *                                                - tell to copy all file to one
   *                                                    directory, ...
   *                                                - create install, runtime, remove scripts
   *                                                -->make tarball
   *                                                - ask for upload URL + publish catalog
   */
  public CreateSoftwarePackageWizard(){
    JPanel jp = new JPanel(new GridBagLayout());
    jp.add(welcomePanel(), new GridBagConstraints(0, 0, 2, 1, 0.0, 0.0,
        GridBagConstraints.WEST, GridBagConstraints.BOTH,
        new Insets(5, 5, 5, 5), 0, 0));
    namePanel = getNamePanel();
    jp.add(namePanel, new GridBagConstraints(0, 1, 2, 1, 0.0, 0.0,
        GridBagConstraints.WEST, GridBagConstraints.BOTH,
        new Insets(5, 5, 5, 5), 0, 0));
    dirPanel = getDirPanel();
    jp.add(dirPanel, new GridBagConstraints(0, 2, 2, 1, 0.0, 0.0,
        GridBagConstraints.WEST, GridBagConstraints.BOTH,
        new Insets(5, 5, 5, 5), 0, 0));
    scriptsPanel = mkScriptsPanel();
    jp.add(scriptsPanel, new GridBagConstraints(0, 3, 2, 1, 0.0, 0.0,
        GridBagConstraints.WEST, GridBagConstraints.BOTH,
        new Insets(5, 5, 5, 5), 0, 0));
    uploadPanel = uploadPanel();
    jp.add(uploadPanel, new GridBagConstraints(0, 4, 2, 1, 0.0, 0.0,
        GridBagConstraints.WEST, GridBagConstraints.BOTH,
        new Insets(5, 5, 5, 5), 0, 0));
    JPanel jpp = new JPanel(new BorderLayout());
    jpp.add(jp);
    sp.getViewport().add(jpp);
    add(sp);
    
    okButton.setEnabled(false);
    updateComponentTreeUI0(dirPanel, false);
    updateComponentTreeUI0(scriptsPanel, false);
    updateComponentTreeUI0(uploadPanel, false);
    
    int maxHeight = Toolkit.getDefaultToolkit().getScreenSize().height-10;
    int maxWidth = Toolkit.getDefaultToolkit().getScreenSize().width-10;
    this.setMaximumSize(new Dimension(maxWidth, maxHeight>400?400:maxHeight));
    this.setPreferredSize(new Dimension(660, 400));
    this.pack();
  }
  
  private JPanel welcomePanel(){
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    setTitle(title);
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Welcome"));
    String msg = 
            "Welcome to the software package creation wizard.\n\n" +
            "You will now be guided through 4 simple steps to create\n" +
            "and publish a software package AKA a runtime environment.\n\n" +
            "You can exit this wizard at any time by clicking \"Cancel\"";
    JLabel jlDirInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");

    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.BOTH;
    ct.anchor = GridBagConstraints.WEST;
    ct.weightx = 0;
    ct.insets = new Insets(7, 7, 7, 7);
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth = 3;
    ct.gridheight = 1;
    panel.add(jlDirInstructions, ct);
    
    cancelButton.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          exit();
        }
        catch(Throwable e1){
          e1.printStackTrace();
        }
      }
    }
    );
    okButton.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          thisFrame.dispose();
        }
        catch(Throwable e1){
          e1.printStackTrace();
        }
      }
    }
    );
    ct.gridwidth = 1;
    ct.gridy = 1;
    ct.ipady = 0;
    JPanel cancelPanel = new JPanel();
    cancelPanel.add(cancelButton, ct);
    panel.add(cancelPanel, ct);
    ct.gridx = 1;
    JPanel okPanel = new JPanel();
    cancelPanel.add(okButton, ct);
    panel.add(okPanel, ct);
    ct.weightx = 10;
    ct.gridx = 2;
    panel.add(new JLabel(" "), ct);    
    return panel;
  }
  
  private void exit(){
    // TODO: remove temporary directory
    thisFrame.dispose();
  }
  
  private JPanel getNamePanel(){
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    setTitle(title);
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Step 1/4: choose name"));
    String msg = 
            "First, you have to choose a name for the software package.\n" +
            "If you plan to publish it in a runtime environment catalog, you should follow the convention of\n" +
            "of scoping the name and giving it a versions number. E.g. version 12.0.6 of the ATLAS software\n" +
            "package used in High Energy Physics is named \"APP/HEP/ATLAS/ATLAS-12.0.6\"\n\n";
    JLabel jlDirInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");
    JTextField jtf =  new JTextField(TEXTFIELDWIDTH);
    jtf.setPreferredSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));
    jtf.setSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));

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
    
    ct.gridy = 1;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    ct.insets = new Insets(0, 7, 0, 7);
    panel.add(new JLabel("Name"), ct);
    ct.gridy = 1;
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.insets = new Insets(7, 7, 7, 7);
    panel.add(jtf, ct);
    
    JButton continueButton = new JButton("Continue");
    continueButton.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        updateComponentTreeUI0(dirPanel, true);
        updateComponentTreeUI0(namePanel, false);
        // TODO: find out how to scroll down...
        sp.scrollRectToVisible(new Rectangle(0, 20));
      }
    }
    );
    ct.gridx = 2;
    panel.add(continueButton, ct);

    ct.gridx = 3;
    ct.weightx = 10;
    panel.add(new JLabel(" "), ct);

    return panel;
  }
  
  private JPanel getDirPanel(){
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    setTitle(title);
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Step 2/4: consolidate all files in a directory"));
    String msg = "If all files this software needs are not already in a single directory,\n" +
            "to one directory, please copy them to one directory.\n\n" +
        "Below, give the path to the directory containing all files of this software.\n\n";
    JLabel jlDirInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");
    JTextField jtf =  new JTextField(TEXTFIELDWIDTH);
    jtf.setPreferredSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));
    jtf.setSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));

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
    
    ct.gridy = 1;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    ct.insets = new Insets(0, 7, 0, 7);
    panel.add(Util.createCheckPanel1(this,
        "Directory containing software", jtf, true), ct);    
    ct.gridy = 1;
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.insets = new Insets(7, 7, 7, 7);
    panel.add(jtf, ct);    
    JButton continueButton = new JButton("Continue");
    continueButton.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        updateComponentTreeUI0(scriptsPanel, true);
        updateComponentTreeUI0(dirPanel, false);
      }
    }
    );
    ct.gridx = 2;
    panel.add(continueButton, ct);
    ct.gridx = 3;
    ct.weightx = 10;
    panel.add(new JLabel(" "), ct);
    return panel;
  }
  
  private void createPackagingTmpDir() throws IOException {
    String cacheDir = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "Pull cache directory");
    if(cacheDir==null || cacheDir.equals("")){
      throw new IOException("No cache directory defined.");
    }
    
  }
  
  private JPanel mkScriptsPanel(){
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    String title = "Create scripts";
    setTitle(title);
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Step 3/4: create install, runtime and remove scripts"));
    String msg = "You need to create three scripts that will be packed together with your software:\n\n" +
        "- an install script that installs the sofware\n\n" +
        "- a setup script that sets up the environment of a job so it can use the installed software\n\n" +
        "- a removal script\n\n" +
        "These script have a number of variables at their disposal:\n\n" +
        "" +
        "For each script, a template is provided. By clicking on the buttons below, you can edit these\n" +
        "to match your specific software.\n\n";
    JLabel jlInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");
    JTextField jtfInstall =  new JTextField(TEXTFIELDWIDTH);
    JTextField jtfRuntime =  new JTextField(TEXTFIELDWIDTH);
    JTextField jtfRemove =  new JTextField(TEXTFIELDWIDTH);
    jtfInstall.setPreferredSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));
    jtfInstall.setSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));
    jtfRuntime.setPreferredSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));
    jtfRuntime.setSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));
    jtfRemove.setPreferredSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));
    jtfRemove.setSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));
    JPanel jpInstall = Util.createCheckPanel1(this,
        "Install script", jtfInstall, true);
    JPanel jpRuntime = Util.createCheckPanel1(this,
        "Setup script", jtfRuntime, true);
    JPanel jpRemove = Util.createCheckPanel1(this,
        "Remove script", jtfRemove, true);

    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.BOTH;
    ct.anchor = GridBagConstraints.WEST;
    ct.insets = new Insets(7, 7, 7, 7);
    ct.weightx = 0;
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth = 4;
    ct.gridheight = 1;
    panel.add(jlInstructions, ct);
    
    ct.gridy = 1;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    ct.insets = new Insets(0, 7, 0, 7);
    panel.add(jpInstall, ct);    
    ct.gridy = 1;
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.insets = new Insets(7, 7, 7, 7);
    panel.add(jtfInstall, ct); 
    ct.gridx = 2;
    panel.add(new JLabel(" "), ct);    
    
    ct.gridy = 3;
    ct.gridx = 0;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    ct.insets = new Insets(0, 7, 0, 7);
    panel.add(jpRuntime, ct);    
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.insets = new Insets(7, 7, 7, 7);
    panel.add(jtfRuntime, ct); 
    ct.gridx = 2;
    panel.add(new JLabel(" "), ct);    
    
    ct.gridy = 5;
    ct.gridx = 0;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    ct.insets = new Insets(0, 7, 0, 7);
    panel.add(jpRemove, ct);    
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.insets = new Insets(7, 7, 7, 7);
    panel.add(jtfRemove, ct); 
    
    JButton continueButton = new JButton("Continue");
    continueButton.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        updateComponentTreeUI0(uploadPanel, false);
        updateComponentTreeUI0(scriptsPanel, false);
      }
    }
    );
    ct.gridx = 2;
    panel.add(continueButton, ct);
    
    ct.gridx = 3;
    ct.weightx = 10;
    panel.add(new JLabel(" "), ct);
    
    return panel;
  }
  
  private JPanel uploadPanel(){
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    String title = "Upload and publish runtime environment";
    setTitle(title);
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Step 4/4: upload runtime environment and publish the URL in a catalog"));
    String msg = "The software has now been packaged and is ready to be uploaded to a\n" +
        "remote URL. This URL will then be published in the catalog you choose.\n\n";
    JLabel jlInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");
    JTextField jtfInstall =  new JTextField(TEXTFIELDWIDTH);
    JTextField jtfRuntime =  new JTextField(TEXTFIELDWIDTH);
    jtfInstall.setPreferredSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));
    jtfInstall.setSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));
    jtfRuntime.setPreferredSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));
    jtfRuntime.setSize(new Dimension(TEXTFIELDWIDTH, TEXTFIELDHEIGHT));
    JPanel jpInstall = Util.createCheckPanel1(this,
        "Install script", jtfInstall, true);
    JPanel jpRuntime = Util.createCheckPanel1(this,
        "Setup script", jtfRuntime, true);

    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.BOTH;
    ct.anchor = GridBagConstraints.WEST;
    ct.insets = new Insets(7, 7, 7, 7);
    ct.weightx = 0;
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth = 4;
    ct.gridheight = 1;
    panel.add(jlInstructions, ct);
    
    ct.gridy = 1;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    ct.insets = new Insets(0, 7, 0, 7);
    panel.add(jpInstall, ct);    
    ct.gridy = 1;
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.insets = new Insets(7, 7, 7, 7);
    panel.add(jtfInstall, ct); 
    ct.gridx = 2;
    panel.add(new JLabel(" "), ct);    
    
    ct.gridy = 3;
    ct.gridx = 0;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    ct.insets = new Insets(0, 7, 0, 7);
    panel.add(jpRuntime, ct);    
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.insets = new Insets(7, 7, 7, 7);
    panel.add(jtfRuntime, ct); 
    ct.gridx = 2;
    panel.add(new JLabel(" "), ct);    
        
    JButton continueButton = new JButton("Continue");
    continueButton.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        updateComponentTreeUI0(uploadPanel, false);
        cancelButton.setEnabled(false);
        okButton.setEnabled(true);
      }
    }
    );
    ct.gridx = 2;
    panel.add(continueButton, ct);
    
    ct.gridx = 3;
    ct.weightx = 10;
    panel.add(new JLabel(" "), ct);
    
    return panel;
  }
  
  private static void updateComponentTreeUI0(Component c, boolean enabled){
    if(c instanceof JComponent){
      ((JComponent) c).setEnabled(enabled);
      ((JComponent) c).updateUI();
    }
    Component[] children = null;
    if(c instanceof JMenu){
      children = ((JMenu)c).getMenuComponents();
    }
    else if(c instanceof Container){
      children = ((Container)c).getComponents();
    }
    if(children!=null){
      for(int i=0; i<children.length; i++){
        updateComponentTreeUI0(children[i], enabled);
      }
    }
  }


}
