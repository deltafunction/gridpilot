package gridpilot.wizards.manage_software;

import gridpilot.Debug;
import gridpilot.GPFrame;
import gridpilot.GridPilot;
import gridpilot.Util;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.IOException;
import java.util.Arrays;

import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;

public class CreateSoftwarePackageWizard extends GPFrame{
  
  private static final long serialVersionUID = 493635130266263014L;

  private static int TEXTFIELDWIDTH = 32;

  /**
   *                    "Create software package" : tell to copy all file to one
   *                                                directory, ...
   *                                                create install, runtime, remove scripts
   *                                                make tarball
   *                                                ask if it should be uploaded to a URL
   *                                                ask if it should be published
   */
  public CreateSoftwarePackageWizard(){
    Debug.debug("Starting software package creation wizard...", 2);
    JScrollPane sp = new JScrollPane();
    JPanel jp = new JPanel(new GridBagLayout());
    jp.add(mkDirPanel(), new GridBagConstraints(0, 0, 2, 1, 0.0, 0.0,
        GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
        new Insets(5, 5, 5, 5), 0, 0));
    jp.add(mkScriptsPanel(), new GridBagConstraints(0, 1, 2, 1, 0.0, 0.0,
        GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
        new Insets(5, 5, 5, 5), 0, 0));
    sp.getViewport().add(jp);
    add(sp);
  }
  
  private JPanel mkDirPanel(){
    JPanel panel = new JPanel();
    panel.setLayout(new GridBagLayout());
    String title = "Create new software package (runtime environment)";
    setTitle(title);
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Step 1/5: consolidate all files in a directory"));
    String msg = "If all files this software needs are not already in a single directory, to one directory, please copy them to one directory.\n\n" +
        "Below, give the path to the directory containing all files of this software.\n\n";
    JLabel jlDirInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");
    JTextField jtf =  new JTextField(TEXTFIELDWIDTH);

    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.VERTICAL;
    ct.anchor = GridBagConstraints.ABOVE_BASELINE;
    ct.insets = new Insets(7, 7, 7, 7);
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth = 2;
    ct.gridheight = 1;
    panel.add(jlDirInstructions, ct);
    
    ct.gridy = 1;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    panel.add(Util.createCheckPanel1(JOptionPane.getRootFrame(),
        "Software directory", jtf, true), ct);    
    ct.gridy = 1;
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.insets = new Insets(7, 7, 7, 7);
    panel.add(jtf, ct);    
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
        Color.white, new Color(165, 163, 151)), "Step 2/5: create install, runtime and remove scripts"));
    String msg = "You need to create three scripts that will be packed together with your software:\n\n" +
        "- an install script that installs the sofware\n\n" +
        "- a setup script that sets up the environment of a job so it can use the installed software,\n" +
        "  e.g. adds relevant executables to the shell PATH\n\n" +
        "- a removal script\n\n" +
        "These script have a number of variables at their disposal:\n\n" +
        "" +
        "For each script, a template is provided, by clicking on the buttons below, you can edit these\n" +
        "to match your specific software.\n\n";
    JLabel jlInstructions = new JLabel("<html>"+msg.replaceAll("\n", "<br>")+"</html>");
    JTextField jtfInstall =  new JTextField(TEXTFIELDWIDTH);
    JTextField jtfRuntime =  new JTextField(TEXTFIELDWIDTH);
    JTextField jtfRemove =  new JTextField(TEXTFIELDWIDTH);
    JPanel jpInstall = Util.createCheckPanel1(JOptionPane.getRootFrame(),
        "Install script", jtfInstall, true);
    JPanel jpRuntime = Util.createCheckPanel1(JOptionPane.getRootFrame(),
        "Setup script", jtfRuntime, true);
    JPanel jpRemove = Util.createCheckPanel1(JOptionPane.getRootFrame(),
        "Remove script", jtfRemove, true);

    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.BOTH;
    ct.anchor = GridBagConstraints.ABOVE_BASELINE;
    ct.insets = new Insets(7, 7, 7, 7);
    
    ct.gridx = 0;
    ct.gridy = 0;   
    ct.gridwidth = 2;
    ct.gridheight = 1;
    panel.add(jlInstructions, ct);
    
    ct.gridy = 1;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    ct.insets = new Insets(0, 0, 0, 0);
    panel.add(jpInstall, ct);    
    ct.gridy = 1;
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.insets = new Insets(7, 7, 7, 7);
    panel.add(jtfInstall, ct); 
    
    ct.gridy = 3;
    ct.gridx = 0;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    ct.insets = new Insets(0, 0, 0, 0);
    panel.add(jpRuntime, ct);    
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.insets = new Insets(7, 7, 7, 7);
    panel.add(jtfRuntime, ct); 
    
    ct.gridy = 5;
    ct.gridx = 0;
    ct.gridwidth = 1;
    ct.gridheight = 2;
    ct.insets = new Insets(0, 0, 0, 0);
    panel.add(jpRemove, ct);    
    ct.gridx = 1;
    ct.gridheight = 1;
    ct.insets = new Insets(7, 7, 7, 7);
    panel.add(jtfRemove, ct); 
    
    return panel;
  }

}
