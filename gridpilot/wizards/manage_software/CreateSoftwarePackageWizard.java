package gridpilot.wizards.manage_software;

import gridpilot.Debug;
import gridpilot.GPFrame;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;

public class CreateSoftwarePackageWizard extends GPFrame{
  
  private static final long serialVersionUID = 493635130266263014L;
  private JPanel pTop = new JPanel();

  /**
   * TODO:
   * 
   *                    "Create software package" : tell to copy all file to one
   *                                                directory, ...
   *                                                make tarball
   *                                                ask if it should be uploaded to a URL
   *                                                ask if it should be published
   */
  public CreateSoftwarePackageWizard(){
    Debug.debug("Starting software package creation wizard...", 2);
    initGUI();
  }
  
  private void initGUI(){
    String title = "Create new software package (runtime environment)";
    setTitle(title);
    ((JPanel) this.getContentPane()).setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Step 1/4: consolidate all files in a directory"));
    JLabel jlDirInstructions = new JLabel("Please copy or move all files this software needs to one directory.");
    pTop.add(jlDirInstructions);
    setLayout(new BorderLayout());
    pTop.setLayout(new FlowLayout());
    
    add(pTop);
    ((JPanel) this.getContentPane()).updateUI();
  }
}
