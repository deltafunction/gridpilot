package gridpilot;

import java.awt.*;
import javax.swing.*;
import java.awt.event.*;

public class CreateEditDialog extends JDialog{

  private JPanel buttonPanel = new JPanel();
  private final int BPREV = 0;
  private final int BCREATE = 1;
  private final int BCLEAR = 2;

  JPanel pCreateEdit = new JPanel(new BorderLayout());
  CreateEditPanel createEditPanel;
  
  private boolean editing;

  private JButton bPrev = new JButton("Close");
  private JButton bCreate = null;
  private JButton bClear = new JButton("Clear");
  private JCheckBox cbShowResults = new JCheckBox("Show before writing to DB", true);


  public CreateEditDialog(JFrame frame, CreateEditPanel _panel, boolean _editing,
      boolean modal) {
    super(frame, "jobDefinition", true);
    
    createEditPanel = _panel;
    editing = _editing;
    
    if(editing){
      bCreate = new JButton("Update");
    }
    else{
      bCreate = new JButton("Create");
    }

    try {
      setContentPane(pCreateEdit);
      initGUI();
      pack();
      requestFocusInWindow();
      setModal(modal);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public void initGUI() {
    //buttonPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));

    // buttons initialisation

    bPrev.setMnemonic(BPREV);
    bPrev.addActionListener(new java.awt.event.ActionListener() {
      public void actionPerformed(ActionEvent e) {
        button_actionPerformed(e);
      }
    });

    bClear.setMnemonic(BCLEAR);
    bClear.addActionListener(new java.awt.event.ActionListener() {
      public void actionPerformed(ActionEvent e) {
        button_actionPerformed(e);
      }
    });

    bCreate.setMnemonic(BCREATE);
    bCreate.addActionListener(new java.awt.event.ActionListener() {
      public void actionPerformed(ActionEvent e) {
        button_actionPerformed(e);
      }
    });


    buttonPanel.add(cbShowResults);
    buttonPanel.add(bPrev);
    buttonPanel.add(bClear);
    buttonPanel.add(bCreate);

    pCreateEdit.add(createEditPanel, BorderLayout.NORTH);
    pCreateEdit.add(buttonPanel, BorderLayout.SOUTH);

    // center in window
    //
    //Dimension  scrnSize = Toolkit.getDefaultToolkit().getScreenSize();
    //setLocation((scrnSize.width / 2) - 150, (scrnSize.height / 2) - 50);
    //this.setSize(new Dimension(400, 0));

    // Initialize CreateEditPanel
    createEditPanel.initGUI();
  }

  /**
   * Called when a button is clicked
   */
  void button_actionPerformed(ActionEvent e) {
    switch(((JButton)e.getSource()).getMnemonic()){
      case BPREV :
        this.hide();
        break;

      case BCREATE :
        new Thread(){
          public void run(){
            createEditPanel.create(cbShowResults.isSelected(), editing);
          }
          }.start();
        //if(editing){
          //this.hide();
        //}
        break;

      case BCLEAR :
        createEditPanel.clear();
        break;

    }
  }
}
