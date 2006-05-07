package gridpilot;

import java.awt.*;
import javax.swing.*;
import java.awt.event.*;

public class CreateEditDialog extends GPFrame{

  private static final long serialVersionUID = 1L;
  private final int BCLOSE = 0;
  private final int BCREATEUPDATE = 1;
  private final int BCLEAR = 2;
  private JPanel pCreateEdit = new JPanel(new BorderLayout());
  private CreateEditPanel createEditPanel;
  private boolean editing;
  private JButton bClose = new JButton("Close");
  private JButton bCreateUpdate = null;
  private JButton bClear = new JButton("Clear");
  private JCheckBox cbShowResults = new JCheckBox("Show before writing to DB", true);

  public JPanel buttonPanel = new JPanel();

  public CreateEditDialog(CreateEditPanel _panel, boolean _editing){
    
    super();
    
    createEditPanel = _panel;
    editing = _editing;
    
    this.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
    this.addWindowListener(new WindowAdapter(){
      public void windowClosing(WindowEvent we){
        createEditPanel.windowClosing();
        we.getWindow().setVisible(false);
        //Debug.debug("Thwarted user attempt to close window.", 3);
      }
    });
    
    if(editing){
      bCreateUpdate = new JButton("Update");
    }
    else{
      bCreateUpdate = new JButton("Create");
    }

    try{
      setContentPane(pCreateEdit);
      initGUI();
      pack();
      requestFocusInWindow();
      // Doesn't seem to make any difference...
      //setAlwaysOnTop(false);
      this.setVisible(true);
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }
  
  public void initGUI(){
    //buttonPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));

    // buttons initialisation

    bClose.setMnemonic(BCLOSE);
    bClose.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        button_actionPerformed(e);
      }
    });

    bClear.setMnemonic(BCLEAR);
    bClear.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        button_actionPerformed(e);
      }
    });

    bCreateUpdate.setMnemonic(BCREATEUPDATE);
    bCreateUpdate.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        button_actionPerformed(e);
      }
    });


    buttonPanel.add(cbShowResults);
    buttonPanel.add(bClose);
    buttonPanel.add(bClear);
    buttonPanel.add(bCreateUpdate);

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
  void button_actionPerformed(ActionEvent e){
    switch(((JButton)e.getSource()).getMnemonic()){
      case BCLOSE :
        createEditPanel.windowClosing();
        this.setVisible(false);
        break;

      case BCREATEUPDATE :
        new Thread(){
          public void run(){
            createEditPanel.create(cbShowResults.isSelected(), editing);
          }
        }.start();
        break;

      case BCLEAR :
        createEditPanel.clearPanel();
        break;
    }
  }
}
