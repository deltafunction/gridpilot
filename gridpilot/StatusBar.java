package gridpilot;

import javax.swing.*;

import gridpilot.GridPilot;
import gridpilot.Debug;

import java.awt.event.*;
import java.awt.*;
import java.net.URL;

/**
 * The <code>StatusBar</code> implements a status bar in two parts : a label on the left,
 * and a progress bar on the right. <p>
 * The label can be used in two mode : in the first one, the label is shown until another label
 * overwrite this label, in the second mode, the label is shown until a specified delay
 * is elapsed or another label is set. <br>
 * The progress bar has two mode as well : the first one is a "normal" progress bar which is
 * created outside this status bar, and the second one is an undeterminate progress bar.
 */
public class StatusBar extends JPanel {

  private static final long serialVersionUID = 1L;
  private JLabel label = new JLabel();
  private boolean labelOn = false;
  private JProgressBar indeterminatePB = new JProgressBar();
  private JComponent comp = new JLabel("");

  java.util.Stack stackProgressBar = new java.util.Stack();

  private Frame frame;
  private ImageIcon save = new ImageIcon();
  private ImageIcon waitingIcon;
  // Semaphore
  private boolean statusBarActive = false;

  public StatusBar(){
    setLayout(new BorderLayout());

    add(new JLabel(" "), BorderLayout.WEST);
    comp = new JLabel("");
    add(comp, BorderLayout.CENTER);
    
    URL imgURL=null;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "wait.png");
      waitingIcon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.resourcesPath + "wait.png", 3);
      waitingIcon = new ImageIcon();
    }
  }

  /**
   * Sets this JComponent in the center on this status bar
   */
  public /*synchronized*/ void setCenterComponent(JComponent _comp){

    if(statusBarActive){
      return;
    }
    statusBarActive = true;
    remove(comp);
    comp = _comp;
    add(comp, BorderLayout.CENTER);
    updateUI();
    statusBarActive = false;
  }

  public /*synchronized*/ void clearCenterComponent(){
    Debug.debug("Trying to clear center component", 2);
    if(statusBarActive){
      return;
    }
    statusBarActive = true;
    remove(comp);
    comp = new JLabel("");
    add(comp, BorderLayout.CENTER);
    updateUI();
    statusBarActive = false;
    Debug.debug("Center component cleared", 2);
  }
  
  public boolean isCenterComponentSet(){
    try{
      JLabel jComp = ((JLabel) comp);
      return !jComp.getText().equals("");
    }
    catch(Exception e){
      return true;
    }
  }

 /**
   * Sets this JLabel on the left on this status bar
   */
  public /*synchronized*/ void setLabel(JLabel _label){

    if(statusBarActive){
      return;
    }
    statusBarActive = true;
    
    remove(label);
    label = new JLabel(" ");
    add(label, BorderLayout.WEST);
    updateUI();

    label = _label;

    add(label, BorderLayout.WEST);

    updateUI();
    
    labelOn = true;
    
    statusBarActive = false;
  }

  /**
   * Sets this String on the left of this status bar
   */
  public /*synchronized*/ void setLabel(String s){
    //setLabel(new JLabel(s));
    if(statusBarActive){
      return;
    }
    statusBarActive = true;
    label.setText(s);
    if(!labelOn){
      add(label, BorderLayout.WEST);
      labelOn = true;
    }
    label.updateUI();
    statusBarActive = false;
  }

  /**
   * Sets this JProgressBar on the rigth of this status bar. <br>
   */
  public /*synchronized*/ void setProgressBar(JProgressBar pb) {
    if(statusBarActive){
      return;
    }
    statusBarActive = true;
    if(!stackProgressBar.empty()){
      //remove((JProgressBar) stackProgressBar.peek());
      statusBarActive = false;
      return;
    }
    stackProgressBar.push(pb);
    add(pb, BorderLayout.EAST);
    statusBarActive = false;
  }

  /**
   * Removes the current label
   */
  public /*synchronized*/ void removeLabel(){
    if(statusBarActive){
      return;
    }
    statusBarActive = true;
    remove(label);
    label = new JLabel(" ");
    add(label, BorderLayout.CENTER);
    updateUI();
    statusBarActive = false;
  }

  /**
   * Removes the current progress bar. <p>
   * If another progress bar was shown before that <code>p</code> was set, the
   * previous progress bar is re-shown (if this old progress bar hasn't been removed)
   */
  public /*synchronized*/ void removeProgressBar(JProgressBar p){
    if(statusBarActive){
      return;
    }
    statusBarActive = true;
    if(p==null){
      statusBarActive = false;
      return;
    }
    remove(p);
    if(stackProgressBar.contains(p)){
      stackProgressBar.remove(p);
    }
    if(!stackProgressBar.isEmpty()){
      setProgressBar((JProgressBar) stackProgressBar.pop());
    }
    updateUI();
    statusBarActive = false;
  }


  /**
   * Sets an indeterminate progress bar on the right of this progress bar
   */
  public /*synchronized*/ void animateProgressBar(){
    if(statusBarActive){
      return;
    }
    statusBarActive = true;
    setProgressBar(indeterminatePB);
    indeterminatePB.setIndeterminate(true);
    if(frame==null){
      frame = JFrame.getFrames()[1]; // ?? dangerous !!!
    }

    save.setImage(frame.getIconImage());
    frame.setIconImage(waitingIcon.getImage());
    statusBarActive = false;
  }

  /**
   * Removes this animated progress bar
   */
  public /*synchronized*/ void stopAnimation(){
    if(statusBarActive){
      return;
    }
    statusBarActive = true;
    indeterminatePB.setIndeterminate(false);
    removeProgressBar(indeterminatePB);
    indeterminatePB.removeAll();
    frame.setIconImage(save.getImage());
    statusBarActive = false;
  }

  /**
   * Adds a MouseListener to the animated progres bar. <p>
   */
  public void addIndeterminateProgressBarMouseListener(MouseListener ml){
    if(statusBarActive){
      return;
    }
    statusBarActive = true;
    indeterminatePB.addMouseListener(ml);
    statusBarActive = false;
  }

  /**
   * Adds a ToolTip to animated progres bar. <p>
   */
  public void setIndeterminateProgressBarToolTip(String s){
    if(statusBarActive){
      return;
    }
    statusBarActive = true;
    indeterminatePB.setToolTipText(s);
    statusBarActive = false;
  }
}

