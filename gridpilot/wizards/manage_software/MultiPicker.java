package gridpilot.wizards.manage_software;

import javax.swing.*;

/**
 * From http://forum.java.sun.com/thread.jspa?messageID=4155224&tstart=0
 */
public class MultiPicker extends JList {
  private static final long serialVersionUID=-5388795072961412046L;

  public MultiPicker() {
    super();
  }

  public MultiPicker(String[] items) {
    
    super( items );

    DefaultListSelectionModel selectionModel = new DefaultListSelectionModel() {
      private static final long serialVersionUID=-1135870501580517949L;
        // implements javax.swing.ListSelectionModel
        public boolean isSelectedIndex( int index ) {
            if( index == 0 ) return false;
            return super.isSelectedIndex( index );
        }
    };
    this.setSelectionMode( ListSelectionModel.MULTIPLE_INTERVAL_SELECTION );
    this.setSelectionModel( selectionModel );
  }
    
}