package gridpilot;

import org.globus.util.GlobusURL;

/**
 * Interface implemented by classes providing file access.
 * All such classes, listed in the configuration file are
 * loaded by GridPilot.
 * Notice that in order to be loaded they must have a
 * constructor with no arguments.
 */
public interface FileTransfer {

  /**
   * This transfer has been queued, but hasn't yet started.
   */
  public final static int STATUS_WAIT = 1;
  /**
   * This transfer is running
   */
  public final static int STATUS_RUNNING = 2;
  /**
   * This transfer has succesfully finished
   */
  public final static int STATUS_DONE = 3;

  /**
   * An error occured, but it is temporary and possibly recoverable
   */
  public final static int STATUS_ERROR = 4;
  /**
   * A fatal error occured, this tranfer won't be checked any more
   */
  public final static int STATUS_FAILED = 5;

  /**
   * Checks if protocols are supported.
   * @param   srcUrls    the source URLs
   * @param   destUrls   the destination URLs
   * srcUrls and destUrls must have the same number of entries.
   */
  public boolean checkURLs(GlobusURL [] srcUrls,
      GlobusURL [] destUrls) throws Exception;

  /**
   * Initiate transfers and return identifiers.
   * @param   srcUrls    the source URLs
   * @param   destUrls   the destination URLs
   * srcUrls and destUrls must have the same number of entries.
   */
  public String [] startCopyFiles(GlobusURL [] srcUrls, GlobusURL [] destUrls) throws Exception;

  /**
   * Get the user ID used for file transfers by this plugin.
   */
  public String getUserInfo() throws Exception;

  /**
   * Get the full status of a specific file transfer.
   * @param   fileTransferID   the unique ID of the transfer.
   */
  public String getFullStatus(String fileTransferID) throws Exception;

  /**
   * Get the status of a specific file transfer.
   * @param   fileTransferID   the unique ID of the transfer.
   */
  public String getStatus(String fileTransferID) throws Exception;

  /**
   * Maps the status as returned by getStatus of this plugin of
   * to the corresponding internal status code (FileTransfer.STATUS_*).
   * @param   ftStatus   status of the transfer.
   */
  public int getInternalStatus(String ftStatus) throws Exception;

  /**
   * Get the size of the file in bytes.
   * May return -1 if the information is not available.
   * @param   fileTransferID   the unique ID of the transfer.
   */
  public long getFileBytes(GlobusURL url) throws Exception;

  /**
   * Get the number of bytes of the file that has been copied.
   * May return -1 if the information is not available.
   * @param   fileTransferID   the unique ID of the transfer.
   */
  public long getBytesTransferred(String fileTransferID) throws Exception;

  /**
   * Get the percentage of the file that has been copied.
   * Returns a number between 0 and 100. May return -1 if
   * the information is not available.
   * @param   fileTransferID   the unique ID of the transfer.
   */
  public int getPercentComplete(String fileTransferID) throws Exception;
 
   /**
   * Cancel the transfer and release the file on the server.
   * @param   fileTransferID   the unique ID of the transfer.
   */
  public void cancel(String fileTransferID) throws Exception;

  /**
   * Release the file on the server.
   * @param   fileTransferID   the unique ID of this transfer.
   */
  public void finalize(String fileTransferID) throws Exception;

  /**
   * Delete a list of files.
   * @param   destUrls    list of files to be deleted on the server.
   */
  public void deleteFiles(GlobusURL [] destUrls) throws Exception;

}
