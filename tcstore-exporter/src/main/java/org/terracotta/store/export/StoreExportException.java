/**
 * StoreExportException
 */
package org.terracotta.store.export;


public class StoreExportException extends Exception {
  public StoreExportException() {
    super();
  }

  public StoreExportException(String message) {
    super(message);
  }

  public StoreExportException(String message, Throwable cause) {
    super(message, cause);
  }

}