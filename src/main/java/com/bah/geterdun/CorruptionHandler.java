package com.bah.geterdun;

import com.bah.geterdun.LogResolver.CorruptLogException;

/**
 * Handles corrupt logs. This happens whenever the format of the log cannot be
 * read (probably because of a corrupt block on the file system, or an
 * incomplete log record, which is unlikely but technically possible). The log
 * format is not recoverable, so after a corrupt record the rest of the log is
 * effectively lost.
 */
public interface CorruptionHandler {

  void handleCorruption(CorruptLogException e);

}
