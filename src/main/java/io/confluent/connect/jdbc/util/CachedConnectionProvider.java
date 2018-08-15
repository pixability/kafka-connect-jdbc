/*
 *  Copyright 2016 Confluent Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.confluent.connect.jdbc.util;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class CachedConnectionProvider {

  private static final Logger log = LoggerFactory.getLogger(CachedConnectionProvider.class);

  private static final int VALIDITY_CHECK_TIMEOUT_S = 5;

  private final String url;
  private final String username;
  private final String password;
  private final int maxBorrows;

  private Connection connection;
  private int borrows = 0;

  public CachedConnectionProvider(String url) {
    this(url, null, null, 0);
  }

  public CachedConnectionProvider(String url, String username, String password) {
    this(url, username, password, 0);
  }

  public CachedConnectionProvider(String url, String username, String password, int maxBorrows) {
    this.url = url;
    this.username = username;
    this.password = password;
    this.maxBorrows = maxBorrows;
  }

  public synchronized Connection getValidConnection() {
    try {
      if (connection == null) {
        newConnection();
      } else if ((maxBorrows > 0 && ++borrows > maxBorrows) || !connection.isValid(VALIDITY_CHECK_TIMEOUT_S)) {
        log.info("Invalidating current connection and reconnecting...");
        closeQuietly();
        newConnection();
      }
    } catch (SQLException sqle) {
      throw new ConnectException(sqle);
    }
    return connection;
  }

  private void newConnection() throws SQLException {
    log.debug("Attempting to connect to {}", url);
    connection = DriverManager.getConnection(url, username, password);
    onConnect(connection);
    borrows = 1;
  }

  public synchronized void closeQuietly() {
    if (connection != null) {
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        log.warn("Ignoring connection rollback error", sqle);
      } finally {
        try {
          connection.close();
        } catch (SQLException sqle) {
          log.warn("Ignoring error closing connection", sqle);
        } finally {
          connection = null;
        }
      }
    }
  }

  protected void onConnect(Connection connection) throws SQLException {}

}
