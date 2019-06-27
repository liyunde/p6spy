/**
 * P6Spy
 * <p>
 * Copyright (C) 2002 - 2019 P6Spy
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.p6spy.engine.spy.appender;

import com.p6spy.engine.logging.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Appender which delegates to SLF4J. All logger messages are logged at the INFO
 * level using the "p6spy" category, except debug and error ones that logger on the
 * respective SLF4J categories.
 */
public class Slf4JLogger extends FormattedLogger {

  private static final Logger logger = LoggerFactory.getLogger("p6spy");

  public Slf4JLogger() {
  }

  @Override
  public void logException(Exception e) {
    logger.error("", e);
  }

  @Override
  public void logText(String text) {
    logger.info(text);
  }

  @Override
  public void logSQL(int connectionId, String now, long elapsed,
                     Category category, String prepared, String sql, String url) {

    final String msg = strategy.formatMessage(connectionId, now, elapsed,
                                              category.toString(), prepared, sql, url);

    if (Category.ERROR.equals(category)) {
      logger.error(msg);
    } else if (Category.WARN.equals(category)) {
      logger.warn(msg);
    } else if (Category.DEBUG.equals(category)) {
      logger.debug(msg);
    } else {
      logger.info(msg);
    }
  }

  @Override
  public boolean isCategoryEnabled(Category category) {
    if (Category.ERROR.equals(category)) {
      return logger.isErrorEnabled();
    } else if (Category.WARN.equals(category)) {
      return logger.isWarnEnabled();
    } else if (Category.DEBUG.equals(category)) {
      return logger.isDebugEnabled();
    } else {
      return logger.isInfoEnabled();
    }
  }
}
