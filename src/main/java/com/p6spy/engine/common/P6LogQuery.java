/**
 * P6Spy
 *
 * Copyright (C) 2002 - 2019 P6Spy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.p6spy.engine.common;

import com.p6spy.engine.logging.Category;
import com.p6spy.engine.logging.P6LogLoadableOptions;
import com.p6spy.engine.logging.P6LogOptions;
import com.p6spy.engine.spy.P6ModuleManager;
import com.p6spy.engine.spy.P6SpyOptions;
import com.p6spy.engine.spy.appender.FileLogger;
import com.p6spy.engine.spy.appender.FormattedLogger;
import com.p6spy.engine.spy.appender.MessageFormattingStrategy;
import com.p6spy.engine.spy.appender.P6Logger;
import com.p6spy.engine.spy.option.P6OptionChangedListener;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class P6LogQuery implements P6OptionChangedListener {
  
  private static final Set<Category> CATEGORIES_IMPLICITLY_INCLUDED = new HashSet<Category>(
	      Arrays.asList(Category.ERROR, Category.OUTAGE /* we still want to have outage category enabled! */));
  /**
   * Options that cause re-init of {@link P6LogQuery}.
   */
  private static final Set<String> ON_CHANGE = new HashSet<String>(Arrays.asList(
    P6SpyOptions.APPENDER_INSTANCE, P6SpyOptions.LOGFILE, P6SpyOptions.LOG_MESSAGE_FORMAT_INSTANCE));

  protected static P6Logger logger;

  static {
    initialize();
  }

  @Override
  public void optionChanged(final String key, final Object oldValue, final Object newValue) {
    if (ON_CHANGE.contains(key)) {
      initialize();
    }
  }

  public static synchronized void initialize() {
    final P6ModuleManager moduleManager = P6ModuleManager.getInstance();
    if (null == moduleManager) {
      // not initialized yet => can't proceed
      return;
    }
    
    final P6SpyOptions opts = moduleManager.getOptions(P6SpyOptions.class);
    logger = opts.getAppenderInstance();
    if (logger != null) {
      if (logger instanceof FileLogger) {
        final String logfile = opts.getLogfile();
        ((FileLogger) logger).setLogfile(logfile);
      }
      if (logger instanceof FormattedLogger) {
        final MessageFormattingStrategy strategy = opts.getLogMessageFormatInstance();
        if (strategy != null) {
          ((FormattedLogger) logger).setStrategy(strategy);
        }
      }
    }
  }

  protected static void doLog(long elapsed, Category category, String prepared, String sql) {
    doLog(-1, elapsed, category, prepared, sql, "");
  }

  // this is an internal method called by logElapsed
  protected static void doLogElapsed(int connectionId, long timeElapsedNanos, Category category, String prepared, String sql, String url) {
    doLog(connectionId, timeElapsedNanos, category, prepared, sql, url);
  }

	/**
	 * Writes log information provided.
	 * 
	 * @param connectionId
	 * @param elapsedNanos
	 * @param category
	 * @param prepared
	 * @param sql
   * @param url
	 */
	protected static void doLog(int connectionId, long elapsedNanos, Category category, String prepared, String sql, String url) {
	    // give it one more try if not initialized yet
	    if (logger == null) {
	      initialize();
	      if (logger == null) {
	    	  return;
	      }
	    }
    
      final String format = P6SpyOptions.getActiveInstance().getDateformat();
      final String stringNow;
      if (format == null) {
        stringNow = Long.toString(System.currentTimeMillis());
      } else {
        stringNow = new SimpleDateFormat(format).format(new java.util.Date()).trim();
      }

      logger.logSQL(connectionId, stringNow, TimeUnit.NANOSECONDS.toMillis(elapsedNanos), category, prepared, sql, url);

      final boolean stackTrace = P6SpyOptions.getActiveInstance().getStackTrace();
      if (stackTrace) {
	    final String stackTraceClass = P6SpyOptions.getActiveInstance().getStackTraceClass();
    	Exception e = new Exception();
        if (stackTraceClass != null) {
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          e.printStackTrace(pw);
          String stack = sw.toString();
          if (!stack.contains(stackTraceClass)) {
            e = null;
          }
        }
        if (e != null) {
          logger.logException(e);
        }
      }
  }

  static boolean isLoggable(String sql) {
    // empty batches and connection commits/rollbacks
    if (sql == null || sql.isEmpty()) {
      return true;
    }

    final P6LogLoadableOptions opts = P6LogOptions.getActiveInstance();
    
    if (!opts.getFilter()) {
      return true;
    }

    final Pattern sqlExpressionPattern = opts.getSQLExpressionPattern();
    final Pattern includeExcludePattern = opts.getIncludeExcludePattern();
    
    return (sqlExpressionPattern == null || sqlExpressionPattern.matcher(sql).matches())
        && (includeExcludePattern == null || includeExcludePattern.matcher(sql).matches());
  }

  static boolean isCategoryOk(Category category) {
    final P6LogLoadableOptions opts = P6LogOptions.getActiveInstance();
    if (null == opts) {
      return CATEGORIES_IMPLICITLY_INCLUDED.contains(category);
    }
    
    final Set<Category> excludeCategories = opts.getExcludeCategoriesSet();
    
    return logger != null && logger.isCategoryEnabled(category) 
    	&& (excludeCategories == null || !excludeCategories.contains(category));
  }
  
  // ----------------------------------------------------------------------------------------------------------
  // public accessor methods for logging and viewing query data
  // ----------------------------------------------------------------------------------------------------------

  public static void log(Category category, String prepared, String sql) {
    if (logger != null && isCategoryOk(category)) {
      doLog(-1, category, prepared, sql);
    }
  }

  public static void log(Category category, Loggable loggable) {
    if (logger != null && isCategoryOk(category) && isLoggable(loggable.getSql())) {
      doLog(-1, category, loggable.getSql(), loggable.getSqlWithValues());
    }
  }

  public static void logElapsed(int connectionId, long timeElapsedNanos, Category category, String prepared, String sql, String url) {
    if (logger != null && meetsThresholdRequirement(timeElapsedNanos) && isCategoryOk(category) && isLoggable(sql) ) {
      doLogElapsed(connectionId, timeElapsedNanos, category, prepared, sql, url == null ? "" : url);
    } else if (isDebugEnabled()) {
      debug("P6Spy intentionally did not log category: " + category + ", statement: " + sql + "  Reason: logger=" + logger + ", isLoggable="
          + isLoggable(sql) + ", isCategoryOk=" + isCategoryOk(category) + ", meetsTreshold=" + meetsThresholdRequirement(timeElapsedNanos));
    }
  }
  
  public static void logElapsed(int connectionId, long timeElapsedNanos, Category category, Loggable loggable) {
    // usually an expensive operation => cache where possible
    String sql = loggable.getSql();
    String url = loggable.getConnectionInformation().getUrl();
    if (logger != null && meetsThresholdRequirement(timeElapsedNanos) && isCategoryOk(category) && isLoggable(sql)) {
      doLogElapsed(connectionId, timeElapsedNanos, category, sql, loggable.getSqlWithValues(), url == null ? "" : url);
    } else if (isDebugEnabled()) {
      sql = loggable.getSqlWithValues();
      debug("P6Spy intentionally did not log category: " + category + ", statement: " + sql + "  Reason: logger=" + logger + ", isLoggable="
          + isLoggable(sql) + ", isCategoryOk=" + isCategoryOk(category) + ", meetsTreshold=" + meetsThresholdRequirement(timeElapsedNanos));
    }
  }

  //->JAW: new method that checks to see if this statement should be logged based
  //on whether on not it has taken greater than x amount of time.
  private static boolean meetsThresholdRequirement(long timeTaken) {
        final P6LogLoadableOptions opts = P6LogOptions.getActiveInstance();
        long executionThreshold = null != opts ? opts.getExecutionThreshold() : 0;
    
    return executionThreshold <= 0 || TimeUnit.NANOSECONDS.toMillis(timeTaken) > executionThreshold;
  }

  public static void info(String sql) {
    if (logger != null && isCategoryOk(Category.INFO)) {
      doLog(-1, Category.INFO, "", sql);
    }
  }

  public static boolean isDebugEnabled() {
    return isCategoryOk(Category.DEBUG);
  }

  public static void debug(String sql) {
    if (isDebugEnabled()) {
      if (logger != null) {
        doLog(-1, Category.DEBUG, "", sql);
      } else {
        System.err.println(sql);
      }
    }
  }

  public static void error(String sql) {
    System.err.println("Warning: " + sql);
    if (logger != null) {
      doLog(-1, Category.ERROR, "", sql);
    }
  }
  
  public static P6Logger getLogger() {
    return logger;
  }

}
