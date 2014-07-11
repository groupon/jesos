/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.groupon.mesos.util;

import static java.lang.String.format;

import static com.google.common.base.Preconditions.checkNotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tiny weeny wrapper around slf4j to alleviate the worst of the pain.
 */
public final class Log
{
    public static Log getLog(final String categoryName)
    {
        return new Log(categoryName);
    }

    public static Log getLog(final Class<?> clazz)
    {
        checkNotNull(clazz, "clazz is null");
        return new Log(clazz.getName());
    }

    private final Logger logger;

    private Log(final String categoryName)
    {
        this.logger = LoggerFactory.getLogger(categoryName);
    }

    public void debug(final String formatString, final Object ... values)
    {
        checkNotNull(formatString, "format is null");
        if (values.length == 0) {
            logger.debug(formatString);
        }
        else {
            logger.debug(format(formatString, values));
        }
    }

    public void info(final String formatString, final Object ... values)
    {
        checkNotNull(formatString, "format is null");
        if (values.length == 0) {
            logger.info(formatString);
        }
        else {
            logger.info(format(formatString, values));
        }
    }

    public void warn(final String formatString, final Object ... values)
    {
        checkNotNull(formatString, "format is null");
        if (values.length == 0) {
            logger.warn(formatString);
        }
        else {
            logger.warn(format(formatString, values));
        }
    }

    public void warn(final Throwable t, final String formatString, final Object ... values)
    {
        checkNotNull(formatString, "format is null");
        if (values.length == 0) {
            logger.warn(formatString, t);
        }
        else {
            logger.warn(format(formatString, values), t);
        }
    }

    public void error(final String formatString, final Object ... values)
    {
        checkNotNull(formatString, "format is null");
        if (values.length == 0) {
            logger.error(formatString);
        }
        else {
            logger.error(format(formatString, values));
        }
    }

    public void error(final Throwable t, final String formatString, final Object ... values)
    {
        checkNotNull(formatString, "format is null");
        if (values.length == 0) {
            logger.error(formatString, t);
        }
        else {
            logger.error(format(formatString, values), t);
        }
    }
}
