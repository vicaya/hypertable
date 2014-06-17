/**
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

package org.hypertable.examples.PerformanceTest;

import java.io.IOException;
import java.util.logging.Logger;

public abstract class Driver {

  static final Logger log = Logger.getLogger("org.hypertable.examples.PerformanceTest");

  public abstract void setup(String tableName, Task.Type testType, int parallelism);

  public abstract void teardown();

  public abstract void runTask(Task task) throws IOException;

  public Result getResult() { return mResult; }

  protected Result mResult;
  protected DriverCommon mCommon = new DriverCommon();
}
