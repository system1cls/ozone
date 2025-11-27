/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.upgrade;

import java.io.IOException;

import org.apache.hadoop.hdds.utils.db.Table;

public interface FinalizationStateManager {
  void addFinalizingMark() throws IOException;

  void removeFinalizingMark() throws IOException;

  String finalizeLayoutFeature(Integer layoutVersion, String clientId)
      throws IOException;

  void finalizeLayoutFeatureLocal(Integer layoutVersion)
          throws IOException;

  boolean isHasFinalizingMark();

  void reinitialize(Table<String, String> newFinalizationStore)
      throws IOException;
}

