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

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OzoneManager;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.ratis.protocol.ClientId;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.AddFinalizingMark;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.RemoveFinalizingMark;

/**
 * UpgradeFinalizer implementation for the Ozone Manager service.
 */
public class OMUpgradeFinalizer extends BasicUpgradeFinalizer<OzoneManager,
    OMLayoutVersionManager> {

  public OMUpgradeFinalizer(OMLayoutVersionManager versionManager) {
    super(versionManager);
  }

  @Override
  public void preFinalizeUpgrade(OzoneManager ozoneManager) {
    ClientId clientId = clientIdFromString(this.clientID);
    final OMRequest omRequest = OMRequest.newBuilder()
            .setCmdType(AddFinalizingMark)
            .setClientId(clientId)
            .build();
    try {
      LOG.info("Try to send request to add finalizing mark");
      OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest, clientId, 0);
      emitOMAddFinalizingMarkSuccess();
    } catch (Throwable e) {
      LOG.error("Add finalizing mark request failed.", e);
      emitOMAddFinalizingMarkError();
    }
  }

  @Override
  public void finalizeLayoutFeature(LayoutFeature layoutFeature,
                                    OzoneManager om) throws UpgradeException {
    try {
      logAndEmit(om.getFinalizationManager().getFinalizationStateManager()
          .finalizeLayoutFeature(layoutFeature.layoutVersion(), this.clientID));
    } catch (IOException ex) {
      throw new UpgradeException(ex,
          UpgradeException.ResultCodes.LAYOUT_FEATURE_FINALIZATION_FAILED);
    }
  }


  void replicatedFinalizationSteps(OMLayoutFeature layoutFeature, OzoneManager om) throws UpgradeException {
    super.finalizeLayoutFeature(layoutFeature,
        layoutFeature.action(LayoutFeature.UpgradeActionType.ON_FINALIZE),
        om.getOmStorage());
  }

  @Override
  public void postFinalizeUpgrade(OzoneManager ozoneManager) {
    ClientId clientId = clientIdFromString(this.clientID);
    final OMRequest omRequest = OMRequest.newBuilder()
            .setCmdType(RemoveFinalizingMark)
            .setClientId(clientId)
            .build();
    try {
      LOG.info("Try to send request to remove finalizing mark");
      OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest, clientId, 0);
      emitRemoveFinalizingMarkSuccess();
    } catch (Throwable e) {
      LOG.error("Remove finalizing mark request failed.", e);
      emitRemoveFinalizingMarkError();
    }
  }

  public void runPrefinalizeStateActions(Storage storage, OzoneManager om)
      throws IOException {
    super.runPrefinalizeStateActions(
        lf -> ((OMLayoutFeature) lf)::action, storage, om);
  }


  private ClientId clientIdFromString(String strId) {
      return ClientId.valueOf(strId);
  }

    private void emitOMAddFinalizingMarkSuccess() {
        String msg = "Successfully send request to add finalizing mark";
        logAndEmit(msg);
    }

    private void emitOMAddFinalizingMarkError() {
      String msg = "Add finalizing mark request failed.";
      this.logAndEmit(msg);
    }

    private void emitRemoveFinalizingMarkSuccess() {
      String msg = "Successfully send request to remove finalizing mark";
      logAndEmit(msg);
    }

    private void emitRemoveFinalizingMarkError() {
      String msg = "Remove finalizing mark request failed.";
      logAndEmit(msg);
    }
}
