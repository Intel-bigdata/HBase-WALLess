/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.handler;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.assignment.AssignReplicaAsPrimaryRegionProcedure;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

@InterfaceAudience.Private
public class ConvertReplicaToPrimaryRegionHandler extends OpenPriorityRegionHandler {

  private static final Log LOG = LogFactory.getLog(ConvertReplicaToPrimaryRegionHandler.class);
  private final HRegionInfo replicaRegion;

  public ConvertReplicaToPrimaryRegionHandler(Server server, RegionServerServices rsServices,
      HRegionInfo regionInfo, HRegionInfo replicaRegion, HTableDescriptor htd,
      long masterSystemTime) {
    super(server, rsServices, regionInfo, htd, masterSystemTime);
    this.replicaRegion = replicaRegion;
    LOG.info("Converting replica region "+replicaRegion+ " to primary region "+regionInfo);
  }

  protected HRegion openRegion() {
    HRegion region = (HRegion) this.rsServices
        .getFromOnlineRegions(this.replicaRegion.getEncodedName());
    LOG.info("Converting region "+region.getRegionInfo() +  "  to primary region");
    region.convertAsPrimaryRegion();
    return region;
  }

  @Override
  protected PostOpenDeployTasksThread createPostOpenDeployTasksThread(HRegion r,
      long masterSystemTime, AtomicBoolean signaller) {
    // TODO Auto-generated method stub
    return super.createPostOpenDeployTasksThread(r, masterSystemTime, signaller);
  }

  @Override
  protected void addToOnlineRegions(HRegion region) {
    // First remove the replica region from online regions.
    // TODO we are not following the regular way of close of a region where 1st it is in RIT
    // This typecasting is ok as in Trunk we will do many changes and here in handler we will have
    // HRS
    ((HRegionServer) this.rsServices).removeFromOnlineRegions(this.replicaRegion);
    this.rsServices.addToOnlineRegions(region);
  }
}
