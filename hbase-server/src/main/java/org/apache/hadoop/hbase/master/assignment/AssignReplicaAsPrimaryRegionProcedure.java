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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.assignment.RegionStates.RegionStateNode;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.ReplicaToPrimaryRegionConvertOperation;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AssignReplicaRegionAsPrimaryStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionTransitionState;
import org.apache.hadoop.hbase.util.Pair;

@InterfaceAudience.Private
public class AssignReplicaAsPrimaryRegionProcedure extends AssignProcedure {

  private static final Log LOG = LogFactory.getLog(AssignReplicaAsPrimaryRegionProcedure.class);
  private HRegionInfo destinationRegion;// The replica region which is going to be converted as the
                                        // primary. This in other terms that on the destination
                                        // server we are doing a partial kind of region open

  public AssignReplicaAsPrimaryRegionProcedure() {
  }

  public AssignReplicaAsPrimaryRegionProcedure(HRegionInfo regionInfo, ServerName destinationServer,
      HRegionInfo destinationRegion) {
    super(regionInfo, destinationServer);
    this.destinationRegion = destinationRegion;
    LOG.info("Creating assign replica procedure for the region " + regionInfo + " dest region "
        + destinationRegion + " " + destinationServer);
  }
  
  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    AssignReplicaRegionAsPrimaryStateData.Builder state = AssignReplicaRegionAsPrimaryStateData
        .newBuilder().setTransitionState(getTransitionState())
        .setRegionInfo(HRegionInfo.convert(getRegionInfo()))
        .setTargetServer(ProtobufUtil.toServerName(this.targetServer))
        .setDestinationRegion(HRegionInfo.convert(this.destinationRegion));
    state.build().writeDelimitedTo(stream);
  }
  
  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    AssignReplicaRegionAsPrimaryStateData state = AssignReplicaRegionAsPrimaryStateData
        .parseDelimitedFrom(stream);
    setTransitionState(state.getTransitionState());
    setRegionInfo(HRegionInfo.convert(state.getRegionInfo()));
    this.targetServer = ProtobufUtil.toServerName(state.getTargetServer());
    this.destinationRegion = HRegionInfo.convert(state.getDestinationRegion());
  }
  
  @Override
  protected void queueForBalance(RegionStateNode regionNode, MasterProcedureEnv env,
      boolean retain) {
    // This is basically a noop as we strictly use the destination server which we already know.
    // This is a special way of Region open where the replica of this region is already open in that
    // RS and we just have to go to that RS and mark this replica as a primary one now
    // The core assign flow will wait on the Procedure for this Region node. So just mark this ready
    // so that this wait will get passed
    // 'env.getProcedureScheduler().waitEvent(regionNode.getProcedureEvent(), this))'
    LOG.info("Start " + this + "; " + regionNode.toShortString() + "; forceNewPlan="
        + this.forceNewPlan + ", retain=" + retain);
    //env.getProcedureScheduler().markEventReady(regionNode.getProcedureEvent());
    env.getProcedureScheduler().wakeEvents(1, regionNode.getProcedureEvent());
  }

  @Override
  public RemoteOperation remoteCallBuild(final MasterProcedureEnv env,
      final ServerName serverName) {
    assert serverName.equals(getRegionState(env).getRegionLocation());
    return new ReplicaToPrimaryRegionConvertOperation(this, getRegionInfo(),
        env.getAssignmentManager().getFavoredNodes(getRegionInfo()), false, this.destinationRegion);
  }

  protected void handleFailure(final MasterProcedureEnv env, final RegionStateNode regionNode) {
    if (incrementAndCheckMaxAttempts(env, regionNode)) {
      aborted.set(true);
    }
    Pair<HRegionInfo, ServerName> pair = env.getAssignmentManager()
        .getNextReplicaRegion(getRegionInfo());
    this.destinationRegion = pair.getFirst();
    this.targetServer = pair.getSecond();
    regionNode.offline();
    // We were moved to OPENING state before dispatch. Undo. It is safe to call
    // this method because it checks for OPENING first.
    env.getAssignmentManager().undoRegionAsOpening(regionNode);
    setTransitionState(RegionTransitionState.REGION_TRANSITION_QUEUE);
  }

  @Override
  protected Procedure finishTransition(final MasterProcedureEnv env,
      final RegionStateNode regionNode) throws IOException {
    env.getAssignmentManager().markRegionAsOffline(this.destinationRegion);
    env.getAssignmentManager().markRegionAsOpened(regionNode);
    // This success may have been after we failed open a few times. Be sure to cleanup any
    // failed open references. See #incrementAndCheckMaxAttempts and where it is called.
    env.getAssignmentManager().getRegionStates().removeFromFailedOpen(regionNode.getRegionInfo());
    // TODO : Shall we add a new state for this so that on failure this assign alone is done once again
    // rather than other steps??
    return new AssignProcedure(this.destinationRegion, true);
  }

  @Override
  protected void postFinish(MasterProcedureEnv env, RegionStateNode regionNode) {
    // Only for this procedure we do this because we don't want the parent to be exeecuted once
    // again.
    this.transitionState = RegionTransitionState.PRIMARY_REGION_REPLICA_SWTICH_OVER;
  }
}
