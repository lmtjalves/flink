package org.apache.flink.runtime.jobmanager.scheduler;


import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.CompletableFuture;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceListener;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.instance.InstanceDiedException;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Queue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;


public class OptimisticScheduler implements InstanceListener, SlotAvailabilityListener, SlotProvider {

	/** Scheduler-wide logger */
	private static final Logger LOG = LoggerFactory.getLogger(OptimisticScheduler.class);


	/** All modifications to the scheduler structures are performed under a global scheduler lock */
	private final Object globalLock = new Object();

	/** All instances that the scheduler can deploy to */
	private final HashMap<ResourceID, Instance> allInstances = new HashMap<>();

	/** All tasks pending to be scheduled */
	private final Queue<QueuedTask> taskQueue = new ArrayDeque<>();

	// ------------------------------------------------------------------------

	/**
	 * Shuts the scheduler down. After shut down no more tasks can be added to the scheduler.
	 */
	public void shutdown() {
		synchronized (globalLock) {
			for (Instance i : allInstances.values()) {
				i.removeSlotListener();
				i.cancelAndReleaseAllSlots();
			}
			allInstances.clear();
			taskQueue.clear();
		}
	}

	// ------------------------------------------------------------------------
	//  Scheduling
	// ------------------------------------------------------------------------


	@Override
	public Future<SimpleSlot> allocateSlot(ScheduledUnit task, boolean allowQueued)
		throws NoResourceAvailableException {

		final Object ret = scheduleTask(task, allowQueued);
		if (ret instanceof SimpleSlot) {
			return FlinkCompletableFuture.completed((SimpleSlot) ret);
		}
		else if (ret instanceof Future) {
			return (Future) ret;
		}
		else {
			throw new RuntimeException();
		}
	}

	/**
	 * Returns either a {@link SimpleSlot}, or a {@link Future}.
	 */
	private Object scheduleTask(ScheduledUnit task, boolean queueIfNoResource) throws NoResourceAvailableException {
		if (task == null) {
			throw new NullPointerException();
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Scheduling task " + task);
		}

		final ExecutionVertex vertex = task.getTaskToExecute().getVertex();

		final Iterable<TaskManagerLocation> preferredLocations = vertex.getPreferredLocations();
		final boolean forceExternalLocation = vertex.isScheduleLocalOnly() &&
			preferredLocations != null && preferredLocations.iterator().hasNext();

		synchronized (globalLock) {

			SimpleSlot slot = getFreeSlotForTask(vertex, preferredLocations, forceExternalLocation);
			if (slot != null) {
				return slot;
			}
			else {
				// no resource available now, so queue the request
				if (queueIfNoResource) {
					CompletableFuture<SimpleSlot> future = new FlinkCompletableFuture<>();
					this.taskQueue.add(new OptimisticScheduler.QueuedTask(task, future));
					return future;
				}
				else if (forceExternalLocation) {
					String hosts = getHostnamesFromInstances(preferredLocations);
					throw new NoResourceAvailableException("Could not schedule task " + vertex
						+ " to any of the required hosts: " + hosts);
				}
				else {
					throw new NoResourceAvailableException("There are " + getNumberOfAvailableInstances()
						+ " instances available");
				}
			}
		}
	}

	/**
	 * Gets a suitable instance to schedule the vertex execution to.
	 * <p>
	 * NOTE: This method does is not thread-safe, it needs to be synchronized by the caller.
	 *
	 * @param vertex The task to run.
	 * @return The instance to run the vertex on, it {@code null}, if no instance is available.
	 */
	private SimpleSlot getFreeSlotForTask(ExecutionVertex vertex,
											Iterable<TaskManagerLocation> requestedLocations,
											boolean localOnly) {
		// we need potentially to loop multiple times, because there may be false positives
		// in the set-with-available-instances
		while (true) {
			Pair<Instance, Locality> instanceLocalityPair = findInstance(requestedLocations, localOnly);

			if (instanceLocalityPair == null){
				return null;
			}

			Instance instanceToUse = instanceLocalityPair.getLeft();
			Locality locality = instanceLocalityPair.getRight();

			try {
				SimpleSlot slot = instanceToUse.allocateSimpleSlot(vertex.getJobId());

				if (slot != null) {
					slot.setLocality(locality);

					LOG.info("SCHEDULED;" + vertex.getIdentifier() + ";" + instanceToUse.getId());
					return slot;
				}
			}
			catch (InstanceDiedException e) {
				// the instance died it has not yet been propagated to this scheduler
				// remove the instance from the set of available instances
				removeInstance(instanceToUse);
			}

			// if we failed to get a slot, fall through the loop
		}
	}

	/**
	 * Tries to find a requested instance. If no such instance is available it will return a non-
	 * local instance. If no such instance exists (all slots occupied), then return null.
	 *
	 * <p><b>NOTE:</b> This method is not thread-safe, it needs to be synchronized by the caller.</p>
	 *
	 * @param requestedLocations The list of preferred instances. May be null or empty, which indicates that
	 *                           no locality preference exists.
	 * @param localOnly Flag to indicate whether only one of the exact local instances can be chosen.
	 */
	private Pair<Instance, Locality> findInstance(Iterable<TaskManagerLocation> requestedLocations, boolean localOnly) {

		Iterator<TaskManagerLocation> locations = requestedLocations == null ? null : requestedLocations.iterator();

		if (locations != null && locations.hasNext()) {
			ArrayList<Instance> instances = new ArrayList<>();
			Locality locality = Locality.LOCAL;

			// we have a locality preference
			while (locations.hasNext()) {
				TaskManagerLocation location = locations.next();
				if (location != null) {
					instances.add(allInstances.get(location.getResourceID()));
				}
			}

			if(instances.isEmpty()) {
				// no local instance available
				if (localOnly) {
					return null;
				}
				else {
					locality = Locality.NON_LOCAL;
					instances.addAll(allInstances.values());
				}
			}

			Instance instanceToUse = Collections.min(instances, new Comparator<Instance>() {
				@Override
				public int compare(Instance i1, Instance i2) {
				return i1.getCpuLoad() - i2.getCpuLoad();
				}
			});

			return new ImmutablePair<>(instanceToUse, locality);
		} else {
			// no location preference, so use some instance
			Instance instanceToUse = Collections.min(allInstances.values(), new Comparator<Instance>() {
				@Override
				public int compare(Instance i1, Instance i2) {
					return i1.getCpuLoad() - i2.getCpuLoad();
				}
			});

			return new ImmutablePair<>(instanceToUse, Locality.UNCONSTRAINED);
		}
	}

	@Override
	public void newSlotAvailable(final Instance instance) {
		// Nothing to do, we don't care about slots
	}

	// --------------------------------------------------------------------------------------------
	//  Instance Availability
	// --------------------------------------------------------------------------------------------

	@Override
	public void newInstanceAvailable(Instance instance) {
		if (instance == null) {
			throw new IllegalArgumentException();
		}
		if (instance.getNumberOfAvailableSlots() <= 0) {
			throw new IllegalArgumentException("The given instance has no resources.");
		}
		if (!instance.isAlive()) {
			throw new IllegalArgumentException("The instance is not alive.");
		}

		// synchronize globally for instance changes
		synchronized (this.globalLock) {
			// check we do not already use this instance
			if (this.allInstances.put(instance.getTaskManagerID(), instance) != null) {
				throw new IllegalArgumentException("The instance is already contained.");
			}
		}
	}

	@Override
	public void instanceDied(Instance instance) {
		if (instance == null) {
			throw new IllegalArgumentException();
		}

		instance.markDead();

		// we only remove the instance from the pools, we do not care about the
		synchronized (this.globalLock) {
			// the instance must not be available anywhere any more
			removeInstance(instance);
		}
	}

	private void removeInstance(Instance instance) {
		if (instance == null) {
			throw new NullPointerException();
		}

		allInstances.remove(instance.getTaskManagerID());
	}

	// --------------------------------------------------------------------------------------------
	//  Status reporting
	// --------------------------------------------------------------------------------------------

	public int getNumberOfAvailableInstances() {
		int numberAvailableInstances = 0;
		synchronized (this.globalLock) {
			for (Instance instance: allInstances.values()){
				if (instance.isAlive()){
					numberAvailableInstances++;
				}
			}
		}

		return numberAvailableInstances;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static String getHostnamesFromInstances(Iterable<TaskManagerLocation> locations) {
		StringBuilder bld = new StringBuilder();

		boolean successive = false;
		for (TaskManagerLocation loc : locations) {
			if (successive) {
				bld.append(", ");
			} else {
				successive = true;
			}
			bld.append(loc.getHostname());
		}

		return bld.toString();
	}

	// ------------------------------------------------------------------------
	//  Nested members
	// ------------------------------------------------------------------------

	/**
	 * An entry in the queue of schedule requests. Contains the task to be scheduled and
	 * the future that tracks the completion.
	 */
	private static final class QueuedTask {

		private final ScheduledUnit task;

		private final CompletableFuture<SimpleSlot> future;


		public QueuedTask(ScheduledUnit task, CompletableFuture<SimpleSlot> future) {
			this.task = task;
			this.future = future;
		}

		public ScheduledUnit getTask() {
			return task;
		}

		public CompletableFuture<SimpleSlot> getFuture() {
			return future;
		}
	}
}
