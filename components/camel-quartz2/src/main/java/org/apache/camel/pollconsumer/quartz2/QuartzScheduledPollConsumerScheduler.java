/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.pollconsumer.quartz2;

import java.util.TimeZone;

import org.apache.camel.CamelContext;
import org.apache.camel.Consumer;
import org.apache.camel.Route;
import org.apache.camel.component.quartz2.QuartzComponent;
import org.apache.camel.component.quartz2.QuartzConstants;
import org.apache.camel.component.quartz2.QuartzHelper;
import org.apache.camel.spi.ScheduledPollConsumerScheduler;
import org.apache.camel.support.ServiceSupport;
import org.apache.camel.util.ObjectHelper;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A quartz based {@link ScheduledPollConsumerScheduler} which uses a {@link CronTrigger} to define when the
 * poll should be triggered.
 */
public class QuartzScheduledPollConsumerScheduler extends ServiceSupport implements ScheduledPollConsumerScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(QuartzScheduledPollConsumerScheduler.class);
    private Scheduler quartzScheduler;
    private CamelContext camelContext;
    private String routeId;
    private Consumer consumer;
    private Runnable runnable;
    private String cron;
    private String triggerId;
    private String triggerGroup = "QuartzScheduledPollConsumerScheduler";
    private TimeZone timeZone = TimeZone.getDefault();
    private volatile CronTrigger trigger;
    private volatile JobDetail job;

    @Override
    public void onInit(Consumer consumer) {
        this.consumer = consumer;
        // find the route of the consumer
        for (Route route : consumer.getEndpoint().getCamelContext().getRoutes()) {
            if (route.getConsumer() == consumer) {
                this.routeId = route.getId();
                break;
            }
        }
    }

    @Override
    public void scheduleTask(Runnable runnable) {
        this.runnable = runnable;
    }

    @Override
    public void unscheduleTask() {
        if (trigger != null) {
            LOG.debug("Unscheduling trigger: {}", trigger.getKey());
            try {
                quartzScheduler.unscheduleJob(trigger.getKey());
            } catch (SchedulerException e) {
                throw ObjectHelper.wrapRuntimeCamelException(e);
            }
        }
    }

    @Override
    public void startScheduler() {
        // the quartz component starts the scheduler
    }

    @Override
    public boolean isSchedulerStarted() {
        try {
            return quartzScheduler != null && quartzScheduler.isStarted();
        } catch (SchedulerException e) {
            return false;
        }
    }

    @Override
    public void setCamelContext(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    @Override
    public CamelContext getCamelContext() {
        return camelContext;
    }

    public Scheduler getQuartzScheduler() {
        return quartzScheduler;
    }

    public void setQuartzScheduler(Scheduler scheduler) {
        this.quartzScheduler = scheduler;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public String getTriggerId() {
        return triggerId;
    }

    public void setTriggerId(String triggerId) {
        this.triggerId = triggerId;
    }

    public String getTriggerGroup() {
        return triggerGroup;
    }

    public void setTriggerGroup(String triggerGroup) {
        this.triggerGroup = triggerGroup;
    }

    @Override
    protected void doStart() throws Exception {
        ObjectHelper.notEmpty(cron, "cron", this);

        if (quartzScheduler == null) {
            // get the scheduler form the quartz component
            QuartzComponent quartz = getCamelContext().getComponent("quartz2", QuartzComponent.class);
            setQuartzScheduler(quartz.getScheduler());
        }

        String id = triggerId;
        if (id == null) {
            id = "trigger-" + getCamelContext().getUuidGenerator().generateUuid();
        }
        TriggerKey triggerKey = new TriggerKey(id, triggerGroup);

        // Add or use existing trigger to/from scheduler
        Trigger trigger = quartzScheduler.getTrigger(triggerKey);

        if (trigger == null) {
            createAndScheduleTrigger(triggerKey);
            return;
        }

        // trigger exists already; make some sanity checks and update it if
        // needed
        this.job = retrieveAndvalidateJobOfTrigger(trigger);
        // it's now safe to cast
        CronTrigger cronTrigger = (CronTrigger) trigger;
        
        if (isTriggerHasExpectedCronExpression(cronTrigger)) {
            LOG.debug("Trigger with key {} is already present in scheduler. Lets assume nothing needs to be updated. "
                + "Job: {}, JobData: {}, Trigger: {}", new Object []{cronTrigger.getKey(), job, job.getJobDataMap(), cronTrigger});

        } else {
            LOG.info("Cron expression {} in existing trigger with key {} deviates from the configured expression {}. "
                + "Will replace the trigger with the configured expression", new Object[] {cronTrigger.getCronExpression(), cronTrigger.getKey(), this.cron});
            updateAndRescheduleTrigger(triggerKey, trigger, cronTrigger);
        }
    }



    private void updateAndRescheduleTrigger(TriggerKey triggerKey, Trigger trigger, CronTrigger cronTrigger) throws SchedulerException {
        JobDataMap jobData = job.getJobDataMap();
        jobData.put(QuartzConstants.QUARTZ_TRIGGER_CRON_EXPRESSION, getCron());
        jobData.put(QuartzConstants.QUARTZ_TRIGGER_CRON_TIMEZONE, getTimeZone().getID());

        // store additional information on job such as camel context etc
        QuartzHelper.updateJobDataMap(getCamelContext(), job, null);

        // update the job in the scheduler. Note the replace=true argument
        quartzScheduler.addJob(job, true, true);

        CronScheduleBuilder cronSchedule = CronScheduleBuilder.cronSchedule(getCron()).inTimeZone(getTimeZone());
        Trigger newTrigger = cronTrigger.getTriggerBuilder().withSchedule(cronSchedule).build();

        LOG.info("Re-scheduling job: {} with trigger: {}", job, trigger.getKey());
        quartzScheduler.rescheduleJob(triggerKey, newTrigger);
    }

    private void createAndScheduleTrigger(TriggerKey triggerKey) throws SchedulerException {
        JobDataMap map = new JobDataMap();
        // do not store task as its not serializable, if we have route id
        if (routeId != null) {
            map.put("routeId", routeId);
        } else {
            LOG.warn("No routeId is available to reference in the Quartz job; resorting to store the Runnable directly. Note that this renders the Quartz job non-serializable!");
            map.put("task", runnable);
        }
        map.put(QuartzConstants.QUARTZ_TRIGGER_TYPE, "cron");
        map.put(QuartzConstants.QUARTZ_TRIGGER_CRON_EXPRESSION, getCron());
        map.put(QuartzConstants.QUARTZ_TRIGGER_CRON_TIMEZONE, getTimeZone().getID());

        job = JobBuilder.newJob(QuartzScheduledPollConsumerJob.class)
                .usingJobData(map)
                .build();

        // store additional information on job such as camel context etc
        QuartzHelper.updateJobDataMap(getCamelContext(), job, null);

        CronScheduleBuilder cronSchedule = CronScheduleBuilder.cronSchedule(getCron()).inTimeZone(getTimeZone());
        trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).withSchedule(cronSchedule).build();

        LOG.info("Scheduling job: {} with trigger: {}", job, trigger.getKey());
        quartzScheduler.scheduleJob(job, trigger);
    }

    /**
     * Retrieves the job belonging to the passed trigger and validates that both
     * are compatible with the settings found in our configuration.
     * <p>
     * If {@link JobDetail} or {@link Trigger} are found to be incompatible with
     * the configuration, an {@link IllegalStateException} is thrown. We want to
     * avoid doing any damage to those, so abort.
     * <p>
     * 
     * @param trigger The existing Trigger
     * @return the {@link JobDetail} belonging to the passed Trigger. If a
     *         JobDetail is returned, it is guaranteed that the passed trigger
     *         is a {@link CronTrigger}.
     * @throws SchedulerException if job cannot be retrieved from Scheduler
     */
    private JobDetail retrieveAndvalidateJobOfTrigger(Trigger trigger) throws SchedulerException {
        if (!(trigger instanceof CronTrigger)) {
            throw new IllegalStateException("Trigger with key " + trigger.getKey() + " exists already, but is not a Cron Trigger. Giving up on incompatible trigger types: "
                                            + trigger.getClass().getName());
        }

        JobDetail jobOfTrigger = quartzScheduler.getJobDetail(trigger.getJobKey());
        JobDataMap jobDataMap = jobOfTrigger.getJobDataMap();
        String routeIdFromTrigger = jobDataMap.getString("routeId");

        if (routeIdFromTrigger != null && !routeIdFromTrigger.equals(routeId)) {
            throw new IllegalStateException("Trigger key " + trigger.getKey() + " is already used by route" + routeIdFromTrigger + ". Can't re-use it for route " + routeId);
        }

        if (!QuartzScheduledPollConsumerJob.class.equals(jobOfTrigger.getJobClass())) {
            throw new IllegalStateException("Job " + jobOfTrigger + " of trigger with key " + trigger.getKey() + " runs unexpected Job class "
                                            + jobOfTrigger.getJobClass().getName() + ", not the expected " + QuartzScheduledPollConsumerJob.class.getName());
        }

        // TODO any more sanity checks we can do?

        return jobOfTrigger;
    }

    /**
     * Checks whether the existing trigger needs to be updated. It is compared
     * against the configured Cron pattern.
     * 
     * @param trigger The existing Trigger
     * @return {@code true} if the existing trigger reflects the current
     *         configuration. Otherwise {@code false}.
     */
    private boolean isTriggerHasExpectedCronExpression(CronTrigger trigger) {
        String expectedCronExpression = getCron();
        if (expectedCronExpression == null || expectedCronExpression.trim().isEmpty()) {
            return false;
        }

        return expectedCronExpression.trim().equals(trigger.getCronExpression());

    }

    @Override
    protected void doStop() throws Exception {
        if (trigger != null) {
            LOG.debug("Unscheduling trigger: {}", trigger.getKey());
            quartzScheduler.unscheduleJob(trigger.getKey());
        }
    }

    @Override
    protected void doShutdown() throws Exception {
        // do nothing
    }

}
