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

        } else {
            checkTriggerIsNonConflicting(trigger);
            // it's now safe to cast
            CronTrigger cronTrigger = (CronTrigger) trigger;
            job = quartzScheduler.getJobDetail(cronTrigger.getJobKey());;

            if (isTriggerHasExpectedCronExpressionAndJobClass(job, cronTrigger, getCron())) {
                LOG.debug("Trigger with key {} is already present in scheduler. Lets assume nothing needs to be updated.", cronTrigger.getKey());

                LOG.debug("Job: {}, JobData: {}, Trigger: {}", new Object[] {job, job.getJobDataMap(), cronTrigger});
                // job = quartzScheduler.getJobDetail(cronTrigger.getJobKey());
                //
                // QuartzHelper.updateJobDataMap(getCamelContext(), job, null);
                //
                // // update the job in the scheduler. Note the replace=true argument
                // quartzScheduler.addJob(job, true);
                //
                // LOG.info("Re-scheduling job: {} with trigger: {}", job, trigger.getKey());
                // quartzScheduler.rescheduleJob(triggerKey, cronTrigger);
            } else {
                LOG.info(
                    "Cron expression {} in existing trigger with key {} deviates from the configured expression {}. Will replace the trigger with the configured expression",
                         new Object[] {cronTrigger.getCronExpression(), cronTrigger.getKey(), this.cron});

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

        }
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

    private void checkTriggerIsNonConflicting(Trigger trigger) {
        JobDataMap jobDataMap = trigger.getJobDataMap();
        String routeIdFromTrigger = jobDataMap.getString("routeId");
        if (routeIdFromTrigger != null && !routeIdFromTrigger.equals(routeId)) {
            throw new IllegalArgumentException("Trigger key " + trigger.getKey() + " is already used by route" + routeIdFromTrigger + ". Can't re-use it for route " + routeId);
        }

        if (!(trigger instanceof CronTrigger)) {
            throw new IllegalArgumentException("Trigger with key " + trigger.getKey()
                + " exists already, but is not a Cron Trigger. Giving up on incompatible trigger types: " + trigger.getClass().getName());
        }
        // TODO any more sanity checks we can do?
    }

    private boolean isTriggerHasExpectedCronExpressionAndJobClass(JobDetail job, CronTrigger trigger, String expectedCronExpression) {
        JobDataMap jobData = trigger.getJobDataMap();
        LOG.debug("jobdata map keys & values for trigger {}: {} {}", new Object[] {trigger.getKey(), jobData.getWrappedMap().keySet(), jobData.getWrappedMap().values()});

        boolean result = expectedCronExpression.trim().equals(trigger.getCronExpression());
        result = result && QuartzScheduledPollConsumerJob.class.equals(job.getJobClass());
        return result;
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
    }

}
