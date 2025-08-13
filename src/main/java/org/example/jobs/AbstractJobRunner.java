package org.example.jobs;

import org.example.configuration.ConfigurationProperties;
import org.example.logger.Logger;
import org.example.logger.SystemLogger;

import java.time.*;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides common logic for scheduling and running file migration job runners.
 */
public abstract class AbstractJobRunner implements Runnable {
    protected int backoffCounter = 0;
    private final static ZoneId ET_ZONE = ZoneId.of("America/New_York");
    protected final static List<Integer> DEFAULT_BACKOFF_PERIODS = List.of(
            30,
            300,
            600,
            1800,
            3600
    );
    protected final Logger log = new SystemLogger();
    protected final ConfigurationProperties config;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean shutdown;

    public AbstractJobRunner(ConfigurationProperties config, ScheduledExecutorService scheduler, AtomicBoolean shutdown) {
        this.config = config;
        this.scheduler = scheduler;
        this.shutdown = shutdown;
    }

    @Override
    public void run() {
        if (this.shouldRun()) {
            this.process();
        }
    }

    protected boolean shouldRun() {
        if (this.shutdown.get()) {
            return false;
        }
        boolean enabled = this.config.getBoolean("ENABLE_JOB", false);
        if (enabled) {
            boolean afterHoursOnly = this.config.getBoolean("AFTER_HOURS", true);
            if (afterHoursOnly) {
                int startHour = this.config.getInteger("START_HOUR", 22);
                int startMinute = this.config.getInteger("START_MINUTE", 0);
                int endHour = this.config.getInteger("END_HOUR", 6);
                int endMinute = this.config.getInteger("END_MINUTE", 0);
                // only run after target time window during weekdays or on weekends
                ZonedDateTime nowEst = ZonedDateTime.now(ET_ZONE);
                LocalTime now = nowEst.toLocalTime();
                boolean isAfterStartTime = now.isAfter(LocalTime.of(startHour, startMinute, 0, 0));
                boolean isBeforeEndTime = now.isBefore(LocalTime.of(endHour, endMinute, 0, 0));

                DayOfWeek day = nowEst.getDayOfWeek();
                boolean isWeekend = (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY);
                boolean shouldRun = isWeekend || (isAfterStartTime && isBeforeEndTime);

                if (!shouldRun) {
                    // wait to try again until target time ET (i.e. 10PM ET)
                    ZonedDateTime targetTime = nowEst.withHour(startHour).withMinute(startMinute).withSecond(0).withNano(0);
                    Duration waitDuration = Duration.between(nowEst, targetTime);
                    long secondsToWait = waitDuration.getSeconds();
                    this.schedule(secondsToWait);
                }

                return shouldRun;
            }
            return true;
        }
        return false;
    }

    public void schedule() {
        long delay = this.getDefaultDelay();
        if (this.backoffCounter > 0) {
            List<Integer> backoffPeriods = this.getBackoffPeriods();
            // increment delay based on backoff periods if no records are available at this time to avoid wasting resources
            int index = Math.min(backoffCounter - 1, backoffPeriods.size() - 1);
            delay = backoffPeriods.get(index);
            this.log.info(String.format("%s job backoff triggered; delaying job for %d seconds", this.getName(), delay));
        }
        this.schedule(delay);
    }

    public void schedule(long delay) {
        if (!this.shutdown.get() && !this.scheduler.isShutdown() && !this.scheduler.isTerminated()) {
            this.scheduler.schedule(this, delay, TimeUnit.SECONDS);
        }
    }

    protected List<Integer> getBackoffPeriods() {
        return DEFAULT_BACKOFF_PERIODS;
    }

    protected abstract void process();
    protected abstract long getDefaultDelay();
    protected abstract String getName();
}