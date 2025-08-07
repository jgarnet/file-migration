package org.example.jobs;

import org.example.configuration.ConfigurationProperties;
import org.example.logger.Logger;
import org.example.logger.SystemLogger;

import java.time.*;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

    public AbstractJobRunner(ConfigurationProperties config, ScheduledExecutorService scheduler) {
        this.config = config;
        this.scheduler = scheduler;
    }

    @Override
    public void run() {
        if (this.shouldRun()) {
            this.process();
        }
    }

    protected boolean shouldRun() {
        boolean enabled = this.config.getBoolean("ENABLE_JOB", false);
        if (enabled) {
            boolean afterHoursOnly = this.config.getBoolean("AFTER_HOURS", true);
            if (afterHoursOnly) {
                int targetHour = this.config.getInteger("TARGET_HOUR", 22);
                int targetMinute = this.config.getInteger("TARGET_MINUTE", 0);
                // only run after 10PM weekdays or on weekends
                ZonedDateTime nowEst = ZonedDateTime.now(ET_ZONE);
                boolean isAfterTargetTime = nowEst.toLocalTime().isAfter(LocalTime.of(targetHour, targetMinute, 0, 0));

                DayOfWeek day = nowEst.getDayOfWeek();
                boolean isWeekend = (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY);
                boolean shouldRun = isWeekend || isAfterTargetTime;

                if (!shouldRun) {
                    // wait to try again until target time ET (i.e. 10PM ET)
                    ZonedDateTime targetTime = nowEst.withHour(targetHour).withMinute(targetMinute).withSecond(0).withNano(0);
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
        }
        this.schedule(delay);
    }

    public void schedule(long delay) {
        this.scheduler.schedule(this, delay, TimeUnit.SECONDS);
    }

    protected List<Integer> getBackoffPeriods() {
        return DEFAULT_BACKOFF_PERIODS;
    }

    protected abstract void process();
    protected abstract long getDefaultDelay();
}

