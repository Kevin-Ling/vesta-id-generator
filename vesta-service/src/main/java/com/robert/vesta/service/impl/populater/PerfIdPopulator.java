package com.robert.vesta.service.impl.populater;

import com.robert.vesta.service.bean.Id;
import com.robert.vesta.service.impl.bean.IdMeta;
import com.robert.vesta.service.impl.bean.IdType;
import com.robert.vesta.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReference;

public class PerfIdPopulator implements IdPopulator {

    private static final Logger log = LoggerFactory.getLogger(PerfIdPopulator.class);

    private volatile AtomicReference<IdElement> referenceIdElement;

    private static long PEAK_SEQUENCE = 1000000L;

    private final Timer timer;

    private class IdElement {
        private final long sequence;
        private final long timeStamp;

        public IdElement(long sequence, long timeStamp) {
            this.sequence = sequence;
            this.timeStamp = timeStamp;
        }

        public long getSequence() {
            return sequence;
        }

        public long getTimeStamp() {
            return timeStamp;
        }

        public boolean isOutOfSeq() {
            return sequence >= PEAK_SEQUENCE;
        }
    }

    public PerfIdPopulator() {
        timer = new Timer();
        long period = 1000L;
        refreshAtomicIdElement();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long timestamp = TimeUtils.genTime(IdType.MAX_PEAK);
                long lastTimestamp = referenceIdElement.get().getTimeStamp();
                if (timestamp <= lastTimestamp) {
                    log.error(String
                            .format("Clock moved backwards.  Refusing to generate id for %d second/milisecond.",
                                    lastTimestamp - timestamp));
                    lockAtomicIdElement(lastTimestamp);
                } else{
                    refreshAtomicIdElement();
                }
            }
        }, getNextSecond(), period);
    }

    private void lockAtomicIdElement(long lastTimestamp) {
        referenceIdElement = new AtomicReference<IdElement>(new IdElement(PEAK_SEQUENCE,lastTimestamp));
    }

    public void populateId(Id id, IdMeta idMeta) {
        IdElement idElement = getIdElement();
        long sequence = idElement.getSequence();
        sequence &= idMeta.getSeqBitsMask();
        id.setSeq(sequence);
        id.setTime(idElement.getTimeStamp());
    }

    private void refreshAtomicIdElement() {
        long timestamp = TimeUtils.genTime(IdType.MAX_PEAK);
        referenceIdElement = new AtomicReference<IdElement>(new IdElement(0L,timestamp));
    }

    private Date getNextSecond() {
        Calendar now = Calendar.getInstance();
        Calendar nextSecond = Calendar.getInstance();
        nextSecond.set(
                now.get(Calendar.YEAR),
                now.get(Calendar.MONTH),
                now.get(Calendar.DAY_OF_MONTH),
                now.get(Calendar.HOUR_OF_DAY),
                now.get(Calendar.MINUTE),
                now.get(Calendar.SECOND)+1);
        return nextSecond.getTime();
    }

    private IdElement getIdElement() {
        while (referenceIdElement==null || referenceIdElement.get().isOutOfSeq()) { }

        IdElement result;
        while (true) {
            result = referenceIdElement.get();
            boolean isSet = referenceIdElement.compareAndSet(result, new IdElement(result.getSequence() + 1, result.getTimeStamp()));
            if (isSet) {
                break;
            }
        }
        return result;
    }

    @PreDestroy
    public void destroy() {
        timer.cancel();
    }

}
