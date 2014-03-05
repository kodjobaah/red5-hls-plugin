/*
 * RED5 HLS plugin - https://github.com/mondain/red5-hls-plugin
 * 
 * Copyright 2006-2013 by respective authors (see below). All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is dixquustributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.red5.service.httpstream;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.red5.logging.Red5LoggerFactory;
import org.red5.service.httpstream.model.Segment;
import org.red5.stream.util.BufferUtils;
import org.red5.humble.reader.RTMPReader;
import org.red5.humble.writer.HLSStreamWriter;

import org.slf4j.Logger;

import io.humble.video.Global;
import io.humble.video.MediaAudio;
import io.humble.video.Codec;
import io.humble.video.Muxer;
import io.humble.video.MediaPacket;
import io.humble.video.javaxsound.StereoS16AudioConverter;



/**
 * Common location for segment related objects.
 * 
 * @author Paul Gregoire (mondain@gmail.com)
 */

public class SegmentFacade {

    private static Logger log = Red5LoggerFactory.getLogger(SegmentFacade.class);
    protected final long creationTime;
    protected final WeakReference<SegmenterService> segmenterReference;
    protected final String streamName;

    // reads the source stream data
    private RTMPReader reader;

    // writes the output
    private HLSStreamWriter writer;

    // queue of segments
    private ConcurrentLinkedQueue<Segment> segments = new ConcurrentLinkedQueue<Segment>();

    // queue for data coming from humble
    private ConcurrentLinkedQueue<IQueuedData> dataQueue = new ConcurrentLinkedQueue<IQueuedData>();

    // segment currently being written to
    private volatile Segment segment;

    // segment index counter
    private AtomicInteger counter = new AtomicInteger();

    private AtomicBoolean queueWorkerRunning = new AtomicBoolean(false);

    private Future<?> queueWorkerFuture;

    // length of a segment in milliseconds
    private long segmentTimeLimit;

    // where to write segment files
    private String segmentDirectory;

    // whether to use files or memory for segments
    private boolean memoryMapped;

    // maximum number of segments to keep available per stream
    private int maxSegmentsPerFacade;

    private String outputAudioCodec;
    private String outputVideoCodec;
    private Codec audioCodec;

    public SegmentFacade(SegmenterService segmenter, String streamName) {

	log.debug("Segment facade for: {}", streamName);

	// created at
	creationTime = System.currentTimeMillis();

	// set ref to our parent
	segmenterReference = new WeakReference<SegmenterService>(segmenter);
	this.streamName = streamName;
    }


    /**
     * Initializes a reader to provide a/v data. If a reader is required, it must be initialized before calling initWriter().
     */
    public void initReader() {
	log.info("Initialize reader for {}", streamName);
	reader = new RTMPReader(this,"rtmp://127.0.0.1:1935/hlsapp/" + streamName + " live=1 buffer=1");
	// initialize reader
	reader.init();		

    }


    /**
     * Initializes a writer for HLS segments.
     */
    public void initWriter() {

	log.info("Initialize writer for {}", streamName);
	// setup our writer
	writer = new HLSStreamWriter(streamName);

	// create a description of the output

	log.debug("Output codecs - video: {}", outputVideoCodec);
	writer.setup(this);

	// open the writer so we can configure the coders
	log.info("Opening writer");

	writer.open(reader.getReader());

	// after the writer is started, add adjustments to an existing reader
	if (reader != null) {
	    log.info("Spawning the reader");
	    // start the reader
	    segmenterReference.get().submitJob(reader);
	}
	// spawn the queue worker
	log.info("Spawning and scheduling the queue worker");
	queueWorkerFuture = segmenterReference.get().submitJob(new QueueWorker(), 33L);
    }

    public int getSegmentCount() {
	log.info("Total segments: {}", segments.size());
	return isComplete() ? segments.size() : segments.size() - 1;

    }

    public int getActiveSegmentIndex() {
	return segment.getIndex();

    }


    /**
     * Whether or not this facade is on its last segment.
     * 
     * @return
     */
    public boolean isComplete() {
	return segment != null ? segment.isLast() : false;

    }

    /**
     * Whether or not this facade is receiving data.
     * 
     * @return
     */
    public boolean isReceivingData() {
	if (dataQueue.isEmpty()) {
	    if (reader != null && reader.isClosed()) {
		log.info("No more data being received, reader is closed");
		return false;
	    }
	    
	}		
	
	return true;
    }


    /**
     * Whether or not a timeout from the start of streaming has elapsed. This is used in conjunction with
     * isReceivingData() to determine if a stream is alive.
     * 
     * @return
     */
    private boolean isTimedOut() {
	return (System.currentTimeMillis() - creationTime) > 120000L; // 2 min timeout

    }

    /**
     * Creates and returns a new segment.
     * 
     * @return segment
     */
    public synchronized Segment createSegment() {
	// clean up current segment if it exists
	if (segment != null) {
	    log.info("Close segment {}? Duration: {}", segment.getIndex(), segment.getDuration());

	    // verify that this is not a "new" segment
	    if (segment.getDuration() == 0d) {
		return segment;
		
	    }
	    
	    // closing current segment
	    segment.close();

	}


	log.info("createSegment for {}", streamName);
	// create a segment - default is memory mapped
	segment = new Segment(segmentDirectory, streamName, counter.getAndIncrement(), memoryMapped);
	// add to the map for lookup
	if (segments.add(segment)) {
	    log.info("Segment {} added, total: {}", segment.getIndex(), segments.size());
	}


	// enforce segment list length
	if (segments.size() > maxSegmentsPerFacade) {
	    // get current segments index minus max
	    int index = segment.getIndex() - maxSegmentsPerFacade;
	    for (Segment seg : segments) {
		if (seg.getIndex() <= index) {
		    log.info("Removing segment: {}", seg.getIndex());
		    segments.remove(seg);
		    // access to the segment is no longer required
		    seg.dispose();
		}
	    }
	}
	return segment;

    }


    /**
     * Returns the active segment.
     * 
     * @return segment currently being written to
     */
    public synchronized Segment getSegment() {
		return segment;
    }


    /**
     * Returns a segment matching the requested index.
     * 
     * @return segment matching the index or null
     */
    public Segment getSegment(int index) {
	Segment result = null;
	if (index < counter.get()) {
	    for (Segment seg : segments) {
		if (seg.getIndex() == index) {
		    result = seg;
		    break;
		}
	    }
	} else {
	    log.info("No segment available");

	}
	return result;
    }


    public Segment[] getSegments() {
	// make room for all but the last / current segment
	int count = getSegmentCount();
	log.info("Segments after call to getSegmentCount: {}", count);
	Segment[] segs = new Segment[count];
	log.info("Segments to return: {}", segs.length);
	if (segs.length > 0) {
	    int s = 0;
	    for (Segment seg : segments) {
		int idx = seg.getIndex();
		log.info("Segment index: {}", idx);
		try {
		    segs[s++] = seg;
		} catch (ArrayIndexOutOfBoundsException aiob) {
		    // this happens when we have an active segment
		    
		}
	    }
	} else {
	    log.warn("Not enough segments available");
	}
	return segs;
    }

    
    public Segment popSegment() {
	return segments.poll();

    }


    /**
     * Queue the packet from humble
     *
     * @param packet The packe to queue
     */
    public void queuePacket(MediaPacket packet) {
	log.info("Queue Packet:"+dataQueue.isEmpty());
	dataQueue.add(new QueuedPacketData(packet,Global.DEFAULT_TIME_UNIT));
    }

    /**
     * @param outputAudioCodec the outputAudioCodec to set
     */
    public void setOutputAudioCodec(String outputAudioCodec) {
	this.outputAudioCodec = outputAudioCodec;

    }

    /**
     * @param outputVideoCodec the outputVideoCodec to set
     */
    public void setOutputVideoCodec(String outputVideoCodec) {
	this.outputVideoCodec = outputVideoCodec;

    }

    /**
     * @return the segmentTimeLimit
     */
    public long getSegmentTimeLimit() {
	return segmentTimeLimit;

    }


    /**
     * @param segmentTimeLimit the segmentTimeLimit to set
     */
    public void setSegmentTimeLimit(long segmentTimeLimit) {
	this.segmentTimeLimit = segmentTimeLimit;

    }

    /**
     * @return the segmentDirectory
     */
    public String getSegmentDirectory() {
	return segmentDirectory;
    }

    
    /**
     * @param segmentDirectory the segmentDirectory to set
     */
    public void setSegmentDirectory(String segmentDirectory) {
	this.segmentDirectory = segmentDirectory;

    }

    /**
     * @return the memoryMapped
     */
    public boolean isMemoryMapped() {
	return memoryMapped;

    }

    /**
     * @param memoryMapped the memoryMapped to set
     */
    public void setMemoryMapped(boolean memoryMapped) {
	this.memoryMapped = memoryMapped;

    }

    /**
     * @return the maxSegmentsPerFacade
     */
    public int getMaxSegmentsPerFacade() {
	return maxSegmentsPerFacade;

    }

    /**
     * @param maxSegmentsPerFacade the maxSegmentsPerFacade to set
     */
    public void setMaxSegmentsPerFacade(int maxSegmentsPerFacade) {
	this.maxSegmentsPerFacade = maxSegmentsPerFacade;

    }

    @Override
    public String toString() {
	return streamName;
    }

    /**
     * Interface for queued data originated from Humble
     */
    interface IQueuedData {
	long getTimeStamp();
	TimeUnit getTimeUnit();
    }

    /**
     * Queued audio data originated from Humble
     */
    private final class QueuedAudioData implements IQueuedData {
	
	final short[] samples;
	final long timeStamp;

	final TimeUnit timeUnit;

	@SuppressWarnings("unused")
	QueuedAudioData(MediaAudio isamples, long timeStamp, TimeUnit timeUnit) {
	    StereoS16AudioConverter sac = new StereoS16AudioConverter(isamples.getSampleRate(), isamples.getChannelLayout(), isamples.getFormat());
	    ByteBuffer buf = ByteBuffer.allocate(isamples.getNumSamples()*isamples.getBytesPerSample());
	    sac.toJavaAudio(buf, isamples);
	    byte[] decoded = new byte[buf.limit()];
	    buf.get(decoded);
	    buf.flip();
	    this.samples = BufferUtils.byteToShortArray(decoded, 0, decoded.length, true);
	    this.timeStamp = isamples.getTimeStamp();
	    this.timeUnit = timeUnit;
	}

	/**
	 * @return the samples
	 */
	public short[] getSamples() {
	    return samples;
	}

	/**
	 * @return the timeUnit
	 */
	public TimeUnit getTimeUnit() {
	    return timeUnit;
	}
	
	public long getTimeStamp() {
	    return timeStamp;
	}
	
    }



    /**
     * Queued packet data originated from humble 
     */
    private final class QueuedPacketData implements IQueuedData {


	final MediaPacket packet;
	final TimeUnit timeUnit;
	final long timeStamp;

	QueuedPacketData(MediaPacket packet, TimeUnit timeUnit) {
	    this.packet = packet;
	    this.timeUnit = timeUnit;
	    this.timeStamp = packet.getTimeStamp();
	}

	public MediaPacket getPacket() {
	    return this.packet;
	}

	/**
	 * @return the timeUnit
	 */
	public TimeUnit getTimeUnit() {
	    return this.timeUnit;

	}

	public long getTimeStamp() {
	    return timeStamp;
	}
	
    }


    /**
     * Routes the queued humble derived data to the segments.
     */
    private final class QueueWorker implements Runnable {

	Logger qwLog = Red5LoggerFactory.getLogger(QueueWorker.class);

	public void run() {
	    // ensure the job is not already running
	    if (queueWorkerRunning.compareAndSet(false, true)) {
		qwLog.info("QueueWorker - run:"+dataQueue.isEmpty());
		try {

		    if (!dataQueue.isEmpty()) {
			IQueuedData q = null;
			while ((q = dataQueue.poll()) != null) {
			    // send video to the hls writer
			    log.info("writing packet");
			    writer.encodePacket(((QueuedPacketData) q).getPacket(), q.getTimeStamp(), q.getTimeUnit());
			}
		    } else {
			qwLog.info("Queue is empty");
		    }
		} catch (Exception e) {
		    qwLog.warn("Exception handling queue", e);
		} finally {
		    // check if we are no longer getting data
		    if (!isReceivingData() && isTimedOut()) {
			log.info("Cancelling queue worker, no more data being received");
			queueWorkerFuture.cancel(true);
			writer.close();
		    }
		    queueWorkerRunning.compareAndSet(true, false);
		}
		qwLog.info("QueueWorker - end");
	    } else {
		qwLog.info("QueueWorker - already running");
	    }
	}
	
    }
    
    public void queueAudio(short[] samples, int clock, TimeUnit defaultTimeUnit) {
	// TODO Auto-generated method stub
	
    }
}
