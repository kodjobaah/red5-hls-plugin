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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.red5.service.httpstream;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import org.red5.logging.Red5LoggerFactory;
import org.red5.server.api.scope.IScope;
import org.red5.server.api.stream.IBroadcastStream;
import org.red5.service.httpstream.model.Segment;
import org.slf4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import io.humble.video.Global;

/**
 * Creates, updates, locates, and manages media segments.
 * 
 * @author Paul Gregoire (mondain@gmail.com)
 */
public class SegmenterService implements InitializingBean, DisposableBean {

    private static Logger log = Red5LoggerFactory.getLogger(SegmenterService.class);
	
    // map of currently available segment facades, keyed by stream name
    private static ConcurrentMap<String, SegmentFacade> segmentMap = new ConcurrentHashMap<String, SegmentFacade>();

    // execution handling
    private ThreadPoolTaskScheduler segmentExecutor;

    // length of a segment in milliseconds
    private long segmentTimeLimit = 6000;

    // where to write segment files
    private String segmentDirectory;

    // whether to use files or memory for segments
    private boolean memoryMapped;

    // maximum number of segments to keep available per stream
    private int maxSegmentsPerFacade = 8;
	
    private String outputAudioCodec = "libvo_aacenc";
	
    private String outputVideoCodec = "libx264";

    /**
     * Creates and starts a facade and adds an audio mux for the given scope.
     * 
     * @param scope
     * @param stream
     * @param useRTMPReader
     */
    public void start(IScope scope, IBroadcastStream stream, boolean useRTMPReader) {
	log.info("start - scope: {} stream: {} rtmp reader: {}", scope.getName(), stream.getPublishedName(), useRTMPReader);
	String streamName = stream.getPublishedName();
	start(streamName, useRTMPReader);
    }
	
    /**
     * Creates and starts a facade.
     * 
     * @param name
     * @param useRTMPReader
     */
    public SegmentFacade start(String name, boolean useRTMPReader) {
	log.info("start and creating facade - name: {} rtmp reader: {}", name, useRTMPReader);
	// lookup the associated segment
	SegmentFacade facade = segmentMap.get(name);
	if (facade == null) {
	    log.info("Creating segment facade for {}", name);

	    // create a facade
	    facade = new SegmentFacade(this, name);	

	    // add to the map
	    addFacade(name, facade);

	    // configure
	    facade.setSegmentTimeLimit(segmentTimeLimit);
	    log.info("SEGMENT DIRECTORY:"+segmentDirectory);
	    facade.setSegmentDirectory(segmentDirectory);
	    facade.setMaxSegmentsPerFacade(maxSegmentsPerFacade);
	    facade.setMemoryMapped(memoryMapped);
	    facade.setOutputAudioCodec(outputAudioCodec);
	    facade.setOutputVideoCodec(outputVideoCodec);

	    // initialization
	    if (useRTMPReader) {

		// initialize RTMP reader
		log.info("before init reader");
		facade.initReader();

		// initialize HLS writer
		log.info("before init writer");
		facade.initWriter();

	    }
			

	}
	return facade;
    }

    public void afterPropertiesSet() throws Exception {

	//TODO: Determine if setTurboCharged exists for H-IO
	//ToolFactory.setTurboCharged(true);
	if (log.isDebugEnabled()) {
	    Global.setFFmpegLoggingLevel(99);
	}
	log.info("Executor - prefers short tasks: {} daemon: {}", segmentExecutor.prefersShortLivedTasks(), segmentExecutor.isDaemon());
    }

    public void destroy() throws Exception {
	// walk the map and close them down
	for (Entry<String, SegmentFacade> entry : segmentMap.entrySet()) {
	    
	    SegmentFacade value = entry.getValue();
	    Segment segment = value.getSegment();
	    //TODO: Should the files associated with the segment also be removed
	    if (segment != null) {
		segment.setLast(true);
		segment.close();
	    }
	    segmentMap.remove(entry.getKey(), value);
	}
	segmentMap.clear();
    }

    public Future<?> submitJob(Runnable task) {
	log.debug("submitJob: {}", task.getClass().getName());
	return segmentExecutor.submit(task);
    }

    public Future<?> submitJob(Runnable task, long period) {
	log.debug("submitJob: {} period: {}", task.getClass().getName(), period);
	return segmentExecutor.scheduleAtFixedRate(task, period);
    }	
	
    /**
     * @param segmentExecutor the segmentExecutor to set
     */
    public void setSegmentExecutor(ThreadPoolTaskScheduler segmentExecutor) {
	this.segmentExecutor = segmentExecutor;
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
	
    public long getSegmentTimeLimit() {
	return segmentTimeLimit;
    }

    public void setSegmentTimeLimit(long segmentTimeLimit) {
	this.segmentTimeLimit = segmentTimeLimit;
    }

    public String getSegmentDirectory() {
	return segmentDirectory;
    }

    public void setSegmentDirectory(String segmentDirectory) {
	this.segmentDirectory = segmentDirectory;
    }

    public boolean isMemoryMapped() {
	return memoryMapped;
    }

    public void setMemoryMapped(boolean memoryMapped) {
	this.memoryMapped = memoryMapped;
    }

    public int getMaxSegmentsPerFacade() {
	return maxSegmentsPerFacade;
    }

    public void setMaxSegmentsPerFacade(int maxSegmentsPerFacade) {
	this.maxSegmentsPerFacade = maxSegmentsPerFacade;
    }

    public int getSegmentCount(String streamName) {
	SegmentFacade facade = segmentMap.get(streamName);
	return facade.getSegmentCount();
    }

    public Segment getSegment(String streamName) {
	SegmentFacade facade = segmentMap.get(streamName);
	return facade.getSegment();
    }

    public Segment getSegment(String streamName, int index) {
	SegmentFacade facade = segmentMap.get(streamName);
	return facade.getSegment(index);
    }

    public Segment[] getSegments(String streamName) {
	SegmentFacade facade = segmentMap.get(streamName);
	return facade.getSegments();
    }
	
    public boolean isAvailable(String streamName) {
	return segmentMap.containsKey(streamName);
    }

    protected void addFacade(String streamName, SegmentFacade facade) {
	segmentMap.put(streamName, facade);
    }	

}
