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



package org.red5.xuggler.reader;



import io.humble.video.Demuxer;
import io.humble.video.DemuxerFormat;
import io.humble.video.MediaAudio;
import io.humble.video.MediaPacket;
import io.humble.video.MediaPicture;
import io.humble.video.KeyValueBag;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import org.red5.service.httpstream.SegmentFacade;

//import io.humble.video.IMediaReader;
//import io.humble.video.MediaToolAdapter;
//import io.humble.video.ToolFactory;
//import io.humble.video.event.IAudioSamplesEvent;
//import io.humble.video.event.ICloseEvent;
//import io.humble.video.event.IVideoPictureEvent;

/**
 * Reads media data from an RTMP source.
 * 
 * @author Paul Gregoire (mondain@gmail.com)
 */

public class RTMPReader implements GenericReader {

    private Logger log = LoggerFactory.getLogger(RTMPReader.class);
    private Demuxer reader;
    private String inputUrl;
    private int inputWidth;
    private int inputHeight;
    private int inputSampleRate;
    private int inputChannels;
    private boolean audioEnabled = true;
    private boolean videoEnabled = true;
    private boolean keyFrameReceived;

    // time at which we started reading
    private long startTime;

    // total samples read
    private volatile long audioSamplesRead;

    // total frames read
    private volatile long videoFramesRead;
    private boolean closed = true;

    private SegmentFacade facade = null;

    public RTMPReader() {

    }

    public RTMPReader(SegmentFacade facade, String url) {
	this.facade = facade;
	inputUrl = url;
    }
    public void init() {
	log.info("Input url: {}", inputUrl);
	// url only
	reader = Demuxer.make();
	
	reader.setReadRetryCount(0);
	reader.setInputBufferLength(4096);
	reader.setProperty("analyzeduration", 0);
	DemuxerFormat df = DemuxerFormat.findFormat("flv");

	try  {
	    log.info("before trying to load");
	    reader.open(inputUrl,df,true,true,null,null);
	    log.info("able to open reader");
	} catch (java.lang.InterruptedException ie) {
	    throw new RuntimeException("Unable to Open Reader:"+ie.getMessage());
	} catch (java.io.IOException io) {
	    throw new RuntimeException("Unable to Open Reader:"+io.getMessage());
	}

    }

    public void stop() {
	log.debug("Stop");
	if (reader != null) {
	    try {
		reader.close();
	    } catch (Exception e) {
		log.warn("Exception closing reader", e);
	    } finally {
		closed = true;
	    }
	    reader = null;
	}
    }

    public void run() {
	log.info("RTMPReader - run");
	// read and decode packets from the source
	
	log.info("Starting reader loop");
	startTime = System.currentTimeMillis();

	// open for business
	closed = false;
	int packetsRead = 0;

	try {
	    
	    boolean start =  true;
	    int res = -1;
	    do {
		if (!start) {
		    long elapsedMillis = (System.currentTimeMillis() - startTime);
		    log.info("Reads - frames: {} samples: {}", videoFramesRead, audioSamplesRead);
		    log.info("Reads - packets: {} elapsed: {} ms", packetsRead++, elapsedMillis);
		}else {
		    start = false;
		}
		synchronized(reader) {
		    MediaPacket packet = MediaPacket.make();
		    res = reader.read(packet);
		    if (res == 0) {
		        log.info("adding packets to queue:["+res+"]");
			facade.queueVideo(packet);
		    }
		}
	       
	    }while (res >= 0);
	    log.info("Done demuxing");
		
	} catch (Throwable t) {
	    log.warn("Exception closing reader", t);
	}
	log.info("End of reader loop");
	stop();
	log.info("RTMPReader - end");
    }

    
    public void onClose() {
	log.debug("Reader close");
	this.stop();
    }

    public String getInputUrl() {
	return inputUrl;
    }
    
    public void setInputUrl(String inputUrl) {
	this.inputUrl = inputUrl;
    }

    /**
     * @return the inputWidth
     */
    public int getInputWidth() {
	return inputWidth;
    }

    /**
     * @return the inputHeight
     */
    public int getInputHeight() {
	return inputHeight;
    }

    /**
     * @return the inputSampleRate
     */
    public int getInputSampleRate() {
	return inputSampleRate;
    }

    /**
     * @return the inputChannels
     */
    public int getInputChannels() {
	return inputChannels;
    }

    /**
     * @return the reader
     */
    public Demuxer getReader() {
	return reader;
    }


    /**
     */
    public void disableAudio() {
	this.audioEnabled = false;
    }

    /**
     */
    public void disableVideo() {
	this.videoEnabled = false;
    }

    /**
     * @return the audioEnabled
     */
    public boolean isAudioEnabled() {
	return audioEnabled;
    }

    /**
     * @return the videoEnabled
     */
    public boolean isVideoEnabled() {
	return videoEnabled;
    }

    /**
     * Returns whether or not the reader is closed.
     * 
     * @return true if closed or reader does not exist
     */
    public boolean isClosed() {
	return closed;
    }

    /**
     * Returns true if data has been read from the source.
     * 
     * @return
     */
    public boolean hasReadData() {
	return audioSamplesRead > 0 || videoFramesRead > 0;
    }

    /**
     * @return the keyFrameReceived
     */
    public boolean isKeyFrameReceived() {
	return keyFrameReceived;
    }

}

