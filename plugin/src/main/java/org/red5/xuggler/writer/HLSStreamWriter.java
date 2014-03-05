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

package org.red5.xuggler.writer;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import io.humble.video.AudioFormat.Type;
import io.humble.video.Codec;
import io.humble.video.Codec.ID;
import io.humble.video.Coder;
import io.humble.video.Decoder;
import io.humble.video.Encoder;
import io.humble.video.Global;
import io.humble.video.MediaAudio;
import io.humble.video.MediaPacket;
import io.humble.video.MediaPicture;
import io.humble.video.Muxer;
import io.humble.video.MuxerFormat;
import io.humble.video.MuxerStream;
import io.humble.video.Demuxer;
import io.humble.video.DemuxerStream;
import io.humble.video.PixelFormat;
import io.humble.video.Rational;
import io.humble.video.KeyValueBag;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.red5.server.api.stream.IStream;
import org.red5.service.httpstream.SegmentFacade;
import org.red5.service.httpstream.SegmenterService;
import org.red5.service.httpstream.model.Segment;
import org.red5.stream.http.xuggler.MpegTsHandlerFactory;
import org.red5.stream.http.xuggler.MpegTsIoHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An writer that encodes and decodes media to containers. Based on MediaWriter class from Xuggler.
 * 
 * <table border="1">
 * <tr><td>AAC-LC</td><td>"mp4a.40.2"</td></tr>
 * <tr><td>HE-AAC</td><td>"mp4a.40.5"</td></tr>
 * <tr><td>MP3</td><td>"mp4a.40.34"</td></tr>
 * <tr><td>H.264 Baseline Profile level 3.0</td><td>"avc1.42001e" or avc1.66.30<br />
 * Note: Use avc1.66.30 for compatibility with iOS versions 3.0 to 3.12.</td></tr>
 * <tr><td>H.264 Baseline Profile level 3.1</td><td>"avc1.42001f"</td></tr>
 * <tr><td>H.264 Main Profile level 3.0</td><td>"avc1.4d001e" or avc1.77.30<br />
 * Note: Use avc1.77.30 for compatibility with iOS versions 3.0 to 3.12.</td></tr>
 * <tr><td>H.264 Main Profile level 3.1</td><td>"avc1.4d001f"</td></tr>
 * </table>
 * 
 * #EXT-X-STREAM-INF:PROGRAM-ID=1, BANDWIDTH=3000000, CODECS="avc1.4d001e,mp4a.40.5"
 * 
 * http://developer.apple.com/library/ios/#documentation/networkinginternet/conceptual/streamingmediaguide/FrequentlyAskedQuestions/FrequentlyAskedQuestions.html
 * 
 * Segment handling
 * http://www.ffmpeg.org/ffmpeg-formats.html#toc-mpegts
 * http://www.ffmpeg.org/ffmpeg-formats.html#toc-segment_002c-stream_005fsegment_002c-ssegment
 * 
 * @author Paul Gregoire (mondain@gmail.com)
 * @author Gavriloaie Eugen-Andrei(crtmpserver@gmail.com)
 * @author Andy Shaules (bowljoman@gmail.com)
 */
public class HLSStreamWriter implements IStreamWriter {

    private static final Logger log = LoggerFactory.getLogger(HLSStreamWriter.class);

    static {
	io.humble.ferry.JNIMemoryManager.setMemoryModel(io.humble.ferry.JNIMemoryManager.MemoryModel.NATIVE_BUFFERS);
    }

    /** The default time base. */
    private static final Rational DEFAULT_TIMEBASE = Rational.make(1, (int) Global.DEFAULT_PTS_PER_SECOND);

    private SegmentFacade facade;

    private final String outputUrl;

    private final String streamName;

    // the container
    private Muxer container;

    // the container format
    //private Muxer containerFormat;

    private MuxerStream audioStream;

    private MuxerStream videoStream;

    private Encoder audioCoder;

    private Encoder videoCoder;

    // true if the writer should ask FFMPEG to interleave media
    private boolean forceInterleave = false;

    private boolean audioComplete = false;

    private boolean videoComplete = false;

    private volatile double audioDuration;

    private volatile double videoDuration;

    private volatile double lastNewFilePosition = 0;
    private volatile double currentFilePosition = 0;

    //TODO: implement private int videoBitRate = 360000;

    private long prevAudioTime = 0L;

    private long prevVideoTime = 0L;
    private int segmentSize = 10;

    /**
     * Create a MediaWriter which will require subsequent calls to {@link #addVideoStream} and/or {@link #addAudioStream} to configure the
     * writer.  Streams may be added or further configured as needed until the first attempt to write data.
     *
     * @param streamName the stream name of the source
     */
    public HLSStreamWriter(String streamName) {
	outputUrl = MpegTsHandlerFactory.DEFAULT_PROTOCOL + ':' + streamName;
	this.streamName = streamName;
    }
	
    public void setup(SegmentFacade segmentFacade) {
	log.info("setup {}", outputUrl);
	this.facade = segmentFacade;
	MpegTsIoHandler outputHandler = new MpegTsIoHandler(outputUrl, facade);
	// create a container
	MuxerFormat muxerFormat = MuxerFormat.guessFormat("mpegts",null,null);
	log.info(muxerFormat.toString());
	container = Muxer.make(outputUrl, muxerFormat,null);
	MpegTsHandlerFactory.getFactory().registerStream(outputHandler, container);
    }

    public void encodeVideo(MediaPacket videoPacket, long timeStamp, TimeUnit timeUnit) {
	log.info("encodeVideo {} timestamp {} timeunit {} ", outputUrl,timeStamp,timeUnit.toString());


	currentFilePosition = videoPacket.getTimeStamp() * videoPacket.getTimeBase().getValue();
	// establish the stream, return silently if no stream returned
	if (null != videoPacket) {
	    // encode video picture
	    videoComplete = videoPacket.isComplete();
	    if (videoComplete) {
		final long timeStampMicro;
		if (timeUnit == null) {
		    timeStampMicro = Global.NO_PTS;
		} else {
		    timeStampMicro = MICROSECONDS.convert(timeStamp, timeUnit);
		}
		log.info("Video timestamp {} us", timeStampMicro);
		// write packet
		writePacket(videoPacket);
		// add the duration of our video
		double dur = (timeStampMicro + videoPacket.getDuration() - prevVideoTime) / 1000000d;
		videoDuration += dur;
		log.info("Duration - video: {}", dur);
		videoPacket.delete();
	    } else {
		log.warn("Video packet was not complete");
	    }
	} else {
	    throw new IllegalArgumentException("No video packet");
	}
    }

    /**
     * Write packet to the output container
     * 
     * @param packet the packet to write out
     */
    public void writePacket(MediaPacket packet) {
	log.info("write packet - duration: {} timestamp: {}", packet.getDuration(), packet.getTimeStamp());
	if (createNewSegment()) {
	    log.info("New segment created: {}", facade.getActiveSegmentIndex());
	}
	if (container.write(packet, forceInterleave)) {
	    log.info("Failed to write packet: {} force interleave: {}", packet, forceInterleave);
	}
    }

    public void open(Demuxer reader) {
	log.info("open {}", outputUrl);

	KeyValueBag  meta = KeyValueBag.make();
	meta.setValue("service_provider", "Red5 HLS");
	meta.setValue("title", outputUrl.substring(outputUrl.indexOf(':') + 1));
	meta.setValue("map", "0");
	meta.setValue("segment_time", "" + facade.getSegmentTimeLimit() / 1000);
	meta.setValue("segment_format", "mpegts");
	meta.setValue("reset_timestamps", "0");


	try {
	    int n = reader.getNumStreams();
	    for(int i =0; i < n; i++) {
		DemuxerStream ds = null;
		synchronized(reader) {
		    ds = reader.getStream(i);
		}
		Decoder d = ds.getDecoder();
		log.info("----  MODEC"+d.toString());
		container.addNewStream(d);

	    }
	    container.open(null, null);
	} catch(InterruptedException e) {
	    java.io.StringWriter sw = new java.io.StringWriter();
	    e.printStackTrace(new java.io.PrintWriter(sw));
	    String exceptionAsString = sw.toString();
	    throw new RuntimeException("interruptedException:"+sw.toString());
	    
	} catch (IOException e) {
	    java.io.StringWriter sw = new java.io.StringWriter();
	    e.printStackTrace(new java.io.PrintWriter(sw));
	    String exceptionAsString = sw.toString();
	    throw new RuntimeException("IOException:"+sw.toString());

	}

	log.info("writer opened");

    }

	
    /** 
     * Flush any remaining media data in the media coders.
     */
    public void flush() {
	log.debug("flush {}", outputUrl);
	if (audioCoder.getState().equals(Coder.State.STATE_OPENED)) {
	    MediaPacket packet = MediaPacket.make();
	    while (!packet.isComplete()) {
		audioCoder.encodeAudio(packet, null);
	    }
	    packet.delete();
	}
	// flush video coder
	if (videoCoder.getState().equals(Coder.State.STATE_OPENED)) {
	    MediaPacket packet = MediaPacket.make();
	    while (!packet.isComplete()) {
		videoCoder.encodeVideo(packet, null);
	    }
	    packet.delete();
	}
	// flush the container
	container.close();;
    }

    /** {@inheritDoc} */
    public void close() {
	log.debug("close {}", outputUrl);
	MpegTsHandlerFactory.getFactory().deleteStream(outputUrl);

	flush();
	if(videoCoder.getState().equals(Coder.State.STATE_OPENED)){
	    videoCoder.delete();
	    videoCoder = null;
	}
	if(audioCoder.getState().equals(Coder.State.STATE_OPENED)){
	    audioCoder.delete();
	    audioCoder = null;
	}

	// get the current segment, if one exists
	Segment segment = facade.getSegment();
	// mark it as "last" and close
	if (segment != null && !segment.isLast()) {
	    // mark it as the last
	    segment.setLast(true);
	    segment.close();
	}
    }

    /**
     * Decides whether or not to create a new segment based on the current duration of audio or video.
     * 
     * @return true if a new segment is created and false otherwise
     */
    private boolean createNewSegment() {
	// get the current segment, if one exists
	Segment segment = facade.getSegment();
	if (segment != null) {
	    log.info("Segment returned, check durations currentFilePosition {} lastNewFilePosition {}", currentFilePosition,lastNewFilePosition);
	    // convert segment limit to seconds
	    double limit = facade.getSegmentTimeLimit() / 1000d;
	    log.info("Segment limit: {} audio: {} video: {}", limit, audioDuration, videoDuration);
	    if ((currentFilePosition - lastNewFilePosition)  >= limit) {

		int duration = (int) (currentFilePosition - lastNewFilePosition);
		log.info("Duration matched, create new segment {}", duration);
		segment.setDuration(duration);
		facade.createSegment();
		lastNewFilePosition = currentFilePosition;
		return true;
	    }
	} else {
	    log.info("No segment returned, create first segment");
	    // first segment
	    facade.createSegment();
	    return true;
	}
	return false;
    }

    /**
     * Get the default time base we'll use on our encoders if one is not specified by the codec.
     * @return the default time base
     */
    public Rational getDefaultTimebase() {
	return DEFAULT_TIMEBASE.copyReference();
    }

    /** {@inheritDoc} */
    public String toString() {
	return "HLSStreamWriter[" + outputUrl + "]";
    }

    @Override
    public void setup(SegmentFacade facade, Muxer muxer) {
	// TODO Auto-generated method stub
		
    }

}
