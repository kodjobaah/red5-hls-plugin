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
import io.humble.video.MediaAudio;
import io.humble.video.MediaPacket;
import io.humble.video.MediaPicture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public RTMPReader() {
	}

	public RTMPReader(String url) {
		inputUrl = url;
	}

	public void init() {
		log.debug("Input url: {}", inputUrl);
		/*
		IContainerFormat format = IContainerFormat.make();
		format.setInputFormat("flv");
		IContainer container = IContainer.make(format);
		container.setReadRetryCount(0);
		container.setInputBufferLength(0);
		container.setProperty("strict", "experimental");
		container.setProperty("analyzeduration", 0); // int = specify how many microseconds are analyzed to probe the input (from 0 to INT_MAX)
		if (container.open(inputUrl, IContainer.Type.READ, null, false, false) < 0) {
		    throw new RuntimeException("Unable to open read container");
		}		
		reader = ToolFactory.makeReader(container);
		*/

		// url only
		reader = Demuxer.make();
		//TODO: Determine necessity -> reader.setCloseOnEofOnly(false);
		//TODO: implement in reader.open() -> reader.setQueryMetaData(true);
		//TODO: implement in reader.open() -> reader.setAddDynamicStreams(false);

		// get the container
		reader.setReadRetryCount(0);
		reader.setInputBufferLength(0);
		reader.setProperty("analyzeduration", 0); // int = specify how many microseconds are analyzed to probe the input (from 0 to INT_MAX)
		//		container.setProperty("probesize", 1024); // int = set probing size in bytes (from 32 to INT_MAX)
		//		container.setProperty("fpsprobesize", 4); // int = number of frames used to probe
		//		container.setPreload(1);
		//		IContainerFormat format = container.getContainerFormat();
		//		format.setInputFormat("flv");
		//		container.setFormat(format);
		
		//TODO -> determine what we're trying to do here ->
		/*if (videoEnabled) {
			// have the reader create a buffered image that others can reuse
			//reader.setBufferedImageTypeToGenerate(BufferedImage.TYPE_3BYTE_BGR);
		} else {
			reader.setBufferedImageTypeToGenerate(-1);
		}*/
		
		//add this as the first listener
		//TODO: [#1000] See if we need to dupe this behavior for plugin functionality -> reader.addListener(this);
	}

	public void stop() {
		log.debug("Stop");
		if (reader != null) {
			//TODO: as [#1000] -> reader.removeListener(this);
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
		log.debug("RTMPReader - run");
		// read and decode packets from the source
		log.trace("Starting reader loop");

		//		IContainer container = reader.getContainer();
		//		IPacket packet = IPacket.make();
		//      while (container.readNextPacket(packet) >= 0 && !packet.isKeyPacket()) {
		//			log.debug("Looking for key packet..");        	
		//      }
		//      packet.delete();

		// track start time
		startTime = System.currentTimeMillis();
		// open for business
		closed = false;
		//
		int packetsRead = 0;
		// error holder
		//TODO: [#1001] determine how we're bringin this funcitonality for IError err = null;
		//TODO: [#1003] where is the packet? let's make it?
		MediaPacket packet = MediaPacket.make();
		try {
			// packet read loop
			if (log.isTraceEnabled()) {
				while (reader.read(packet) > 0) {
					//TODO: [#1002] determine if dropping this metric under isTrace is more appropriate ->
					long elapsedMillis = (System.currentTimeMillis() - startTime);
					log.trace("Reads - frames: {} samples: {}", videoFramesRead, audioSamplesRead);
					log.trace("Reads - packets: {} elapsed: {} ms", packetsRead++, elapsedMillis);
				}
			}else{
				while (reader.read(packet) > 0) {
					//do nothing, for great speed and profit!
				}
				log.trace("Done demuxing");
			}
		} catch (Throwable t) {
			log.warn("Exception closing reader", t);
		}
		log.trace("End of reader loop");
		stop();
		log.trace("RTMPReader - end");
	}

	public void onAudioSamples(MediaAudio audio) {
		log.trace("Reader onAudioSamples");
		if (audioEnabled) {
			// increment our count
			audioSamplesRead += audio.getNumSamples();
			// pass the even up the chain
			//TODO: [#1004] implement? super.onAudioSamples(event);
		}
	}

	public void onVideoPicture(MediaPicture picture) {
		log.trace("Reader onVideo");
		if (videoEnabled) {
			// look for a key frame
			keyFrameReceived = picture.isKey() ? true : keyFrameReceived;
			// once we have had one, proceed
			if (keyFrameReceived) {
				videoFramesRead += 1;
				//TODO: #[1006] implement? super.onVideoPicture(event);
			}
		}
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
