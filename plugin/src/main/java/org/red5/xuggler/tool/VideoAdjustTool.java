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

package org.red5.xuggler.tool;

import io.humble.video.MediaPicture;
import io.humble.video.MediaPictureResampler;
import io.humble.video.PixelFormat.Type;

import org.red5.service.httpstream.SegmentFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Video frame dimension adjustment.
 * 
 * @author Paul Gregoire (mondain@gmail.com)
 */
public class VideoAdjustTool implements GenericTool {

	private Logger log = LoggerFactory.getLogger(VideoAdjustTool.class);

	private MediaPictureResampler resampler = null;

	private int width;

	private int height;

	private Type pixelType = Type.PIX_FMT_YUV420P;

	private SegmentFacade facade;

	public VideoAdjustTool(int width, int height) {
		log.trace("Video width: {} height: {}", width, height);
		this.width = width;
		this.height = height;
	}

	@Override
	public void onVideoPicture(MediaPicture in) {
		log.debug("Adjust onVideo");
		log.debug("Video ts: {}", in.getFormattedTimeStamp());
		int inWidth = in.getWidth();
		int inHeight = in.getHeight();
		if (inHeight != height || inWidth != width) {
			log.debug("VideoAdjustTool onVideoPicture");
			//log.trace("Video timestamp: {} pixel type: {}", event.getTimeStamp(), in.getPixelType());
			log.trace("Video in: {} x {} out: {} x {}", new Object[] { inWidth, inHeight, width, height });
			if (resampler == null) {
				resampler = MediaPictureResampler.make
						//(width, height, pixelType, in.getWidth(), in.getHeight(), in.getFormat());
						//(width, height, pixelType, inWidth, inHeight, in.getFormat());
				log.debug("Video resampler: {}", resampler);
			}
			if (resampler != null) {
				MediaPicture out = MediaPicture.make(pixelType, width, height);
				if (resampler.resample(out, in) >= 0) {
					//check complete
					if (out.isComplete()) {
						// queue video
						facade.queueVideo(out, event.getTimeStamp(), event.getTimeUnit());
						in.delete();
					} else {
						log.warn("Resampled picture was not marked as complete");
					}
				} else {
					log.warn("Resample failed");
				}
				out.delete();
			} else {
				log.debug("Resampler was null");
			}
			log.debug("VideoAdjustTool onVideoPicture - end");
		} else {
			// queue video
			facade.queueVideo(in, event.getTimeStamp(), event.getTimeUnit());
		}
	}

	/* (non-Javadoc)
	 * @see com.xuggle.mediatool.MediaToolAdapter#onClose(com.xuggle.mediatool.event.ICloseEvent)
	 */
	@Override
	public void onClose(ICloseEvent event) {
		close();
		super.onClose(event);
	}

	public void close() {
		if (resampler != null) {
			resampler.delete();
		}
	}

	/**
	 * @param facade the facade to set
	 */
	public void setFacade(SegmentFacade facade) {
		this.facade = facade;
	}

}
