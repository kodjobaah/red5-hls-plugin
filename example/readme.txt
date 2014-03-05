Example HLS application


The build requires that the hls-plugin jar be placed in a known location. The maven pom expects "hls-plugin-1.1.jar" to be in the example/lib directory.
To run the application you will need to put the following jars in in the plugin folder of your red5 installation:

hls-plugin-1.1.jar  
humble-video-arch-x86_64-pc-linux-gnu6-0.1-SNAPSHOT.jar  
humble-video-noarch-0.1-SNAPSHOT.jar 

You will also need to modify the following in red5-web.xml:

<property name="segmentDirectory" value="/usr/local/src/red5-read-only/target/red5-server-1.0.2-RC4/webapps/hlsapp/WEB-INF/segments/" />


