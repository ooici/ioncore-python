There are two web servers defined within this directory.

Notify Web Monitor
------------------
The first web service is the notify_web_monitor web server.
It can be used to register for and monitor system events
such as lifecycle state changes, etc.  This web server
is started by running:

bin/twistd -n cc res/deploy/notifywebmonitor.rel [notifytest.py]

where notifytest.py is an optional test script that publishes a new event
every 5 seconds

Once the web server has started, point your browser to:
http://<host running web server>:9999


Instrument Sample Data Web Monitor
----------------------------------
The second web service is used specifically to demonstrate
the publishing of instrument sample data.  This web server
is started by running:

bin/twistd -n cc res/deploy/r1deploy.rel [ion/services/dm/distribution/test/instrument_notifytest.py]

where instrument_notifytest.py is an optional test script that publishes
a new instrument sample data event every 5 seconds.

Once the web server has started, point your browser to:
http://<host running web server>:9998

Note: The web page for the instrument sample data monitoring relies on CSS
served up by the official OOICI web server machine "ion.oceanobservatories.org".
Check the sanity of that machine if styles do not properly render
in the web page.