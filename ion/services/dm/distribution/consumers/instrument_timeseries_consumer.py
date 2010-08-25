#!/usr/bin/env python

"""
@file ion/services/dm/distribution/consumers/instrument_timeseries_consumer.py
@author David Stuebe
@brief Plot a time series output to a consumer as a dap grid dataset
http://code.google.com/apis/visualization/documentation/index.html
"""

from ion.services.dm.distribution import base_consumer

from ion.core.base_process import ProtocolFactory

import logging
logging = logging.getLogger(__name__)
import gviz_api
from ion.util import procutils as pu


line_template = '''
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <meta http-equiv="content-type" content="text/html; charset=utf-8"/>
    <title>
      OOICI Message Count Consumer Visualization
    </title>
    <script type="text/javascript" src="http://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load('visualization', '1', {packages: ['corechart']});
    </script>
    <script type="text/javascript">
      function drawVisualization() {
        // Create and populate the data table.
        var data = new google.visualization.DataTable(%(json)s);
       
        // Create and draw the visualization.
        new google.visualization.LineChart(document.getElementById('visualization')).
            draw(data, {curveType: "function",
                        width: 800, height: 600,
                        vAxis: {maxValue: 10}}
                );
      }
      

      google.setOnLoadCallback(drawVisualization);
    </script>
    <script language="JavaScript">
    <!--
    
    var sURL = unescape(window.location.pathname);
    
    function doLoad()
    {
        // the timeout value should be the same as in the "refresh" meta-tag
        setTimeout( "refresh()", 2*1000 );
    }
    
    function refresh()
    {
        //  This version of the refresh function will cause a new
        //  entry in the visitor's history.  It is provided for
        //  those browsers that only support JavaScript 1.0.
        //
        window.location.href = sURL;
    }
    //-->
    </script>
    
    <script language="JavaScript1.1">
    <!--
    function refresh()
    {
        //  This version does NOT cause an entry in the browser's
        //  page view history.  Most browsers will always retrieve
        //  the document from the web-server whether it is already
        //  in the browsers page-cache or not.
        //  
        window.location.replace( sURL );
    }
    //-->
    </script>
    
    <script language="JavaScript1.2">
    <!--
    function refresh()
    {
        //  This version of the refresh function will be invoked
        //  for browsers that support JavaScript version 1.2
        //
        
        //  The argument to the location.reload function determines
        //  if the browser should retrieve the document from the
        //  web-server.  In our example all we need to do is cause
        //  the JavaScript block in the document body to be
        //  re-evaluated.  If we needed to pull the document from
        //  the web-server again (such as where the document contents
        //  change dynamically) we would pass the argument as 'true'.
        //  
        window.location.reload( false );
    }
    //-->
</script>
    
    
  </head>
  <body onload="doLoad()" style="font-family: Arial;border: 0 none;">
    <div id="visualization" style="width: 800px; height: 600px;"></div>
    %(msg)s
  </body>
</html>
'''



class InstrumentTimeseriesConsumer(base_consumer.BaseConsumer):
    """
    Plot a Dap GridType timeseries
    """
    def customize_consumer(self):
        self.pdata=[]
    
    def ondata(self, data, notification, timestamp, queue='', max_points=15, **kwargs):
        
        vals = data.split(',')
        print 'VALS:',vals
        
        description = [('v1','number', 'value1'),
                        ('v2','number', 'value2'),
                        ('v3','number', 'value3'),
                        ('v4','number', 'value4')
                       ]
        
        self.pdata.append([float(vals[0]),float(vals[1]),float(vals[2]),float(vals[3])])
        
        dlen = len(self.pdata)
        if dlen > max_points:
            self.pdata = self.pdata[dlen-max_points : ]
        
        data_table = gviz_api.DataTable(description)
        data_table.LoadData(self.pdata)
        #json = data_table.ToJSon(columns_order=("name", "salary"),order_by="salary")
        json = data_table.ToJSon()
            
        # Make message for the screen below
        msg = '<p>Timestamp: %s </p>\n' % pu.currenttime()
        
        page = line_template % {'msg':msg,'json':json}
            
        self.queue_result(queue,page,'Google Viz of message counts')

        

# Spawn of the process using the module name
factory = ProtocolFactory(InstrumentTimeseriesConsumer)
