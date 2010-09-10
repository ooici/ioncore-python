#!/usr/bin/env python


import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap

CONF = ioninit.config('startup.web')


import gviz_api

from ion.services.dm.presentation import web_service

# Static definition of message queues
ion_messaging = ioninit.get_config('messaging_cfg', CONF)

# Static definition of service names
web_services = ioninit.get_config('services_cfg', CONF)

page_template = """
<html>
  <script src="http://www.google.com/jsapi" type="text/javascript"></script>
  <script>
    google.load('visualization', '1', {packages:['table']});

    google.setOnLoadCallback(drawTable);
    function drawTable() {
      %(jscode)s
      var jscode_table = new google.visualization.Table(document.getElementById('table_div_jscode'));
      jscode_table.draw(jscode_data, {showRowNumber: true});

      var json_table = new google.visualization.Table(document.getElementById('table_div_json'));
      var json_data = new google.visualization.DataTable(%(json)s, 0.6);
      json_table.draw(json_data, {showRowNumber: true});
    }
  </script>
  <body>
    <H1>Table created using ToJSCode</H1>
    <div id="table_div_jscode"></div>
    <H1>Table created using ToJSon</H1>
    <div id="table_div_json"></div>
  </body>
</html>
"""

line_template = '''
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <meta http-equiv="content-type" content="text/html; charset=utf-8"/>
    <title>
      Google Visualization API Sample
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
                        width: 500, height: 400,
                        vAxis: {maxValue: 10}}
                );
      }
      

      google.setOnLoadCallback(drawVisualization);
    </script>
  </head>
  <body style="font-family: Arial;border: 0 none;">
    <div id="visualization" style="width: 500px; height: 400px;"></div>
  </body>
</html>
'''


def simple_gviz():
    # Creating the data
    description = {"name": ("string", "Name"),
                 "salary": ("number", "Salary"),
                 "full_time": ("boolean", "Full Time Employee")}
    data = [{"name": "Mike", "salary": (10000, "$10,000"), "full_time": True},
          {"name": "Jim", "salary": (800, "$800"), "full_time": False},
          {"name": "Alice", "salary": (12500, "$12,500"), "full_time": True},
          {"name": "Bob", "salary": (7000, "$7,000"), "full_time": True}]
    
    # Loading it into gviz_api.DataTable
    data_table = gviz_api.DataTable(description)
    data_table.LoadData(data)
    
    # Creating a JavaScript code string
    jscode = data_table.ToJSCode("jscode_data",
                               columns_order=("name", "salary", "full_time"),
                               order_by="salary")
    # Creating a JSon string
    json = data_table.ToJSon(columns_order=("name", "salary", "full_time"),
                           order_by="salary")
    
    # Putting the JS code and JSon string into the template
    string = "Content-type: text/html\n"
    string += page_template % vars()
    return string
    
def line_gviz():
    # Creating the data
    description = {"name": ("string", "Name"),
                 "salary": ("number", "Salary")}
    data = [{"name": "Mike", "salary": (10000, "$10,000")},
          {"name": "Jim", "salary": (800, "$800")},
          {"name": "Alice", "salary": (12500, "$12,500")},
          {"name": "Bob", "salary": (7000, "$7,000")}]
    
    # Loading it into gviz_api.DataTable
    data_table = gviz_api.DataTable(description)
    data_table.LoadData(data)
    
    # Creating a JavaScript code string
    jscode = data_table.ToJSCode("jscode_data",
                               columns_order=("name", "salary"),
                               order_by="salary")
    # Creating a JSon string
    json = data_table.ToJSon(columns_order=("name", "salary"),
                           order_by="salary")
    
    # Putting the JS code and JSon string into the template
    #string = "Content-type: text/html\n"
    string = line_template % vars()
    return string





@defer.inlineCallbacks
def main():
    """Main function of bootstrap. Starts system with static config
    """
    logging.info("ION SYSTEM bootstrapping web service now...")
    startsvcs = []
    startsvcs.extend(web_services)

    sup = yield bootstrap.bootstrap(ion_messaging, startsvcs)
    
    
    wc = web_service.WebServiceClient(proc=sup)
    yield wc.set_string(line_gviz())

    # Pull page to make sure it got there
    #page = yield client.getPage('http://127.0.0.1:2100/')

main()