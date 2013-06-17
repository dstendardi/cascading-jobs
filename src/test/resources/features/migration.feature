Feature: Migration from hbase to hdfs

  @hbase
  Scenario: Extract various formats from hbase and put them in hdfs
    Given a table "events" containing the following cells
      | row                                          | family | qualifier            | timestamp     | value                                            |
      | 1-pool-apache-1365783845643-myhost-573df45f  | system | p-http_access-json-1 | 1365783845643 | com/viadeo/cascading/log/json/sample_apache.json |
      | 1-pool-unknown-1365783845643-myhost-573df45c | system | p-syslog-json-1      | 1365783845643 | com/viadeo/cascading/log/json/sample_syslog.json |
    And a target output directory
    When I run the migration job with the following arguments
      | start date    | end date      | prefixes                 |
      | 1365783845642 | 1365783845644 | pool-apache,pool-unknown |
    Then output "/http_access/1/2013/2013-04-12" should contains the following tab separated values
      | 573df45f | 1365783845643 | - | /foo | ?ts=1369824779796 | HTTP/1.1 | http://www.boo.com/fr/company | Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36 | 196.217.151.196, 93.184.214.111, 46.22.78.50 | 53059F00AF23EDB8BD3FE4C4119BC919 | 0031bysgg6zayraw | 200 | 4157 | 26 | text/plain |
    And output "/syslog/1/2013/2013-04-12" should contains the following tab separated values
      | 573df45c | 1365783845643 | 1 | 1 | boo |