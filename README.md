cascading-jobs
==============

The following repository contains a set of cascading jobs.

## Setup

you must have the following componants running on your localhost :
  * elasticsearch
  * hbase

Cucumber tests are not yet integrated to main build, due to the following [issue](https://github.com/cucumber/cucumber-jvm/issues/468).
Thereby you have to launch the goal manually :

```sh
gradle cucumber
```

Running test will create sample input files into "/tmp/cucumber-test"
You can also use directly create a jar in build/libs/cascading-jobs and launch the job manually :

```sh
gradle jar
java -jar build/libs/cascading-jobs.jar /tmp/cucumber-test/input
```
