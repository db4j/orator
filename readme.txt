Orator is a client-server library for running remote tasks
- udp with reliability
- classpath bytecode is automatically sent to the client
  ie, server doesn't need to include client classpath



Orator:
  Loadee needs to be built (it's accessed thru the filesystem by name)
  runs a client and a server


Loader.Proxy is a classloader that uses the parent classloader to find the bytecode
  but then defines the class itself (instead of delegating to the parent)


running client-server (2 consoles in dirs Orator and Loadee):
  mvn package exec:java -Dexec.mainClass=com.nqzero.orator.OratorUtils
  mvn package exec:java -Dexec.mainClass=loadee.Obama
  output from Orator:
    Obama::stdout:1 -- result: Biden: bitter people and their guns, count:1
  output from Obama
    roundtrip completed -- hello world ... Obama::stdout:1 -- result: Biden: bitter people and their guns, count:1



running Orator (with no args) output should be:
  ...
  roundtrip completed -- hello world ... Obama::stdout:96 -- result: Biden: bitter people and their guns, count:96
  roundtrip completed -- hello world ... Obama::stdout:97 -- result: Biden: bitter people and their guns, count:97
  roundtrip completed -- hello world ... Obama::stdout:98 -- result: Biden: bitter people and their guns, count:98
  Obama::stdout:100 -- result: Biden: bitter people and their guns, count:99
  Obama::stdout:99 -- result: Biden: bitter people and their guns, count:100
  roundtrip completed -- hello world ... Obama::stdout:100 -- result: Biden: bitter people and their guns, count:99
  roundtrip completed -- hello world ... Obama::stdout:99 -- result: Biden: bitter people and their guns, count:100
  dumpLog -- log dumped, waiting ...
  dump::count 100, 1676
  



