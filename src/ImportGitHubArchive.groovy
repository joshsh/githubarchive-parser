import com.thinkaurelius.titan.core.*
import org.apache.commons.configuration.BaseConfiguration
import java.text.DateFormat;
import java.text.SimpleDateFormat;


/** 
 * Reads in triplet files
 * and loads into Titan graph
 * @author Vadas Gintautas
 * @author Joshua Shinavier
 *
 *
 * Configuration option are here 
 */

// e.g. 2014-06-01T12:07:24-07:00
DateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

useCassandra = true // if false, use HBase instead
useHBase = false  //if false, use BerkleyDB instead
graphLocation = '../../../scratch/gha-graph' //only for BerkleyDB

//get inputFolder as command line argument
try {
    inputVerticesFile = a1
    inputEdgesFile = a2
} catch (MissingPropertyException) {
    throw new IllegalArgumentException('\n\nusage: gremlin -e ImportGitHubArchive.groovy <inputVertices> <inputEdges>\n')
}

class Counter {
    private int myCount
    private double now
    private double last

    public Counter(start){
        this.myCount = 0
        this.last = start
    }
    
    public void increment() {
        this.myCount++
        if (this.myCount % 1000000 == 0){
            now = System.currentTimeMillis() 
            println this.myCount + ' in ' + ((now - this.last)/1000.0) + ' seconds'
            this.last = now
        }
    }
    public void reset() {
        this.myCount = 0
    }
    public int getCount() {
        return this.myCount
    }
}

//parses triple
def vertexAdder = {g, counter, line ->
    line = line.split('\t')
    if (line.size() < 3) return

    //vertexId = line[0]
    //assert(line[1].startsWith('_'))
    //key = line[1][1..-1]
    //value = line[2]
    
    try {
        if (! line[0].equals(lastVertexId))  {
            vertex=g.addVertex(line[0])
            lastVertexId = line[0]
            lastVertex = vertex
            counter.increment()
        }
        def key = line[1][1..-1]
        def value = line[2]
        if (key.equals("created_at")) {
            value = TIMESTAMP_FORMAT.parse(value).getTime()	
        }
        lastVertex.setProperty(key,value)
    }catch (e){
        println e
        println line
    }
}

//parses triple
def edgeAdder = {g, counter, line ->
    line = line.split('\t')
    if (line.size() < 3){
        return
    }
    try{
        outVertexId = line[0]
        label = line[1]
        inVertexId = line[2]
        edge = g.addEdge(null,g.getVertex(outVertexId),g.getVertex(inVertexId),label)
        if (line.size() > 3 ){
            for (l in line[3..-1]) {
            	def key = l.split('=')[0]
		def value = l.split('=')[1]
		if (key.equals("created_at")) {
	            value = TIMESTAMP_FORMAT.parse(value).getTime()	
		}
                edge.setProperty(key,value)
            }
        }
        counter.increment()
    }catch (e){
        println e
        println line
    }
}


conf = new BaseConfiguration()

conf.setProperty('storage.keyspace','github')

if (useCassandra) {
    conf.setProperty('storage.backend','cassandra')
    conf.setProperty('storage.hostname','localhost')
    conf.setProperty("storage.batch-loading","true")
    baseGraph = TitanFactory.open(conf)
    baseGraph.createKeyIndex('name',Vertex.class)

    baseGraph.makeKey(IdGraph.ID).single().unique().indexed(Vertex.class).indexed(Edge.class).dataType(String.class).make()

    // edge-specific keys
    for (key in ["github_type", "payload", "public", "url"]) {
        if (null == baseGraph.getType(key)) {
            baseGraph.makeKey(key).dataType(String.class).make();
        }
    }
    // vertex keys
    for (key in ["action", "actor", "blog", "comment", "comment_id", "commit", "company", "description", "email", "fork", "forks", "github_id", "github_name", "github_type", "gravatar_id", "has_downloads", "has_issues", "has_wiki", "homepage", "issue", "language", "location", "login", "master_branch", "name", "number", "open_issues", "organization", "owner", "page_name", "payload", "private", "pushed_at", "sha", "size", "stargazers", "title", "type", "url", "watchers"]) {
        if (null == baseGraph.getType(key)) {
            baseGraph.makeKey(key).dataType(String.class).make();
        }	
    }
    // special keys
    if (null == (createdAt = baseGraph.getType("created_at"))) {
        createdAt = baseGraph.makeKey("created_at").dataType(Long.class).make();
    }
    // labels
    for (label in ["added", "created", "deleted", "edited", "forked", "of", "on", "owns", "pullRequested", "pushed", "to", "watched"]) {
        if (null == baseGraph.getType(label)) {
	    baseGraph.makeLabel(label).sortKey(createdAt).sortOrder(Order.DESC).make();
        }
    }

    baseGraph.commit()

    idGraph = new IdGraph(baseGraph, true, true)
    baseGraph = idGraph
    
} else if (useHBase){
    conf.setProperty('storage.backend','hbase')
    conf.setProperty('storage.tablename','titan-big')
    conf.setProperty('storage.hostname','localhost')
    conf.setProperty('persist-attempts',20)
    //conf.setProperty('persist-wait-time',400)
    conf.setProperty('storage.lock-retries',20)
    conf.setProperty('storage.idauthority-block-size',10000000)
    conf.setProperty('storage.idauthority-retries',20)

    //conf.setProperty('storage.transactions','false')
    
    conf.setProperty("storage.batch-loading","true")
    baseGraph = TitanFactory.open(conf)
    baseGraph.createKeyIndex('name',Vertex.class)
    baseGraph.commit()
}else{  //use BerkeleyDB

    graphFile = new File(graphLocation)
    if (graphFile.exists()) assert graphFile.deleteDir()
    assert graphFile.mkdir()

    conf.setProperty('storage.backend','local')
    conf.setProperty('storage.cache_percentage','60')
    conf.setProperty('storage.directory',graphLocation)
    //conf.setProperty('buffer-size',1024)
    
    //conf.setProperty('storage.transactions','false')
    
    conf.setProperty("storage.batch-loading","true")
    baseGraph = TitanFactory.open(conf)
    baseGraph.createKeyIndex('name',Vertex.class)
    baseGraph.commit()
}

graph = new BatchGraph(baseGraph, VertexIDType.STRING, 25000)
graph.setLoadingFromScratch(true)

start = System.currentTimeMillis() 
counter = new Counter(start)
lastVertexId = null
lastVertex = null

try{
    println 'Loading vertices'
    myFile = new File(inputVerticesFile).eachLine {line -> vertexAdder(graph,counter, line)}
    vertexCount = counter.getCount()
    counter.reset()
    println 'Loading edges'
    myFile = new File(inputEdgesFile).eachLine {line ->edgeAdder(graph, counter, line)}
    edgeCount = counter.getCount()
} finally{
    //sun.management.ManagementFactory.getDiagnosticMXBean().dumpHeap('dump.bin', true)
    graph.shutdown()
    //baseGraph.shutdown()
}



now = System.currentTimeMillis()  
elapsed =  ((now - start)/1000.0)
println 'Done.  Statistics:'
println vertexCount + ' vertices'
println edgeCount + ' edges'
println elapsed + ' seconds elapsed'
