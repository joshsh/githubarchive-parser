import groovy.json.JsonSlurper
import groovy.json.JsonBuilder

/** 
 * Reads in uncompressed githubarchive json files in a specified directory
 * and creates lists of vertices and edges
 * @author Vadas Gintautas
 *
 *
 * Configuration option are here 
 */


//get inputFolder as command line argument
try {
    inputFolder = a1
    verticesFileName = a2
    edgesFileName = a3
}
catch (MissingPropertyException) {
    throw new IllegalArgumentException('\n\nusage: gremlin -e ParseGitHubArchive.groovy <inputFolder> <verticesFileName> <edgesFileName> \n')
}

verticesFile = new File(verticesFileName)
edgesFile = new File(edgesFileName)

if (verticesFile.exists()) {
    assert verticesFile.delete()
    assert verticesFile.createNewFile()
}

if (edgesFile.exists()) {
    assert edgesFile.delete()
    assert edgesFile.createNewFile()
}


verticesFileStream = new FileWriter(verticesFileName,true)
edgesFileStream = new FileWriter(edgesFileName,true)

vBuf = new BufferedWriter(verticesFileStream)
eBuf = new BufferedWriter(edgesFileStream)


slurper = new JsonSlurper()
eventCount = 0
vertexCount = 0
edgeCount = 0

def vertexRecorder = {name, type, properties ->
    if (name==null) throw new IllegalArgumentException('Name cannot be null')
    if (type==null) throw new IllegalArgumentException('Type cannot be null')

    name = name.toString()
    vertexId = name+'_'+type
    propertiesJson = new JsonBuilder(properties)

    out = vertexId + '\t' + propertiesJson.toString() +'\n'
    vBuf.write(out)
    vertexCount = vertexCount + 1
    return vertexId
}


def edgeRecorder = {outVertex, inVertex, label, properties->
    if (label==null) throw new IllegalArgumentException('Label cannot be null')
    
    propertiesJson = new JsonBuilder(properties)
    out = outVertex + '\t' + inVertex + '\t' + label + '\t' + propertiesJson.toString() +'\n'
    eBuf.write(out)
    edgeCount = edgeCount + 1
}


def parser = {line -> 
    s = slurper.parseText(line)
    if (s.actor == null) return

    /**
     * Out vertex is always a user.
     * Occasionally actor_attributes is missing so we allow for this.
     */
    actorProperties = ['actor':s.actor]
    if (s.actor_attributes != null) actorProperties = actorProperties +  s.remove('actor_attributes')
    lastVertex=vertexRecorder(s.remove('actor'),'User',actorProperties)


    try {
        repoOrg = s.repository.organization
    }
    catch (NullPointerException) {
        repoOrg = null
    }
            
    edgeLabelMap = [
        'CreateEvent':'created',
        'WatchEvent':'watched',
        'DownloadEvent':'downloaded',
        'DeleteEvent':'deleted',
        'ForkEvent':'forked',
        'ForkApplyEvent':'appliedForkTo',
        'PublicEvent':'madePublic',
        'PullRequestEvent':'pullRequested',
    ]

    switch (s.type){

        /**
         * GistEvent: User created Gist.
         */
        case 'GistEvent':
        
            vertexNames = [s.payload.name]
            vertexTypes = ['Gist']
            vertexProperties = [s.remove('payload')]
            edgeLabels = ['created']
            edgeProperties = [s]
            break
        
        /**
         * FollowEvent: User followed User.
         */
        case 'FollowEvent':
            
            vertexNames = [s.payload.target.login]
            vertexTypes = ['User']
            vertexProperties = [s.remove('payload').remove('target')]
            edgeLabels = ['followed']
            edgeProperties = [s]
            break

        /**
         * MemberEvent, TeamAddEvent: User added User to Repository.
         * No TeamAddEvents observed in entire archive to date.
         * Occasionally repository is missing (here and following cases).
         * If so, ignore the event.
         */
        case ['MemberEvent','TeamAddEvent']:
            if (s.repository == null) return

            vertexNames = [s.payload.member.login,s.repository.name]
            vertexTypes = ['User','Repository']
            vertexProperties = [s.remove('payload').remove('target'),s.remove('repository')]
            edgeLabels = ['added','to']
            edgeProperties = [s,s]
            break

        /**
         * CommitCommentEvent: User created Comment on Repository.
         */
        case 'CommitCommentEvent':
            if (s.repository == null) return
            
            vertexNames = [s.payload.comment_id,s.repository.name]
            vertexTypes = ['Comment','Repository']
            vertexProperties = [s.remove('payload'),s.remove('repository')]
            edgeLabels = ['created','on']
            edgeProperties = [s,s]
            break

        /**
         * IssuesEvent: User created Issue on Repository.
         */
        case 'IssuesEvent':
            if (s.repository == null) return

            vertexNames = [s.payload.issue,s.repository.name]
            vertexTypes = ['Issue','Repository']
            vertexProperties = [s.remove('payload'),s.remove('repository')]
            edgeLabels = ['created','on']
            edgeProperties = [s,s]

            break
        
        /**
         * GollumEvent: User edited WikiPage of Repository.
         */
        case 'GollumEvent':
            if (s.repository == null) return

            pageNames = []
            pageProperties = []
            pageTypes = []
            pageEdgeNames = []
            pageEdgeProperties = []

            for (p in s.payload.pages){
                pageNames.add(p.remove('html_url'))
                pageProperties.add(p)
                pageTypes.add('WikiPage')
                pageEdgeNames.add('edited')
                pageEdgeProperties.add([:])
            }
            

            vertexNames = [pageNames,s.repository.name]
            vertexTypes = [pageTypes,'Repository']
            vertexProperties = [pageProperties, s.remove('repository')]
            edgeLabels = [pageEdgeNames,'of']
            edgeProperties = [pageEdgeProperties,s]

            break

        /**
         * PushEvent: User pushed commit to Repository.
         */
        case 'PushEvent':
            if (s.repository == null) return
            if (s.payload.shas.size() == 0) return

            pageNames = []
            pageProperties = []
            pageTypes = []
            pageEdgeNames = []
            pageEdgeProperties = []

            for (p in s.payload.shas){
                pageNames.add(p[0])
                pageProperties.add(['payload':p.drop(1)])
                pageTypes.add('Commit')
                pageEdgeNames.add('pushed')
                pageEdgeProperties.add(['created_at':s.created_at,'public':s.public,'url':s.url])
            }

            s.remove('payload')
            

            vertexNames = [pageNames,s.repository.name]
            vertexTypes = [pageTypes,'Repository']
            vertexProperties = [pageProperties, s.remove('repository')]
            edgeLabels = [pageEdgeNames,'to']
            edgeProperties = [pageEdgeProperties,s]

            break
        
        
        /**
         * IssuesCommentEvent: User created Comment on Issue on Repository.
         */
        case 'IssueCommentEvent':
            if (s.repository == null) return
            
            vertexNames = [s.payload.comment_id,s.payload.issue_id,s.repository.name]
            vertexTypes = ['Comment','Issue','Repository']
            vertexProperties = [[:],[:],s.remove('repository')]
            edgeLabels = ['created','on','on']
            edgeProperties = [s,s,s]
            break

        /**
         * PullRequestReviewCommentEvent: User created Comment on Repository.
         */
        case 'PullRequestReviewCommentEvent':
            if (s.repository == null) return
            
            vertexNames = [s.payload.comment.commit_id,s.repository.name]
            vertexTypes = ['Comment','Repository']
            vertexProperties = [s.remove('payload'),s.remove('repository')]
            edgeLabels = ['created','on']
            edgeProperties = [s,s]
            break


        /**
         * All other valid cases:  User (edgeLabel) Repository
         */
        case edgeLabelMap.keySet() as List:
            if (s.repository == null) return

            vertexNames = [s.repository.name]
            vertexTypes = ['Repository']
            vertexProperties = [s.remove('repository')]
            edgeLabels = [edgeLabelMap[s.type]]
            edgeProperties = [s]
            break
        

    }

    eventCount = eventCount + 1
    if (eventCount % 1000000 == 0 ) { 
        now = System.currentTimeMillis()
        sec = (now - last)/1000.0
        println sec.toString() +  ' s for ' + eventCount.toString()
        last = now
    }

    for (i in 0..vertexNames.size()-1) {

        if ((vertexTypes[i] == 'Repository') && (repoOrg != null)){
            nextVertex = vertexRecorder(vertexNames[i],vertexTypes[i],vertexProperties[i])
            orgVertex = vertexRecorder(repoOrg,'Organization',[:])
            edgeRecorder(orgVertex,nextVertex,'owns',[:])
        }

        if (vertexNames[i].getClass() == java.util.ArrayList){
            endVertex = vertexRecorder(vertexNames[i+1],vertexTypes[i+1],vertexProperties[i+1])
            for (j in 0..vertexNames[i].size()-1){
                midVertex = vertexRecorder(vertexNames[i][j],vertexTypes[i][j],vertexProperties[i][j])
                edgeRecorder(lastVertex,midVertex,edgeLabels[i][j],edgeProperties[i][j])
                edgeRecorder(midVertex,endVertex,edgeLabels[i+1],edgeProperties[i+1])
            }
            return
        } else{
            nextVertex = vertexRecorder(vertexNames[i],vertexTypes[i],vertexProperties[i])
            edgeRecorder(lastVertex,nextVertex,edgeLabels[i],edgeProperties[i])
            lastVertex = nextVertex
        }
    }
}




start = System.currentTimeMillis() 


baseDir = new File(inputFolder)
fileList = baseDir.listFiles()
fileList.sort()


aCounter = 0;
myFile = new File('/tmp/temp.json')

for (file in fileList){
    fileName = file.toString()
    System.out.println('[' + aCounter++ + ':' + fileList.size() + '] ' + fileName)

    command = 'ruby FixGitHubArchiveDelimiters.rb ' + fileName + ' /tmp/temp.json'
    process = command.execute()
    process.waitFor()
    myFile.eachLine {line ->parser(line)}
}



vBuf.close()
eBuf.close()

now = System.currentTimeMillis()  
elapsed =  ((now - start)/1000.0)
println 'Done.  Statistics:'
println eventCount + ' events'
println vertexCount + ' vertices'
println edgeCount + ' edges'
println elapsed + ' seconds elapsed'
