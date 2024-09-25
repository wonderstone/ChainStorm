## ChainStorm(星云锁链)

A personal graph database using JSON input. 

Graph has vertex(node) and edge component type.

Node has unique name

### Vertex(Node):  
1. Mandatory Part: ID(can be app generated),Collection,Name      
2. Optional Part: Data


### Edge/EdgeJSON
1. Mandatory Part: ID(can be app generated),Collection,From,To,Relationship   
2. Optional Part: Data



#### Functional Purposes:
1. User can Add New info(node,vertex) and edge to the graph
2. User can Update the info store in the node or edge
3. User can lookup the info by ID or Node.Name to locate the ID（1 to 1）
4. Simply store in Json format either by local file （or ArangoDB）
5. Regexp to search the Node name and return the ID

The key query I can think of right now is given a piece of info, and exact several items, to check if some item is in the Graph and give related nodes(in some range and limited edges)

Make the info update continuously, maybe by some crawler or pdf(financial statement) processor based on the LLM, users would have their own local knowledge graph, for confideniallity reasons that may be concerned in Investment

let's see if it can further serve the llm as local knowledge