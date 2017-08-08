"""
The below script is heavily based on reference from https://gist.github.com/matthiaseisen/3278cedcd53afe62c3f3
"""

from pymongo import MongoClient
import graphviz as gv
import functools
import json

def add_nodes(graph, nodes):
    for n in nodes:
        if isinstance(n, tuple):
            graph.node(n[0], **n[1])
        else:
            graph.node(n)
    return graph


def add_edges(graph, edges):
    for e in edges:
        if isinstance(e[0], tuple):
            graph.edge(*e[0], **e[1])
        else:
            graph.edge(*e)
    return graph

def apply_styles(graph, styles):
    graph.graph_attr.update(
        ('graph' in styles and styles['graph']) or {}
    )
    graph.node_attr.update(
        ('nodes' in styles and styles['nodes']) or {}
    )
    graph.edge_attr.update(
        ('edges' in styles and styles['edges']) or {}
    )
    return graph
	
def id_filter(id):

	filtered_id=str(id)
	if id is None:
		filtered_id='null'
	
	return filtered_id

	
def get_graph_node_edge(document):
	
	# setup local reference to tree nodes and edges
	global tree_nodes
	global tree_node_edges
	
	# initialize the node attribute
	nodeAttribute={'label': id_filter(document['_id'])}
	
	# add available versions: draft, published, library (associated with active versions)
	if u'versions' in document:
		
		nodeAttribute['fillcolor']='#4caf50'
		
		if u'draft-branch' in document['versions']:
			edge = ((id_filter(document['_id']), id_filter(document['versions']['draft-branch'])), {'label': 'draft'})
			tree_node_edges.append(edge)
		
		if u'published-branch' in document['versions']:
			edge = ((id_filter(document['_id']), id_filter(document['versions']['published-branch'])), {'label': 'published'})
			tree_node_edges.append(edge)
		
		if u'library' in document['versions']:
			edge = ((id_filter(document['_id']), id_filter(document['versions']['library'])), {'label': 'library'})
			tree_node_edges.append(edge)
	
	# QQ: are these the only attributes we care about?
	# handle previous & original version
	if u'previous_version' in document:
		
		# skip the null reference node (it complicates the graph)
		if document['previous_version'] is not None:
		
			edge = ((id_filter(document['_id']), id_filter(document['previous_version'])), {'label': 'previous'})
			tree_node_edges.append(edge)
		else:
			# instead of adding the null edge, let's visually style the null reference node
			nodeAttribute['fillcolor']='#ff0000'
			
			
	# add the node: the actual addition
	tree_nodes.append((id_filter(document['_id']), nodeAttribute))
	
	# handle original version
	if u'original_version' in document:
        edge = ((id_filter(document['_id']), id_filter(document['original_version'])), {'label': 'original'})
        tree_node_edges.append(edge)

"""
MAIN OPERATIONS
"""

# indicator of whether or not use live data
use_live_data = False

# Tree style
styles = {
    'graph': {
        'fontsize': '6',
        'fontcolor': 'white',
        'bgcolor': '#333333',
        'rankdir': 'TB',
    },
    'nodes': {
        'fontname': 'Helvetica',
        'shape': 'ellipse',
        'fontcolor': 'white',
        'color': 'white',
        'style': 'filled',
        'fillcolor': '#006699',
    },
    'edges': {
        'style': 'dashed',
        'color': 'white',
        'arrowhead': 'open',
        'fontname': 'Courier',
        'fontsize': '10',
        'fontcolor': 'white',
    }
}

graph = functools.partial(gv.Graph, format='svg')
digraph = functools.partial(gv.Digraph, format='svg')

# collection of all available tree nodes
tree_nodes = []
tree_node_edges = []

if use_live_data == True:

    # use a local database as the source
    client = MongoClient()
    db = client.edxapp

    modulestore_activeversion_collection = db.modulestore.active_versions
    modulestore_structures_collection = db.modulestore.structures

    # Query all active versions and setup as nodes
    for document in modulestore_activeversion_collection.find({}):
        get_graph_node_edge(document)

    # Query all structures
    for document in modulestore_structures_collection.find({}):
        get_graph_node_edge(document)
else:

    # use a static dataset
    with open('dataset.json') as data_file:    
        dataset = json.load(data_file)

    for document in dataset:
        get_graph_node_edge(document)


# Generate the graph
print ("Generating graph. Adding edges...")
courseTree = add_edges( add_nodes(digraph(), tree_nodes ), tree_node_edges )

print ("Graphing: Added Edges. Adding Styles...")
courseTree = apply_styles(courseTree, styles)

print ("Graphing: Added Styles. Rendering output")
courseTree.render('img/coursetree')
