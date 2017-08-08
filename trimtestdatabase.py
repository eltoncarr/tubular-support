
from os import path
from pymongo import MongoClient
from bson.objectid import ObjectId

def get_active_version_filter(active_version_id_list):

    # TODO: account for no filter
    av_filter = { '$in' : [] }

    for active_version_id in active_version_id_list.split(","):
        av_filter['$in'].append(ObjectId(active_version_id.strip()))

    # establish the query filter  (respecting cases where no value is specified)
    return get_query_filter(av_filter)


def get_query_filter(doc_filter):

    # establish the query filter  (respecting cases where no value is specified)
    query_filter = None

    if len(doc_filter['$in']) > 0:
        query_filter = { '_id' : doc_filter }

    return query_filter

def get_database(connection=None):

    if connection is None:
        client = MongoClient()
    else:
        client = MongoClient(connection)
    
    return client.edxapp 




def get_structures_filter(active_version_list):

    # TODO: account for no filter
    structure_filter = { '$in' : [] }

    # these are relevant keys
    # TODO: move to higher position for others to use it
    target_keys = [u'library', u'draft-branch', u'published-branch']

    for active_version in active_version_list:

        for target_key in target_keys:    
            if target_key in active_version['versions']:
                structure_filter['$in'].append(ObjectId(active_version['versions'][target_key]))

    # establish the query filter  (respecting cases where no value is specified)
    return get_query_filter(structure_filter)


def get_structures(db, candidate_list, structure_tree, iteration=0):

    fields = {
        "_id": 1,
        "previous_version": 1,
        "original_version": 1
    }

    objectid_list = []

    # build the query filter
    for candidate in candidate_list:
        objectid_list.append(ObjectId(candidate))
        structure_tree.append(candidate)

    query_filter = {'_id': { '$in': objectid_list}}

    # run the query
    result_set = db.modulestore.structures.find(query_filter, fields)

    query_target = []
    for doc in result_set:

        if doc[u'previous_version'] is not None:
            value = str(doc[u'previous_version'])
            query_target.append(value)
            
            
        if doc[u'original_version'] is not None:
            value = str(doc[u'original_version'])
            query_target.append(value)

    if doc[u'previous_version'] is None:
        return structure_tree

    iteration += 1
    print "Iteration {0} looking up {1} next".format(iteration, query_target)

    return get_structures(db, query_target, structure_tree, iteration)

##############################################################
# START
##############################################################

db = get_database()

course_library_list = "583602b0e9ec21ec98727b81"
active_version_filter = get_active_version_filter(course_library_list)

# get the target item active versions
fields = {
        "versions.draft-branch": 1, 
        "versions.published-branch": 1,
        "versions.library": 1, 
    }

# initialize our active versions dictionary
active_versions = {'versions': [] }
required_structures = []

resultset = db.modulestore.active_versions.find({}, fields).limit(5)

structure_tree = []

for active_version_doc in resultset:

        # collect all interesting docs: library & [draft|published]-branch active versions
        avdocs_versions = active_version_doc['versions']

        if u'library' in avdocs_versions or u'draft-branch' in avdocs_versions or u'published-branch' in avdocs_versions:

            if u'library' in avdocs_versions:
                objectId = str(avdocs_versions[u'library'])
                required_structures.append(objectId)

            if u'draft-branch' in avdocs_versions:
                objectId = str(avdocs_versions[u'draft-branch'])
                required_structures.append(objectId)

            if u'published-branch' in avdocs_versions:
                objectId = str(avdocs_versions[u'published-branch'])
                required_structures.append(objectId)


# iteratively pull the structures
structure_tree = get_structures(db, required_structures, structure_tree)
final_list = list(set(structure_tree))

objectid_list = []

for item in final_list:
    objectid_list.append(ObjectId(item))


db.modulestore.structures.remove({ '_id': {'$nin': objectid_list}})



# purge everything not targeted
#db.modulestore.structures.remove({ '_id': {'$nin': final_list}})
# rlll