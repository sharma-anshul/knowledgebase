import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Search
from redis import Redis

from rank import Ranker


class KnowledgeBase(object):
    """ 
    Represents interface to Knowledge Base.

    The Knowledge base uses Elasticsearch to provide primary storage and
    indexing for the articles. It leverages the default analyzer for text
    preprocessing at indexing time.
    
    View counts for articles are stored in Redis. It could be a field in the
    ES mapping for the article but that would mean reindexing the document
    everytime an article is viewed. To alleviate this unnecessary burden on
    the primary storage, the knowledge base uses an in memory key value store
    for fast access as well as update time. 

    The class encapsulates interactions with ElasticSearch including initializing
    indices and mappings.
    """

    # ES Index and Type name.
    INDEX = 'articles'
    TYPE = 'article'
    
    # Path to JSON mappings for Index and Type.
    INDEX_PATH = 'mappings/index_mapping.json'
    TYPE_PATH = 'mappings/type_mapping.json'

    # Configuration items for initializing connections with
    # databases. Ideally, these would be stored separately
    # in YAML format or managed using something like
    # Zookeeper.
    HOSTS = ['localhost']
    USERNAME = 'elastic'
    PASSWORD = 'changeme'

    def __init__(self):
        # Initialize persistent connections to ES and Redis.
        self.client = Elasticsearch(
            hosts=self.HOSTS,
            http_auth=(self.USERNAME, self.PASSWORD),
        )
        self.redis = Redis(
            host=self.HOSTS[0],
        )

    def search(self, query_text, locale=None):
        """
        Return relevant articles given search text.

        Finding the query term in the title of an article is given twice
        as much weight as finding the text in the body.

        After the most relevant articles are obtained, they are ranked by the
        ranking module (uses view counts here, but can be easily extended).
        
        Args:
            query_text(str): Text to be searched.
            locale(str): String to filter results by location.

        Returns:
            List[tuple(str, str)]: Returns a list of ranked tuples containing
                (article_id, Title of article).
        """
        # Create Search object to "match" query text against the title and body
        # of articles stored in the Knowledge base.
        s = Search(
            using=self.client,
            index=self.INDEX,
            doc_type=self.TYPE
        ).query(
            'multi_match',
            query=query_text,
            fields=['title^2', 'body']
        )
        
        # If locale is provided, use it to filter the set of documents that are
        # queried for.
        if locale:
            s = s.filter('term', locale=locale)

        response = s.execute()

        results = []
        for hit in response:
            article_id = hit.meta['id']

            # Retrieve view count for each relevant article.
            results.append((article_id, hit.title, self.redis.get(article_id)))

		# Rank results using Ranking function.
		ranked_results = Ranker.rank(results)

        return ranked_results

    def get(self, article_id):
        """
        Return an article specified by the given article_id.

        Args:
            article_id(str): Unique ID representing an article in the knowledge base.

        Returns:
            dict: Dictionary representing a document, of the following format
                {
                    'title': str,
                    'body': str,
                    'locale': str,
                }
                Returns None if no article matching the id is found.
        """ 
        try:
            response = self.client.get(
                index=self.INDEX,
                doc_type=self.TYPE,
                id=article_id,
            )
        except:
            return None
        else:
			# Increment view count for accessed article.
            self.redis.incr(article_id)
            return response['_source']

    def index(self, article, refresh=True):
		"""
        Index an article in the Knowledge Base.

        Args:
            article(dict): Dictionary representing an article in the knowledge base.
				Must follow the field names defined in the mapping.

        Returns:
            tuple(bool, str): Returns (True, article_id) if article is successfully indexed.
				(False, None) otherwise.
        """
        try:
            response = self.client.index(
                index=self.INDEX,
                doc_type=self.TYPE,
                body=article,
                refresh=refresh,
            )
        except:
            return False, None
        else:
			# Initialize view count for newly indexed article.
            self.redis.set(response['_id'], 0)
            return response['created'], response['_id'] 

    def delete(self, article_id, refresh=True):
		"""
        Delete an article from the Knowledge Base.

        Args:
            article_id(str): Unique ID representing an article in the knowledge base.

        Returns:
            bool: Returns True if article is successfully deleted. False otherwise.
        """
        try:
            response = self.client.delete(
                index=self.INDEX,
                doc_type=self.TYPE,
                id=article_id,
                refresh=refresh,
            )
        except:
            return False
        else:
			# Remove article key from Redis.
            self.redis.delete(article_id)
            return response['found']

    def _in_bulk(self, objects):
		"""
		Helper function to facilitate bulk operations.

		Args:
			objects(list[dict]): A list of dictionaries of the format
				[
					{
						'_op_type': String representing operation, valid choices are
							'index', 'create', 'update' and 'delete',
						'body': Contains updated document or new document to be created,
						'id': ID of article to be deleted. 
					},
					.
					.
				]
					
		"""
        bulk(self.client, objects, index=self.INDEX)

    def _init_index(self):
		"""
		Helper method to initialize Knowledge base store.

		Uses the JSON mappings to initialize an Elasticsearch index with a type mapping
		for storing articles.

		Returns:
			tuple(bool, str): Returns a boolean value representing whether the index and
				mapping were initialized and a string representing the status.

		"""
	
        index_mapping = json.load(open(self.INDEX_PATH))
        type_mapping = json.load(open(self.TYPE_PATH))

        try:
            self.client.indices.create(
                index='articles',
                body=index_mapping,
            )
        except:
            return False, 'Failed to create Index'

        try:
            self.client.indices.put_mapping(
                index='articles',
                doc_type='article',
                body=type_mapping,
            )
        except:
            return False, 'Failed to put Mapping'
        
        return True, 'Successfully initialized Index'

