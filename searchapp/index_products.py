from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from searchapp.constants import DOC_TYPE, INDEX_NAME
from searchapp.data import all_products, ProductData


def main():
    # Connect to localhost:9200 by default.
    es = Elasticsearch()

    es.indices.delete(index=INDEX_NAME, ignore=404)
    es.indices.create(
        index=INDEX_NAME,
        body={
            'mappings': {
                DOC_TYPE: {  # This mapping applies to products.
                    'properties': {  # Just a magic word.
                        'name': {  # The field we want to configure.
                            'type': 'text',  # The kind of data we’re working with.
                            'fields': {  # create an analyzed field.
                                'english_analyzed': {
                                    # Name that field `name.english_analyzed`.
                                    'type': 'text',  # It’s also text.
                                    'analyzer': 'custom_english_analyzer',
                                    # And here’s the analyzer we want to use.
                                }
                            }
                        },
                        # Just a magic word.
                        'description': {  # The field we want to configure.
                            'type': 'text',  # The kind of data we’re working with.
                            'fields': {  # create an analyzed field.
                                'english_analyzed': {
                                    # Name that field `name.english_analyzed`.
                                    'type': 'text',  # It’s also text.
                                    'analyzer': 'english',
                                    # And here’s the analyzer we want to use.
                                }
                            }
                        }
                    }
                }
            },
            'settings': {
                'analysis': {  # magic word.
                    'analyzer': {  # yet another magic word.
                        'custom_english_analyzer': {  # The name of our analyzer.
                            'type': 'english',
                            # The built in analyzer we’re building on.
                            'stopwords': ['made', '_english_'],
                            # Our custom stop words, plus the defaults.
                        },
                    },
                },
            },
        },
    )
    products_to_index(es, all_products())


def index_product(es, product: ProductData):
    """Add a single product to the ProductData index."""

    es.create(
        index=INDEX_NAME,
        doc_type=DOC_TYPE,
        id=product.id,
        body={
            "name": product.name,
            "description": product.description,
            "image": product.image,
            "taxonomy": product.taxonomy,
            "price": product.price,
        }
    )

    # Don't delete this! You'll need it to see if your indexing job is working,
    # or if it has stalled.
    print("Indexed {}".format(product.name))


def products_to_index(es, products: list[ProductData]):
    """Add list of products to the ProductData index."""
    bulk(es, _bulk_create_iter(products))

    # Don't delete this! You'll need it to see if your indexing job is working,
    # or if it has stalled.
    print("Bulk finished")


def _bulk_create_iter(products: list[ProductData]):
    for product in products:
        yield {
            "_op_type": "create",
            "_index": INDEX_NAME,
            "_type": DOC_TYPE,
            "_id": product.id,
            "name": product.name,
            "description": product.description,
            "image": product.image,
            "taxonomy": product.taxonomy,
            "price": product.price,
        }


if __name__ == '__main__':
    main()
