import logging

from searchapp.app.app import app


def main():
    es_logger = logging.getLogger('elasticsearch')
    es_logger.addHandler(logging.StreamHandler())
    es_logger.setLevel(logging.DEBUG)

    app.run(debug=True)


if __name__ == '__main__':
    main()
